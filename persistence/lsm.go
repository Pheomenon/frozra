package persistence

import (
	"github.com/dgraph-io/badger/y"
	"github.com/sirupsen/logrus"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"xonlab.com/frozra/v1/conf"
	"xonlab.com/frozra/v1/persistence/util"
)

type request struct {
	key   []byte
	value []byte
	wg    sync.WaitGroup
}

type Lsm struct {
	setting           conf.Persistence
	writeChan         chan *request
	l0Maintainer      *level0Maintainer
	l1Maintainer      *level1Maintainer
	absPath           string
	metadata          *metadata
	memoryTable       *hashMap
	swap              *hashMap
	flushDisk         chan *hashMap
	writeCloser       *y.Closer
	loadBalanceCloser *y.Closer
	compactCloser     *y.Closer
	flushDiskCloser   *y.Closer
	sync.RWMutex
}

func New(setting conf.Persistence) (*Lsm, error) {
	absPath, err := filepath.Abs(setting.Path)
	if err != nil {
		return nil, err
	}

	metadata, err := loadMetadata(absPath)
	if err != nil {
		return nil, err
	}

	l0Maintainer, err := loadFilter(absPath)

	l1Maintainer := newLevel1Maintainer()
	for _, l1File := range metadata.L1Files {
		t := readTable(absPath, l1File.Index)
		l1Maintainer.addTable(t, l1File.Index)
		t.release()
	}

	lsm := &Lsm{
		setting:           setting,
		writeChan:         make(chan *request, 1024),
		absPath:           absPath,
		metadata:          metadata,
		memoryTable:       newHashMap(setting.MemoryTableSize),
		l0Maintainer:      l0Maintainer,
		l1Maintainer:      l1Maintainer,
		writeCloser:       y.NewCloser(1),
		loadBalanceCloser: y.NewCloser(1),
		compactCloser:     y.NewCloser(1),
		flushDiskCloser:   y.NewCloser(1),
		flushDisk:         make(chan *hashMap, 1),
	}
	go lsm.runCompaction(lsm.compactCloser)
	go lsm.listeningForFlush(lsm.flushDiskCloser)
	go lsm.loadBalancing(lsm.loadBalanceCloser)
	go lsm.acceptWrite(lsm.writeCloser)
	return lsm, nil
}

func (l *Lsm) Set(key, val []byte) {
	r := request{
		key:   key,
		value: val,
	}
	r.wg.Add(1)
	l.writeChan <- &r
	r.wg.Wait()
}

func (l *Lsm) acceptWrite(closer *y.Closer) {
loop:
	for {
		select {
		case req := <-l.writeChan:
			l.write(req)
		case <-closer.HasBeenClosed():
			break loop
		}
	}
	close(l.writeChan)
	for req := range l.writeChan {
		l.write(req)
	}
	closer.Done()
}

func (l *Lsm) write(req *request) {
	// len(req.key) + len(req.value) + 8 is the total occupied of an entry in Lsm's buf
	if !l.memoryTable.isEnoughSpace(len(req.key) + len(req.value) + 8) {
		l.Lock()
		l.swap = l.memoryTable
		l.memoryTable = newHashMap(l.setting.MemoryTableSize)
		l.Unlock()
		l.flushDisk <- l.swap
	}
	l.memoryTable.Set(req.key, req.value)
	req.wg.Done()
}

func (l *Lsm) listeningForFlush(closer *y.Closer) {
loop:
	for {
		select {
		case swap := <-l.flushDisk:
			l.flushMemory(swap)
		case <-closer.HasBeenClosed():
			break loop
		}
	}
	close(l.flushDisk)
	for swap := range l.flushDisk {
		l.flushMemory(swap)
	}
	closer.Done()
}

func (l *Lsm) Get(key []byte) ([]byte, bool) {
	val, exist := l.memoryTable.Get(key)
	if exist {
		return val, exist
	}
	if l.swap != nil {
		val, exist = l.swap.Get(key)
		if exist {
			return val, exist
		}
	}

	val, exist = l.l0Maintainer.get(key, l.absPath)
	if exist {
		return val, exist
	}
	return l.l1Maintainer.get(key)
}

// Close save all data and metadata form memory to disk
func (l *Lsm) Close() {
	l.loadBalanceCloser.SignalAndWait()
	l.compactCloser.SignalAndWait()
	l.writeCloser.SignalAndWait()
	if l.memoryTable.Len() > 0 {
		l.flushDisk <- l.memoryTable
	}
	l.flushDiskCloser.SignalAndWait()
	err := l.metadata.save(l.absPath)
	if err != nil {
		logrus.Fatalf("metadata: unable to save the metadata %s", err.Error())
	}
	err = l.l0Maintainer.save(l.absPath)
	if err != nil {
		logrus.Fatalf("filter: unable to save the filter %s", err.Error())
	}
}

func (l *Lsm) flushMemory(swap *hashMap) {
	nextID := l.metadata.nextFileID()
	// persist swap to disk
	swap.persistence(l.absPath, nextID)
	// add swap's info to metadata
	l.metadata.addL0File(swap.records, swap.minRange, swap.maxRange, swap.occupiedSpace(), nextID)
	// add filter to swap
	l.l0Maintainer.addTable(swap, nextID)
	l.Lock()
	l.swap = nil
	l.Unlock()
}

func (l *Lsm) merge(t1, t2 *table) {
	t1.SeekBegin()
	t2.SeekBegin()
	merger := newTableMerger(int(t1.size + t2.size))
	merger.append(t1.fp, int64(t1.fileInfo.metaOffset))
	merger.append(t2.fp, int64(t2.fileInfo.metaOffset))
	merger.merge(t1.offsetMap, 0)
	merger.merge(t2.offsetMap, uint32(t1.fileInfo.metaOffset))
	buf := merger.setTableInfo()
	l.saveL1Table(buf)
}

func (l *Lsm) saveL1Table(buf []byte) {
	fileID := l.metadata.nextFileID()
	fp, err := os.Create(util.TablePath(l.absPath, fileID))
	if err != nil {
		logrus.Fatalf("compaction: unable to create new while pushing to level 1 %s", err.Error())
		return
	}
	defer fp.Close()
	n, err := fp.Write(buf)
	if err != nil {
		logrus.Fatalf("compaction: unable to write to new level 1 table %s", err.Error())
	}
	if n != len(buf) {
		logrus.Fatalf("compaction: unable to write a new file at level 1 table expected %d but got %d", len(buf), n)
	}
	// l1 table has been created so have to remove those files from l0
	// and add it to l1
	newTable := readTable(l.absPath, fileID)
	l.l1Maintainer.addTable(newTable, fileID)

	l.metadata.addL1File(uint32(newTable.fileInfo.entries), newTable.fileInfo.minRange, newTable.fileInfo.maxRange, int(newTable.size), fileID)
	logrus.Infof("comapction: new l1 file has beed added %d.fza", fileID)
}

func (l *Lsm) runCompaction(closer *y.Closer) {
loop:
	for {
		select {
		case <-closer.HasBeenClosed():
			break loop
		default:
			// check for l0Tables
			l0Len := l.metadata.l0Len()
			if l0Len >= l.setting.L0Capacity {
				// if there is no file on the level 1, just push two level 0 tables to level1
				if l.metadata.l1Len() == 0 {
					l.metadata.sortL0()
					l.metadata.mutex.Lock()
					t1, t2 := readTable(l.absPath, l.metadata.L0Files[0].Index), readTable(l.absPath, l.metadata.L0Files[1].Index)
					l.metadata.mutex.Unlock()
					l.l0Maintainer.compress(t1, t2)
					l.l1Maintainer.persistence(t1, l.absPath, l.metadata.nextFileID())
					l.l1Maintainer.addTable(t1, t1.index)
					l.l0Maintainer.delTable(t1.index)
					l.l0Maintainer.delTable(t2.index)
					l.metadata.delL0File(l.metadata.L0Files[0].Index)
					l.metadata.delL0File(l.metadata.L0Files[1].Index)
					l.metadata.addL1File(uint32(t1.fileInfo.entries), t1.fileInfo.minRange, t1.fileInfo.maxRange, int(t1.size), l.metadata.NextIndex)
				} else {
					// level 1 files already exist so find union set to push
					// if overlapping range then append accordingly otherwise just push down
					l0fs := l.metadata.copyL0()
					logrus.Infof("%+v", l.metadata.L0Files)
					logrus.Infof("%+v", l.metadata.L1Files)
					for _, l0f := range l0fs {
						compactStrategy := l.metadata.l1Status(l0f)
						if compactStrategy.strategy == NOTUNION {
							l.notUnion(l0f)
							continue
						}
						if compactStrategy.strategy == UNION {
							l.union(compactStrategy, l0f)
							continue
						}
						if compactStrategy.strategy == OVERLAPPING {
							l.overlapping(compactStrategy, l0f)
						}
					}
				}
			}
		}
	}
	closer.Done()
}

func (l *Lsm) loadBalancing(closer *y.Closer) {
loop:
	for {
		select {
		case <-closer.HasBeenClosed():
			break loop
		default:
			for _, l1f := range l.metadata.copyL1() {
				if l1f.Size > uint32(l.setting.L1TableSize) {
					logrus.Infof("load balancing: level 1 file %d.fza found which it larger than max l1 file size", l1f.Index)
					l1t := readTable(l.absPath, l1f.Index)
					//entries := l1t.entries()
					//k := len(entries) / 2
					//median := entries[k]
					median := (l1t.fileInfo.maxRange - l1t.fileInfo.minRange) / 2
					mergers := []*tableMerger{newTableMerger(int(l1f.Size) / 2), newTableMerger(int(l1f.Size) / 2)}
					iter := l1t.iter()
					for iter.hasNext() {
						kl, vl, key, val := iter.next()
						c := crc32.New(CrcTable)
						c.Write(key)
						hash := c.Sum32()
						if hash < median {
							mergers[0].add(kl, vl, key, val, hash)
							continue
						}
						mergers[1].add(kl, vl, key, val, hash)
						continue
					}
					l.saveL1Table(mergers[0].setTableInfo())
					l.saveL1Table(mergers[1].setTableInfo())
					l.l1Maintainer.delTable(l1f.Index)
					l.metadata.delL1File(l1f.Index)
					logrus.Infof("load balancing: level 1 file %d.fza is splitted into two l1 files properly", l1f.Index)
				}
			}
		}
	}
	closer.Done()
}
