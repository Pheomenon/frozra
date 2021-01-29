package persistence

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"xonlab.com/frozra/v1/persistence/util"

	"github.com/dgraph-io/badger/y"
)

type request struct {
	key   []byte
	value []byte
	wg    sync.WaitGroup
}

type lsm struct {
	setting           Setting
	writeChan         chan *request
	l0Maintainer      *levelMaintainer
	l1Maintainer      *levelMaintainer
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

func New(setting Setting) (*lsm, error) {
	absPath, err := filepath.Abs(setting.path)
	if err != nil {
		return nil, err
	}

	metadata, err := loadMetadata(absPath)
	if err != nil {
		return nil, err
	}

	l0Maintainer := newLevelMaintainer()
	for _, l0File := range metadata.L0Files {
		t := newTable(absPath, l0File.Index)
		l0Maintainer.andTable(t, l0File.Index)
	}

	l1Maintainer := newLevelMaintainer()
	for _, l1File := range metadata.L1Files {
		t := newTable(absPath, l1File.Index)
		l1Maintainer.andTable(t, l1File.Index)
	}

	lsm := &lsm{
		setting:           setting,
		writeChan:         make(chan *request, 1024),
		absPath:           absPath,
		metadata:          metadata,
		memoryTable:       newHashMap(setting.memTableSize),
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

func (l *lsm) Set(key, val []byte) {
	r := request{
		key:   key,
		value: val,
	}
	r.wg.Add(1)
	l.writeChan <- &r
	r.wg.Wait()
}

func (l *lsm) acceptWrite(closer *y.Closer) {
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

func (l *lsm) write(req *request) {
	if !l.memoryTable.isEnoughSpace(len(req.key) + len(req.value)) {
		l.Lock()
		l.swap = l.memoryTable
		l.memoryTable = newHashMap(l.setting.memTableSize)
		l.Unlock()
		l.flushDisk <- l.swap
	}
	l.memoryTable.Set(req.key, req.value)
	req.wg.Done()
}

func (l *lsm) listeningForFlush(closer *y.Closer) {
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

func (l *lsm) Get(key []byte) ([]byte, bool) {
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

	val, exist = l.l0Maintainer.get(key)
	if exist {
		return val, exist
	}
	return l.l1Maintainer.get(key)
}

func (l *lsm) Close() {
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
}

func (l *lsm) flushMemory(swap *hashMap) {
	nextID := l.metadata.nextFileID()
	swap.persistence(l.absPath, nextID)
	l.metadata.addL0File(swap.records, swap.minRange, swap.maxRange, swap.occupiedSpace(), nextID)
	table := newTable(l.absPath, nextID)
	l.l0Maintainer.andTable(table, nextID)
	l.Lock()
	l.memoryTable = nil
	l.Unlock()
}

func (l *lsm) merge(t1, t2 *table) {
	t1.SeekBegin()
	t2.SeekBegin()
	maintainer := newTableMerger(int(t1.size + t2.size))
	maintainer.append(t1.fp, int64(t1.fileInfo.metaOffset))
	maintainer.append(t2.fp, int64(t2.fileInfo.metaOffset))
	maintainer.merge(t1.offsetMap, 0)
	maintainer.merge(t2.offsetMap, uint32(t1.fileInfo.metaOffset))
	buf := maintainer.finish()
	l.saveL1Table(buf)
}

func (l *lsm) saveL1Table(buf []byte) {
	fileID := l.metadata.nextFileID()
	fp, err := os.Create(util.TablePath(l.absPath, fileID))
	if err != nil {
		logrus.Fatalf("compaction: unable to create new while pushing to level 1 %s", err.Error())
		return
	}
	n, err := fp.Write(buf)
	if err != nil {
		logrus.Fatalf("compaction: unable to write to new level 1 table %s", err.Error())
	}
	if n != len(buf) {
		logrus.Fatalf("compaction: unable to write a new file at level 1 table expected %d but got %d", len(buf), n)
	}
	//l1 table has been created so have to remove those files from l0
	// and add it to l1
	newTable := newTable(l.absPath, fileID)
	l.l1Maintainer.andTable(newTable, fileID)

	l.metadata.addL1File(uint32(newTable.fileInfo.entries), newTable.fileInfo.minRange, newTable.fileInfo.maxRange, int(newTable.size), fileID)
	logrus.Infof("comapction: new l1 file has beed added %d", fileID)
}

func (l *lsm) L0Compaction() {
	l.metadata.sortL0()
	l.metadata.mutex.Lock()
	t1, t2 := newTable(l.absPath, l.metadata.L0Files[0].Index), newTable(l.absPath, l.metadata.L0Files[1].Index)
	l.metadata.mutex.Unlock()
	l.merge(t1, t2)
	l.l0Maintainer.delTable(t1.ID())
	t1.close()
	util.RemoveTable(l.absPath, t1.ID())
	l.metadata.deleteL0Table(t1.ID())
	logrus.Infof("compaction: l0 file has been deleted %d", t1.ID())
	l.l0Maintainer.delTable(t2.ID())
	t2.close()
	util.RemoveTable(l.absPath, t2.ID())
	l.metadata.deleteL0Table(t2.ID())
	logrus.Infof("comapction: l0 file has been deleted %d", t2.ID())
}

func (l *lsm) runCompaction(closer *y.Closer) {
loop:
	for {
		select {
		case <-closer.HasBeenClosed():
			break loop
		default:
			// check for l0Tables
			l0Len := l.metadata.l0Len()
			if l0Len >= l.setting.L0FileCapacity {
				if l.metadata.l1Len() == 0 {
					l.L0Compaction()
				}
				// level 1 files already exist so find union set to push
				// if overlapping range then append accordingly otherwise just push down
				l0fs := l.metadata.copyL0()
				fmt.Printf("%+v \n", l.metadata)
				for _, l0f := range l0fs {
					p := l.metadata.l1Status(l0f)
					if p.strategy == NOTUNION {
						l.notUnion(l0f)
						continue
					}
					if p.strategy == UNION {
						l.union(p, l0f)
						continue
					}
					if p.strategy == OVERLAPPING {
						l.overlapping(p, l0f)
					}
				}
			}
		}
	}
	closer.Done()
}

func (l *lsm) loadBalancing(closer *y.Closer) {
loop:
	for {
		select {
		case <-closer.HasBeenClosed():
			break loop
		default:
			for _, l1f := range l.metadata.copyL1() {
				if l1f.Size > uint32(l.setting.maxL1Size) {
					logrus.Infof("load balancing: l1 file %d found which it larger than max l1 size", l1f.Index)
					l1t := newTable(l.absPath, l1f.Index)
					ents := l1t.entries()
					k := len(ents) / 2
					median := ents[k]
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
					l.saveL1Table(mergers[0].finish())
					l.saveL1Table(mergers[1].finish())
					l.l1Maintainer.delTable(l1f.Index)
					l.metadata.deleteL1Table(l1f.Index)
					logrus.Infof("load balancing: l1 file %d is splitted into two l1 files properly", l1f.Index)
				}
			}
		}
	}
	closer.Done()
}
