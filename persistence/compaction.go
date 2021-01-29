package persistence

import (
	"github.com/sirupsen/logrus"
	"hash/crc32"
	"xonlab.com/frozra/v1/persistence/util"
)

func (l *lsm) notUnion(l0f tableMetadata) {
	newTable := newTable(l.absPath, l0f.Index)
	l.l1Maintainer.andTable(newTable, l0f.Index)
	l.l0Maintainer.delTable(l0f.Index)
	l.metadata.addL1File(uint32(newTable.fileInfo.entries), newTable.fileInfo.minRange, newTable.fileInfo.maxRange, int(newTable.size), l0f.Index)
	l.metadata.deleteL0Table(l0f.Index)
	logrus.Info("compaction: NOT UNION found so simply pushing the l0 file to l1")
}

func (l *lsm) union(cs compactionStrategy, l0f tableMetadata) {
	t1, t2 := newTable(l.absPath, l0f.Index), newTable(l.absPath, cs.tableIDs[0])
	l.merge(t1, t2)
	logrus.Infof("compaction: UNION SET found so merged l0 %d with l1 %d, pushed to l1", t1.ID(), t2.ID())
	t1.close()
	l.l0Maintainer.delTable(t1.ID())
	l.metadata.deleteL0Table(t1.ID())
	util.RemoveTable(l.absPath, t1.ID())
	logrus.Infof("compaction: l0 file has been deleted %d", t1.ID())
	t2.close()
	l.l1Maintainer.delTable(t2.ID())
	l.metadata.deleteL1Table(t2.ID())
	util.RemoveTable(l.absPath, t2.ID())
	logrus.Infof("compaction: l1 file has been deleted %d", t2.ID())
}

func (l *lsm) overlapping(cs compactionStrategy, l0f tableMetadata) {
	logrus.Infof("compaction: OVERLAPPING found")
	mergers := []*tableMerger{}
	// if the the value is not in the range, we'll create a new file and append everything in it
	var extraBuilder *tableMerger
	// some crazy for loop has been written so try to refactor
	for _, idx := range cs.tableIDs {
		t := newTable(l.absPath, idx)
		t.SeekBegin()
		builder := newTableMerger(int(t.size))
		builder.append(t.fp, int64(t.fileInfo.metaOffset))
		builder.merge(t.offsetMap, 0)
		mergers = append(mergers, builder)
	}
	toCompacT := newTable(l.absPath, l0f.Index)
	iter := toCompacT.iter()
	for iter.hasNext() {
		kl, vl, key, val := iter.next()
		c := crc32.New(CrcTable)
		c.Write(key)
		hash := c.Sum32()
		for _, builder := range mergers {
			if hash >= builder.Min() && hash <= builder.Max() {
				c := crc32.New(CrcTable)
				c.Write(key)
				hash := c.Sum32()
				builder.add(kl, vl, key, val, hash)
				continue
			}
			if extraBuilder == nil {
				extraBuilder = newTableMerger(10000000)
			}
			c := crc32.New(CrcTable)
			c.Write(key)
			hash := c.Sum32()
			extraBuilder.add(kl, vl, key, val, hash)
		}
	}
	for _, builder := range mergers {
		l.saveL1Table(builder.finish())
	}
	if extraBuilder != nil {
		l.saveL1Table(extraBuilder.finish())
	}
	for _, idx := range cs.tableIDs {
		l.l1Maintainer.delTable(idx)
		util.RemoveTable(l.absPath, idx)
		l.metadata.deleteL1Table(idx)
	}
	l.l0Maintainer.delTable(l0f.Index)
	util.RemoveTable(l.absPath, l0f.Index)
	l.metadata.deleteL0Table(l0f.Index)
}
