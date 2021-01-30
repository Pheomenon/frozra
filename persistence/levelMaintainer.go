package persistence

import (
	"hash/crc32"
	"sync"
)

type levelMaintainer struct {
	tables  []*table
	indexer *tree
	sync.RWMutex
}

func newLevelMaintainer() *levelMaintainer {
	return &levelMaintainer{
		tables:  make([]*table, 0),
		indexer: NewTree(),
	}
}

func (l *levelMaintainer) addTable(t *table, index uint32) {
	l.Lock()
	defer l.Unlock()
	l.tables = append(l.tables, t)
	l.indexer.put(t.fileInfo.minRange, index)
}

func (l *levelMaintainer) delTable(index uint32) {
	l.Lock()
	defer l.Unlock()
	l.indexer.deleteTable(index)
	for i, table := range l.tables {
		if table.ID() == index {
			l.tables[i] = l.tables[len(l.tables)-1]
			l.tables[len(l.tables)-1] = nil
			l.tables = l.tables[:len(l.tables)-1]
			break
		}
	}
}

func (l *levelMaintainer) get(key []byte) ([]byte, bool) {
	l.RLock()
	defer l.RUnlock()
	c := crc32.New(CrcTable)
	c.Write(key)
	hash := c.Sum32()
	nodes := l.indexer.allLargestRange(hash)

	for _, node := range nodes {
		for _, id := range node.index {
			t := l.getTable(id)
			if t != nil {
				val, exist := t.Get(key)
				if exist {
					return val, true
				}
			}
		}
	}
	return nil, false
}

func (l *levelMaintainer) getTable(index uint32) *table {
	for _, t := range l.tables {
		if t.ID() == index {
			return t
		}
	}
	return nil
}
