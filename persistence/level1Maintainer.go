package persistence

import (
	"hash/crc32"
	"sync"
)

type level1Maintainer struct {
	//tables  []*table
	indexer *tree
	sync.RWMutex
}

func newLevel1Maintainer() *level1Maintainer {
	return &level1Maintainer{
		//tables:  make([]*table, 0),
		indexer: NewTree(),
	}
}

func (lm *level1Maintainer) addTable(t *table, index uint32) {
	lm.Lock()
	defer lm.Unlock()
	//lm.tables = append(lm.tables, t)
	lm.indexer.put(t.fileInfo.minRange, index)
}

func (lm *level1Maintainer) delTable(index uint32) {
	lm.Lock()
	defer lm.Unlock()
	lm.indexer.deleteTable(index)
	//for i, table := range lm.tables {
	//	if table.ID() == index {
	//		lm.tables[i] = lm.tables[len(lm.tables)-1]
	//		lm.tables[len(lm.tables)-1] = nil
	//		lm.tables = lm.tables[:len(lm.tables)-1]
	//		break
	//	}
	//}
}

func (lm *level1Maintainer) get(key []byte) ([]byte, bool) {
	lm.RLock()
	defer lm.RUnlock()
	c := crc32.New(CrcTable)
	c.Write(key)
	//TODO:
	//hash := c.Sum32()
	//nodes := lm.indexer.allLargestRange(hash)
	//
	//for _, node := range nodes {
	//	for _, id := range node.index {
	//		//
	//		t := lm.getTable(id)
	//		if t != nil {
	//			val, exist := t.Get(key)
	//			if exist {
	//				return val, true
	//			}
	//		}
	//	}
	//}
	return nil, false
}

// TODO: return level1Maintainer's table but now it should return fd.
func (lm *level1Maintainer) getTable(index uint32) *table {
	//for _, t := range lm.tables {
	//	if t.ID() == index {
	//		return t
	//	}
	//}
	return nil
}
