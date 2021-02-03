package persistence

import (
	"encoding/binary"
	"fmt"
	"github.com/sirupsen/logrus"
	"os"
	"strconv"
	"sync"
	"xonlab.com/frozra/v1/persistence/util"
)

type level1Maintainer struct {
	indexer *indexer
	sync.RWMutex
}

func newLevel1Maintainer() *level1Maintainer {
	return &level1Maintainer{
		indexer: NewIndexer(),
	}
}

func (lm *level1Maintainer) addTable(t *table, index uint32) {
	lm.Lock()
	defer lm.Unlock()
	lm.indexer.put(t.fileInfo.minRange, index)
}

func (lm *level1Maintainer) delTable(index uint32) {
	lm.Lock()
	defer lm.Unlock()
	lm.indexer.delete(index)
	//remove this table in disk
	s := strconv.Itoa(int(index))
	err := os.Remove(fmt.Sprintf("./%s", s))
	if err != nil {
		logrus.Debugf("l1M: remove table error: %v", err)
	}
}

// get check indexer and return corresponding value if it existed
func (lm *level1Maintainer) get(key []byte) ([]byte, bool) {
	lm.RLock()
	defer lm.RUnlock()
	hash := util.Hashing(key)
	target := lm.indexer.floor(hash)
	table := readTable("./", target.fd)
	return lm.searchKey(table, hash)
}

func (lm *level1Maintainer) searchKey(t *table, hash uint32) ([]byte, bool) {
	position, ok := t.offsetMap[hash]
	if !ok {
		return nil, false
	}
	keyLength := binary.BigEndian.Uint32(t.data[position : position+4])
	position += 4
	valLength := binary.BigEndian.Uint32(t.data[position : position+4])
	position += 4
	position += keyLength
	return t.data[position : position+valLength], true
}
