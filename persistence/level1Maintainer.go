package persistence

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"github.com/sirupsen/logrus"
	"os"
	"path/filepath"
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
		indexer: newIndexer(),
	}
}

func (lm1 *level1Maintainer) addTable(t *table) {
	lm1.Lock()
	defer lm1.Unlock()
	lm1.indexer.put(t.fileInfo.minRange, t.index)
}

func (lm1 *level1Maintainer) delTable(index uint32) {
	lm1.Lock()
	defer lm1.Unlock()
	lm1.indexer.delete(index)
	// remove this table in disk
	s := strconv.Itoa(int(index))
	err := os.Remove(fmt.Sprintf("./%s", s))
	if err != nil {
		logrus.Debugf("l1M: remove table error: %v", err)
	}
}

// get check indexer and return corresponding value if it existed
func (lm1 *level1Maintainer) get(key []byte) ([]byte, bool) {
	lm1.RLock()
	defer lm1.RUnlock()
	hash := util.Hashing(key)
	target := lm1.indexer.floor(hash)
	if target != nil {
		table := readTable("./", target.fd)
		//defer table.release()
		return lm1.searchKey(table, hash)
	} else {
		return nil, false
	}
}

func (lm1 *level1Maintainer) searchKey(t *table, hash uint32) ([]byte, bool) {
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

func (lm1 *level1Maintainer) persistence(t *table, path string) {
	filePath, err := filepath.Abs(path)
	if err != nil {
		panic("persistence in level 1: unable to flushing memory table to disk")
	}
	fp, err := os.Create(fmt.Sprintf("%s/%d.fza", filePath, t.index))
	if err != nil {
		panic(fmt.Sprintf("persistence in level 1: unable to flush memory table, error: %v", err))
	}
	defer fp.Close()

	_, err = fp.Write(t.data)
	if err != nil {
		logrus.Fatalf("persistence in level 1: can't save data to disk: %v", err)
	}
	slots := len(t.offsetMap)
	fib := make([]byte, 32)
	fi := &fileInfo{
		metaOffset: t.fileInfo.metaOffset,
		entries:    slots,
		minRange:   t.fileInfo.minRange,
		maxRange:   t.fileInfo.maxRange,
	}
	fi.Encode(fib)

	// encode every map to mapBuf
	mapBuf := new(bytes.Buffer)
	encoder := gob.NewEncoder(mapBuf)
	err = encoder.Encode(t.offsetMap)
	if err != nil {
		panic("persistence in level 1: unable to encode concurrent map")
	}
	fp.Write(mapBuf.Bytes())
	fp.Write(fib)
}
