package persistence

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path"
	"sync"

	"github.com/AndreasBriese/bbloom"
	"github.com/sirupsen/logrus"
)

type level0Maintainer struct {
	filter map[uint32]bbloom.Bloom
	sync.Mutex
}

func newL0Maintainer() *level0Maintainer {
	return &level0Maintainer{
		filter: map[uint32]bbloom.Bloom{},
	}
}

// addTable add new l0 table's bloom to l0 maintainer
func (lm0 *level0Maintainer) addTable(h *hashMap, fd uint32) {
	// use bloom filter to record every key-value pair
	slots := h.Len()
	filter := bbloom.New(float64(slots), 0.001)
	//var buf bytes.Buffer
	//buf.Grow(4)
	for key := range h.concurrentMap {
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, key)
		filter.Add(buf)
	}
	lm0.Lock()
	defer lm0.Unlock()
	lm0.filter[fd] = filter
}

func (lm0 *level0Maintainer) get(key []byte, holder *tableHolder) ([]byte, bool) {
	var value []byte
	c := crc32.New(CrcTable)
	_, _ = c.Write(key)
	var hash []byte
	// TODO: need to optimize!
	hash = append(hash, c.Sum(hash)...)
	for fd, bloom := range lm0.filter {
		isIn := bloom.Has(hash)
		if isIn {
			value = lm0.search(key, fd, holder)
		}
		if value == nil {
			continue
		} else {
			return value, true
		}
	}
	return nil, false
}

func (lm0 *level0Maintainer) search(key []byte, fd uint32, holder *tableHolder) []byte {
	holder.fdKey <- fdKey{fd: fd, key: key}
	result := <-holder.l0
	if result != nil {
		return result
	}
	return nil

	//t0 := readTable(absPath, fd)
	// TODO: this table's fd should be close but when it close return value will be unreferenced.
	//defer t0.release()
	//hash := util.Hashing(key)
	//if position, ok := t0.offsetMap[hash]; ok {
	//	keyLength := binary.BigEndian.Uint32(t0.data[position : position+4])
	//	position += 4
	//	valLength := binary.BigEndian.Uint32(t0.data[position : position+4])
	//	position += 4
	//	return t0.data[position+keyLength : position+keyLength+valLength]
	//}
}

// save every l0 table's filter to disk
func (lm0 *level0Maintainer) save(absPath string) error {
	filterName := path.Join(absPath, "filter")
	fp, err := os.OpenFile(filterName, os.O_WRONLY, 0666)
	if err != nil {
		return err
	}

	dump := map[uint32][]byte{}
	for fd, bloom := range lm0.filter {
		filterJSON := bloom.JSONMarshal()
		dump[fd] = filterJSON
	}
	encoder := gob.NewEncoder(fp)
	return encoder.Encode(dump)
}

// loadFilter load filter from disk
func loadFilter(absPath string) (*level0Maintainer, error) {
	filterName := path.Join(absPath, "filter")
	_, err := os.Stat(filterName)
	if err != nil {
		if !os.IsExist(err) {
			return createFilter(filterName)
		} else {
			panic(fmt.Sprintf("check filter error: %v", err))
		}
	}

	fp, err := os.Open(filterName)
	if err != nil {
		panic(fmt.Sprintf("load filter error: %v", err))
	}

	dump := map[uint32][]byte{}
	decoder := gob.NewDecoder(fp)
	err = decoder.Decode(&dump)
	if err != nil && err != io.EOF {
		return nil, err
	}

	lm0 := newL0Maintainer()
	for fd, filterJSON := range dump {
		lm0.filter[fd] = bbloom.JSONUnmarshal(filterJSON)
	}
	return lm0, nil
}

func createFilter(filterName string) (*level0Maintainer, error) {
	fp, err := os.Create(filterName)
	if err != nil {
		panic(fmt.Sprintf("create filter error: %v", err))
	}
	fp.Close()
	return newL0Maintainer(), nil
}

func (lm0 *level0Maintainer) delTable(fd uint32) {
	lm0.Lock()
	if _, ok := lm0.filter[fd]; !ok {
		logrus.Warnf("level 0 maintainer: don't found the table that should be deleted")
	}
	delete(lm0.filter, fd)
	lm0.Unlock()
}

// compress two tables into one then delete old file in disk
func (lm0 *level0Maintainer) compress(t1, t2 *table, fd uint32) *table {
	content := bytes.NewBuffer(t1.data)
	content.Grow(len(t1.data) + len(t2.data))
	content.Write(t2.data)
	t1.data = content.Bytes()
	offset := uint32(t1.fileInfo.metaOffset)
	for key, val := range t2.offsetMap {
		t1.offsetMap[key] = val + offset
	}
	if t2.fileInfo.maxRange > t1.fileInfo.maxRange {
		t1.fileInfo.maxRange = t2.fileInfo.maxRange
	}
	if t2.fileInfo.minRange < t1.fileInfo.minRange {
		t1.fileInfo.minRange = t2.fileInfo.minRange
	}
	t1.fileInfo.metaOffset += t2.fileInfo.metaOffset
	t1.fileInfo.entries += t2.fileInfo.entries
	t1.index = fd
	return t1
}
