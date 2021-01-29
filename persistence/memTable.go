package persistence

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"github.com/AndreasBriese/bbloom"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

var CrcTable = crc32.MakeTable(crc32.Castagnoli)

type hashMap struct {
	buf           []byte
	currentOffset int
	minRange      uint32
	maxRange      uint32
	concurrentMap map[uint32]uint32
	size          int
	records       uint32
	sync.RWMutex
}

func newHashMap(size int) *hashMap {
	return &hashMap{
		buf:           make([]byte, size),
		concurrentMap: make(map[uint32]uint32, 0),
		size:          size,
		RWMutex:       sync.RWMutex{},
	}
}

func (h *hashMap) Set(key, value []byte) {
	h.Lock()
	c := crc32.New(CrcTable)
	c.Write(key)
	hash := c.Sum32()
	oldOffSet := h.currentOffset
	keyLength := len(key)
	valLength := len(value)

	//first 8 byte uses to store key and value's length
	binary.BigEndian.PutUint32(h.buf[h.currentOffset:], uint32(keyLength))
	h.currentOffset += 4

	binary.BigEndian.PutUint32(h.buf[h.currentOffset:], uint32(valLength))
	h.currentOffset += 4

	//save key
	copy(h.buf[h.currentOffset:h.currentOffset+keyLength], key)
	h.currentOffset += keyLength

	//save value
	copy(h.buf[h.currentOffset:h.currentOffset+valLength], value)
	h.currentOffset += valLength

	//use CRC checksum as key and this map's position as value
	h.concurrentMap[hash] = uint32(oldOffSet)
	h.Unlock()
	h.setRange(hash)
	atomic.AddUint32(&h.records, 1)
}

func (h *hashMap) Get(item []byte) ([]byte, bool) {
	h.RLock()
	defer h.RUnlock()
	c := crc32.New(CrcTable)
	c.Write(item)
	hash := c.Sum32()
	position, ok := h.concurrentMap[hash]
	if !ok {
		return nil, false
	}
	keyLength := binary.BigEndian.Uint32(h.buf[position : position+4])
	position += 4
	valLength := binary.BigEndian.Uint32(h.buf[position : position+4])
	position += 4

	key := h.buf[position : position+keyLength]
	if bytes.Compare(key, item) != 0 {
		return nil, false
	}
	position += keyLength
	end := position + valLength
	return h.buf[position:end], true
}

func (h *hashMap) setRange(r uint32) {
	h.Lock()
	defer h.Unlock()
	h.setMinRange(r)
	h.setMaxRange(r)
}

func (h *hashMap) setMinRange(r uint32) {
	if h.minRange == 0 {
		h.minRange = r
		return
	}
	if h.minRange > r {
		h.minRange = r
	}
}

func (h *hashMap) setMaxRange(r uint32) {
	if h.maxRange == 0 {
		h.maxRange = r
		return
	}
	if h.maxRange < r {
		h.maxRange = r
	}
}

func (h *hashMap) isEnoughSpace(size int) bool {
	h.RLock()
	defer h.RUnlock()
	left := h.size - h.currentOffset
	if left < size {
		return false
	}
	return true
}

func (h *hashMap) occupiedSpace() int {
	return h.size - h.currentOffset
}

func (h *hashMap) persistence(path string, index uint32) {
	h.Lock()
	defer h.Unlock()
	filePath, err := filepath.Abs(path)
	if err != nil {
		panic("unable to flushing memory table to disk")
	}
	fp, err := os.Create(fmt.Sprintf("%s/%d.fza", filePath, index))
	if err != nil {
		panic(fmt.Sprintf("unable to flush memory table, error: %v", err))
	}
	defer fp.Close()
	fp.Write(h.buf[0:h.currentOffset])
	slots := h.Len()

	// use bloom filter to record every key-value pair
	filter := bbloom.New(float64(slots), 0.01)
	for kv := range h.concurrentMap {
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, kv)
		filter.Add(buf)
	}

	// encode this table's info
	fib := make([]byte, 32)
	filterJson := filter.JSONMarshal()
	fi := &fileInfo{
		metaOffset: h.currentOffset,
		entries:    slots,
		minRange:   h.minRange,
		maxRange:   h.maxRange,
		filterSize: len(filterJson),
	}
	fi.Encode(fib)

	// encode every map to metaBuf
	metaBuf := new(bytes.Buffer)
	encoder := gob.NewEncoder(metaBuf)
	err = encoder.Encode(h.concurrentMap)
	if err != nil {
		panic("unable to encode concurrent map")
	}
	fp.Write(metaBuf.Bytes())
	fp.Write(filterJson)
	fp.Write(fib)
}

func (h *hashMap) Len() int {
	return len(h.concurrentMap)
}

type fileInfo struct {
	metaOffset int
	entries    int
	minRange   uint32
	maxRange   uint32
	filterSize int
}

func (fi *fileInfo) Decode(buf []byte) {
	fi.metaOffset = int(binary.BigEndian.Uint32(buf[0:4]))
	fi.entries = int(binary.BigEndian.Uint32(buf[4:8]))
	fi.minRange = binary.BigEndian.Uint32(buf[8:16])
	fi.maxRange = binary.BigEndian.Uint32(buf[16:24])
	fi.filterSize = int(binary.BigEndian.Uint32(buf[24:32]))
}

func (fi *fileInfo) Encode(buf []byte) {
	binary.BigEndian.PutUint32(buf[0:4], uint32(fi.metaOffset))
	binary.BigEndian.PutUint32(buf[4:8], uint32(fi.entries))
	binary.BigEndian.PutUint32(buf[8:16], fi.minRange)
	binary.BigEndian.PutUint32(buf[16:24], fi.maxRange)
	binary.BigEndian.PutUint32(buf[24:32], uint32(fi.filterSize))
}
