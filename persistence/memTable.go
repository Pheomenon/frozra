package persistence

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"github.com/sirupsen/logrus"
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
	_, _ = c.Write(key)
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
	if uint32(h.Len()) != h.records {
		atomic.AddUint32(&h.records, 1)
	}
}

func (h *hashMap) Get(item []byte) ([]byte, bool) {
	h.RLock()
	defer h.RUnlock()
	c := crc32.New(CrcTable)
	_, _ = c.Write(item)
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

	// traverse every key-value pair and copy its content.
	// because memory table just append entry's content and change entry's value
	// if a entry updated frequently that will waste massive buffer.
	var content bytes.Buffer
	content.Grow(len(h.buf))
	// this var use to store the real entry position(origin position - duplicate key caused offset)
	var entryPosition uint32
	for hash, position := range h.concurrentMap {
		entryPosition = uint32(content.Len())
		// key length
		content.Write(h.buf[position : position+4])
		// value length
		content.Write(h.buf[position+4 : position+8])
		// key content
		keyLength := binary.BigEndian.Uint32(h.buf[position : position+4])
		content.Write(h.buf[position+8 : position+8+keyLength])
		// value content
		valLength := binary.BigEndian.Uint32(h.buf[position+4 : position+8])
		content.Write(h.buf[position+8+keyLength : position+8+keyLength+valLength])
		h.concurrentMap[hash] = entryPosition
	}

	_, err = fp.Write(content.Bytes())
	if err != nil {
		logrus.Fatalf("persistence: can't save data to disk: %v", err)
	}
	slots := h.Len()
	fib := make([]byte, 32)
	fi := &fileInfo{
		metaOffset: content.Len(),
		entries:    slots,
		minRange:   h.minRange,
		maxRange:   h.maxRange,
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
	//filterSize int
}

func (fi *fileInfo) Decode(buf []byte) {
	fi.metaOffset = int(binary.BigEndian.Uint32(buf[0:4]))
	fi.entries = int(binary.BigEndian.Uint32(buf[4:8]))
	fi.minRange = binary.BigEndian.Uint32(buf[8:16])
	fi.maxRange = binary.BigEndian.Uint32(buf[16:24])
}

func (fi *fileInfo) Encode(buf []byte) {
	binary.BigEndian.PutUint32(buf[0:4], uint32(fi.metaOffset))
	binary.BigEndian.PutUint32(buf[4:8], uint32(fi.entries))
	binary.BigEndian.PutUint32(buf[8:16], fi.minRange)
	binary.BigEndian.PutUint32(buf[16:24], fi.maxRange)
}
