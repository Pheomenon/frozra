package persistence

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"github.com/AndreasBriese/bbloom"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"syscall"
	"xonlab.com/frozra/v1/persistence/util"
)

type table struct {
	data      []byte
	path      string
	fileInfo  *fileInfo
	size      int64
	fp        *os.File
	status    os.FileInfo
	filter    *bbloom.Bloom
	offsetMap map[uint32]uint32
	index     uint32
	sync.RWMutex
}

// readTable return table's content
func readTable(path string, index uint32) *table {
	path = util.TablePath(path, index)
	fp, err := os.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		panic(fmt.Sprintf("unable to open table file, error: %v", err))
	}
	status, err := os.Stat(path)
	if err != nil {
		panic(fmt.Sprintf("unable to get table file status, error: %v", err))
	}
	data, err := syscall.Mmap(int(fp.Fd()), int64(0), int(status.Size()), syscall.PROT_READ, syscall.MAP_PRIVATE)
	if err != nil {
		panic(fmt.Sprintf("unable to mmap: %v", err))
	}
	fi := &fileInfo{}
	// get file info
	fi.Decode(data[status.Size()-32 : status.Size()])

	filter := bbloom.JSONUnmarshal(data[status.Size()-32-int64(fi.filterSize) : status.Size()-32])
	metaBuf := new(bytes.Buffer)
	// metaBuf saved all maps in this table
	metaBuf.Write(data[fi.metaOffset : status.Size()-32-int64(fi.filterSize)])
	offsetMap := map[uint32]uint32{}
	decoder := gob.NewDecoder(metaBuf)
	err = decoder.Decode(&offsetMap)
	if err != nil {
		panic(fmt.Sprintf("unable to decode map, error: %v", err))
	}
	return &table{
		data:      data,
		path:      path,
		fileInfo:  fi,
		size:      status.Size(),
		fp:        fp,
		status:    status,
		filter:    &filter,
		offsetMap: offsetMap,
		index:     index,
	}
}

func (t *table) Get(key []byte) ([]byte, bool) {
	c := crc32.New(CrcTable)
	c.Write(key)
	hash := c.Sum32()
	if !t.exist(hash) {
		return nil, false
	}
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

func (t *table) exist(hash uint32) bool {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, hash)
	return t.filter.Has(buf)
}

func (t *table) SeekBegin() {
	t.fp.Seek(0, io.SeekStart)
}

func (t *table) ID() uint32 {
	return t.index
}

func (t *table) iter() *iterator {
	return newIterator(t.fp, t.fileInfo.metaOffset)
}

func (t *table) close() {
	t.fp.Close()
}

func (t *table) entries() []uint32 {
	entries := make([]uint32, 0)
	for key := range t.offsetMap {
		entries = append(entries, key)
	}
	return entries
}
