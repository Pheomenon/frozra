package persistence

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/sirupsen/logrus"
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
	dataRef   []byte // file reference provided by mmap
	status    os.FileInfo
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
	dataRef, err := syscall.Mmap(int(fp.Fd()), int64(0), int(status.Size()), syscall.PROT_READ, syscall.MAP_PRIVATE)
	if err != nil {
		panic(fmt.Sprintf("unable to mmap: %v", err))
	}
	fi := &fileInfo{}
	// get file info
	fi.Decode(dataRef[status.Size()-32 : status.Size()])

	metaBuf := new(bytes.Buffer)
	// metaBuf saved all map's entry in this table
	metaBuf.Write(dataRef[fi.metaOffset : status.Size()-32])
	offsetMap := map[uint32]uint32{}
	decoder := gob.NewDecoder(metaBuf)
	err = decoder.Decode(&offsetMap)
	if err != nil {
		panic(fmt.Sprintf("unable to decode map, error: %v", err))
	}
	return &table{
		data:      dataRef[0:fi.metaOffset], // this field stored table's content
		path:      path,
		fileInfo:  fi,
		dataRef:   dataRef,
		size:      status.Size(),
		fp:        fp,
		status:    status,
		offsetMap: offsetMap,
		index:     index,
	}
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

func (t *table) release() {
	if syscall.Munmap(t.dataRef) != nil {
		logrus.Warnf("failed to munmap")
	}
	t = nil
}
