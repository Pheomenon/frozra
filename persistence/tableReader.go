package persistence

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
	"syscall"
)

type tableReader struct {
	path string
}

func newTableReader(path string) *tableReader {
	return &tableReader{
		path: path,
	}
}

// readTable return table's content
func (r *tableReader) readTable(path string, fd uint32) *table {
	path = r.tablePath(path, fd)
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
		index:     fd,
	}
}

func (r *tableReader) tablePath(abs string, fd uint32) string {
	return fmt.Sprintf("%s/%d.fza", abs, fd)
}
