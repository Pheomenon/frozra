package persistence

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
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

//readTableV2 just decode table's map
func (r *tableReader) readTableV2(path string, fd uint32) *map[uint32]uint32 {
	path = r.tablePath(path, fd)
	fp, err := os.OpenFile(path, os.O_RDONLY, 0666)
	status, err := os.Stat(path)
	if err != nil {
		panic(fmt.Sprintf("unable to get table file status, error: %v", err))
	}
	dataRef, err := syscall.Mmap(int(fp.Fd()), int64(0), int(status.Size()), syscall.PROT_READ, syscall.MAP_PRIVATE)
	defer r.release(dataRef)
	defer fp.Close()
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
	return &offsetMap
}

func (r *tableReader) searchKey(path string, fd uint32, offsetMap map[uint32]uint32, hash uint32) (string, error) {
	path = r.tablePath(path, fd)
	fp, err := os.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		panic(fmt.Sprintf("unable to read table, error: %v", err))
	}
	defer fp.Close()

	var offset int64
	if _, ok := offsetMap[hash]; ok {
		offset = int64(offsetMap[hash])
	} else {
		return "", errors.New("not exist")
	}

	//in next version entry should be klen-vlen-vval-kval, it can help us to get vval
	//without knowing the length of key
	length := make([]byte, 4)
	_, err = fp.ReadAt(length, offset)
	if err != nil {
		panic(fmt.Sprintf("unable to read value's length, error: %v", err))
	}
	keyLen := r.byte2int64(length)

	_, err = fp.ReadAt(length, offset+4)
	if err != nil {
		panic(fmt.Sprintf("unable to read value's length, error: %v", err))
	}
	valLen := r.byte2int64(length)

	valByte := make([]byte, valLen)
	_, err = fp.ReadAt(valByte, offset+8+keyLen)
	if err != nil {
		panic(fmt.Sprintf("unable to read value's length, error: %v", err))
	}

	val := make([]byte, len(valByte))
	binary.Read(bytes.NewReader(valByte), binary.BigEndian, &val)

	return string(val), nil
}

func (r *tableReader) byte2int64(b []byte) int64 {
	var res int64
	for _, v := range b {
		res += int64(v)
	}
	return res
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

func (r *tableReader) release(dataRef []byte) {
	if syscall.Munmap(dataRef) != nil {
		logrus.Warnf("failed to munmap")
	}
}
