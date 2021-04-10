package persistence

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"io"
	"os"

	"github.com/sirupsen/logrus"
)

// tableMerger used to merge two table into memory buffer
type tableMerger struct {
	buf       *bytes.Buffer
	offsetMap map[uint32]uint32
	min       uint32
	max       uint32
}

func newTableMerger(size int) *tableMerger {
	buf := new(bytes.Buffer)
	buf.Grow(size)
	return &tableMerger{
		buf:       buf,
		offsetMap: map[uint32]uint32{},
		min:       0,
		max:       0,
	}
}

func (t *tableMerger) Min() uint32 {
	return t.min
}

func (t *tableMerger) Max() uint32 {
	return t.max
}

// append data to the buffer
func (t *tableMerger) append(fp *os.File, limit int64) {
	writer := bufio.NewWriter(t.buf)
	n, err := io.CopyN(writer, fp, limit)
	if err != nil {
		logrus.Fatalf("tableMerger: unable to append data while mering %s", err.Error())
	} else if limit != n {
		logrus.Fatalf("tableMerger: unable to append completely. expected %d but got %d", limit, n)
	}
}

func (t *tableMerger) add(keyLength, valLength, key, val []byte, hash uint32) {
	offset := t.buf.Len()
	t.offsetMap[hash] = uint32(offset)
	t.setMax(hash)
	t.setMin(hash)

	n, err := t.buf.Write(keyLength)
	if err != nil {
		logrus.Fatalf("tableMerger: unable to insert key length %s", err.Error())
	} else if n != len(keyLength) {
		logrus.Fatalf("tableMerger: key length  is not written completly expected %d but got %d", len(keyLength), n)
	}

	n, err = t.buf.Write(valLength)
	if err != nil {
		logrus.Fatalf("tableMerger: unable to insert value length %s", err.Error())
	} else if n != len(valLength) {
		logrus.Fatalf("tableMerger: value length is not written completly expected %d but got %d", len(valLength), n)
	}

	n, err = t.buf.Write(key)
	if err != nil {
		logrus.Fatalf("tableMerger: unable to insert key %s", err.Error())
	} else if len(key) != n {
		logrus.Fatalf("tableMerger: key is not written completly expected %d but got %d", len(key), n)
	}

	n, err = t.buf.Write(val)
	if err != nil {
		logrus.Fatalf("tableMerger: unable to insert value %s", err.Error())
	} else if len(val) != n {
		logrus.Fatalf("tableMerger: value is not written completly expected %d but got %d", len(val), n)
	}
}

func (t *tableMerger) setMin(min uint32) {
	if t.min == 0 {
		t.min = min
		return
	} else if t.min > min {
		t.min = min
	}
}

func (t *tableMerger) setMax(max uint32) {
	if t.max == 0 {
		t.max = max
		return
	} else if t.max < max {
		t.max = max
	}
}

// merge hashmap and make filter for all the key, then write it to disk
func (t *tableMerger) merge(left map[uint32]uint32, offsetAdder uint32) {
	for key, value := range left {
		t.offsetMap[key] = value + offsetAdder
		t.setMin(key)
		t.setMax(key)
	}
}

func (t *tableMerger) appendFileInfo(fi *fileInfo) {
	fib := make([]byte, 32)
	fi.Encode(fib)
	n, err := t.buf.Write(fib)
	if err != nil {
		logrus.Fatalf("tableMerger: unable to append file info %s", err.Error())
	}
	if n != 32 {
		logrus.Fatalf("tableMerger: unable to append file info completly expected %d got %d", 32, n)
	}
}

// setTableInfo setup table's info
func (t *tableMerger) setTableInfo() []byte {
	slots := len(t.offsetMap)
	buf := make([]byte, 4)
	for key := range t.offsetMap {
		binary.BigEndian.PutUint32(buf, key)
	}
	mo := t.buf.Len()
	fi := &fileInfo{
		metaOffset: mo,
		minRange:   t.min,
		maxRange:   t.max,
		entries:    slots,
	}
	e := gob.NewEncoder(t.buf)
	err := e.Encode(t.offsetMap)

	if err != nil {
		logrus.Fatalf("tableMerger: unable to encode merged hashmap %s", err.Error())
	}
	if err != nil {
		logrus.Fatalf("tableMerger: unable to write filter to the buffer %s", err.Error())
	}
	t.appendFileInfo(fi)
	return t.buf.Bytes()
}
