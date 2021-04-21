package persistence

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/sirupsen/logrus"
)

type iterator struct {
	currentOffset int
	metaOffset    int
	fp            *os.File
}

func newIterator(fp *os.File, metaOffset int) *iterator {
	fp.Seek(0, io.SeekStart)
	return &iterator{
		currentOffset: 0,
		metaOffset:    metaOffset,
		fp:            fp,
	}
}

func (i *iterator) hasNext() bool {
	hasNext := i.currentOffset == i.metaOffset
	if hasNext {
		i.fp.Close()
		return false
	}
	return true
}

func (i *iterator) next() ([]byte, []byte, []byte, []byte) {
	buf := make([]byte, 8)
	n, err := i.fp.Read(buf)
	if err != nil {
		logrus.Fatalf("iterator: failed during reading key and value length %s", err.Error())
	} else if n != 8 {
		logrus.Fatalf("iterator: failed to read key and value length expected 8 but got %d", n)
	}
	keyLength := binary.BigEndian.Uint32(buf[0:4])
	valLength := binary.BigEndian.Uint32(buf[4:8])
	kv := make([]byte, keyLength+valLength)
	n, err = i.fp.Read(kv)
	if err != nil {
		logrus.Fatalf("iterator: failed during reading key and value  %s", err.Error())
	} else if n != int(keyLength+valLength) {
		logrus.Fatalf("iterator: failed to read key and value  expected %d but got %d", keyLength+valLength, n)
	}
	i.currentOffset += 8 + int(keyLength) + int(valLength)
	return buf[0:4], buf[4:8], kv[0:keyLength], kv[keyLength : keyLength+valLength]
}
