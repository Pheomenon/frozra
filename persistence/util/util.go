package util

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"hash/crc32"
	"os"
)

var CrcTable = crc32.MakeTable(crc32.Castagnoli)

func InArray(arr []uint32, val uint32) (index int, exists bool) {
	exists = false
	index = -1
	for i, u := range arr {
		if val == u {
			index = i
			exists = true
			return
		}
	}
	return
}

func Hashing(key []byte) uint32 {
	c := crc32.New(CrcTable)
	c.Write(key)
	hash := c.Sum32()
	return hash
}

func TablePath(abs string, index uint32) string {
	return fmt.Sprintf("%s/%d.fza", abs, index)
}

func RemoveTable(abs string, idx uint32) {
	tp := TablePath(abs, idx)
	err := os.Remove(tp)
	if err != nil {
		logrus.Errorf("unable to delete the %d table", idx)
	}
	logrus.Infof("compaction: remove %d table", idx)
}
