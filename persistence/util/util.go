package util

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"os"
)

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
