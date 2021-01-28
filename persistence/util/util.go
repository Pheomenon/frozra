package util

import "fmt"

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
