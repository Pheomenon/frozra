package persistence

import (
	"encoding/binary"
	"github.com/sirupsen/logrus"
	"sync"
	"syscall"
	"time"
	"xonlab.com/frozra/v1/persistence/util"
)

type tableHolder struct {
	table  []*table
	keys   chan []byte
	reader *tableReader
	sync.RWMutex
}

func newTableHolder(path string) *tableHolder {
	holder := &tableHolder{
		table:  nil,
		keys:   make(chan []byte, 1024),
		reader: newTableReader(path),
	}
	go holder.eliminate()
	return holder
}

func (h *tableHolder) search(fd uint32) ([]byte, bool) {
	for {
		select {
		case key := <-h.keys:
			hash := util.Hashing(key)
			tableFullName := util.TablePath(h.reader.path, fd)
			for _, item := range h.table {
				if item.path == tableFullName {
					return searchKey(item, hash)
				}
			}
			t := readTable(h.reader.path, fd)
			val, exist := searchKey(t, hash)
			if exist {
				h.table = append(h.table, t)
				return val, exist
			}
			return nil, false
		}
	}
}

func (h *tableHolder) eliminate() {
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-ticker.C:
			if len(h.table) > 3 {
				h.Lock()
				h.release()
				h.RUnlock()
			}
		}
	}
}

func (h *tableHolder) release() {
	if syscall.Munmap(h.table[0].dataRef) != nil {
		logrus.Warnf("failed to munmap")
	}
	h.table = h.table[1:]
}

func searchKey(t *table, hash uint32) ([]byte, bool) {
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
