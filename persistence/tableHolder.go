package persistence

import (
	"encoding/binary"
	"sync"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/Pheomenon/frozra/v1/persistence/util"
)

type fdKey struct {
	level uint8
	fd    uint32
	key   []byte
}

type tableHolder struct {
	table  []*table
	fdKey  chan fdKey
	l0     chan []byte
	l1     chan []byte
	reader *tableReader
	sync.RWMutex
}

func newTableHolder(path string) *tableHolder {
	holder := &tableHolder{
		table:  nil,
		fdKey:  make(chan fdKey, 4096),
		l0:     make(chan []byte), //todo: have buffer or not??
		l1:     make(chan []byte),
		reader: newTableReader(path),
	}
	go holder.search()
	go holder.eliminate()
	return holder
}

func (h *tableHolder) search() {
	for {
		select {
		case fk := <-h.fdKey:
			hash := util.Hashing(fk.key)
			tableFullName := h.reader.tablePath(h.reader.path, fk.fd)
			var t *table
			for _, item := range h.table {
				if item.path == tableFullName {
					t = item
					h.sendValue(item, hash, fk)
					break
				}
			}
			if t == nil {
				t = h.reader.readTable(h.reader.path, fk.fd)
				h.table = append(h.table, t)
				h.sendValue(t, hash, fk)
			}
			//if exist {
			//	h.table = append(h.table, t)
			//	return val, exist
			//}
		}
	}
}

func (h *tableHolder) sendValue(item *table, hash uint32, fk fdKey) {
	val, exist := searchKey(item, hash)
	if exist {
		if fk.level == 0 {
			h.l0 <- val
		} else {
			h.l1 <- val
		}
	} else {
		if fk.level == 0 {
			h.l0 <- nil
		} else {
			h.l1 <- nil
		}
	}
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

func (h *tableHolder) eliminate() {
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-ticker.C:
			if len(h.table) > 3 {
				h.Lock()
				h.release()
				h.Unlock()
			}
		}
	}
}

func (h *tableHolder) remove(fd uint32) {
	for i := 0; i < len(h.table); i++ {
		if h.table[i].index == fd {
			h.Lock()
			if syscall.Munmap(h.table[i].dataRef) != nil {
				logrus.Warnf("failed to munmap")
			}
			h.table = append(h.table[:i], h.table[i+1:]...)
			h.Unlock()
		}
	}
}

func (h *tableHolder) release() {
	if syscall.Munmap(h.table[0].dataRef) != nil {
		logrus.Warnf("failed to munmap")
	}
	h.table = h.table[1:]
}
