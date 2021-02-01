package persistence

import (
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"sync"
	"sync/atomic"
)

type tableMetadata struct {
	MaxRange uint32
	MinRange uint32
	Index    uint32
	Size     uint32
	Records  uint32
	Density  float32
}

type descendingList []tableMetadata

func (t descendingList) Len() int {
	return len(t)
}

func (t descendingList) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

func (t descendingList) Less(i, j int) bool {
	return t[i].Density > t[j].Density
}

type metadata struct {
	L0Files   []tableMetadata
	L1Files   []tableMetadata
	NextIndex uint32
	mutex     sync.RWMutex
}

func loadMetadata(absPath string) (*metadata, error) {
	metadataName := path.Join(absPath, "metadata")
	_, err := os.Stat(metadataName)
	if err != nil {
		if !os.IsExist(err) {
			return createMetadata(metadataName)
		} else {
			panic(fmt.Sprintf("check metadata error: %v", err))
		}
	}

	fp, err := os.Open(metadataName)
	if err != nil {
		panic(fmt.Sprintf("load metadata error: %v", err))
	}
	m := &metadata{}
	decoder := gob.NewDecoder(fp)
	err = decoder.Decode(m)
	if err != nil && err != io.EOF {
		return nil, err
	}
	return m, nil
}

func createMetadata(metadataName string) (*metadata, error) {
	fp, err := os.Create(metadataName)
	if err != nil {
		panic(fmt.Sprintf("create metadata error: %v", err))
	}
	fp.Close()
	return &metadata{
		L0Files:   make([]tableMetadata, 0),
		L1Files:   make([]tableMetadata, 0),
		NextIndex: 0,
	}, nil
}

func (m *metadata) nextFileID() uint32 {
	atomic.AddUint32(&m.NextIndex, 1)
	return m.NextIndex
}

func (m *metadata) save(absPath string) error {
	metadataName := path.Join(absPath, "metadata")
	fp, err := os.OpenFile(metadataName, os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	defer fp.Close()
	encoder := gob.NewEncoder(fp)
	return encoder.Encode(m)

}

func (m *metadata) addL0File(records, minRange, maxRange uint32, size int, index uint32) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.L0Files = append(m.L0Files, tableMetadata{
		Records:  records,
		MinRange: minRange,
		MaxRange: maxRange,
		Size:     uint32(size),
		Density:  float32(records) / float32(maxRange-minRange),
		Index:    index,
	})
}

func (m *metadata) addL1File(records, minRange, maxRange uint32, size int, index uint32) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.L1Files = append(m.L1Files, tableMetadata{
		Records:  records,
		MinRange: minRange,
		MaxRange: maxRange,
		Size:     uint32(size),
		Density:  float32(records) / float32(maxRange-minRange),
		Index:    index,
	})
}

func (m *metadata) l0Len() int {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return len(m.L0Files)
}

func (m *metadata) l1Len() int {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return len(m.L1Files)
}

func (m *metadata) sortL0() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	sort.Sort(descendingList(m.L0Files))
}

func (m *metadata) deleteL0Table(index uint32) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	for i := 0; i < len(m.L0Files); i++ {
		if m.L0Files[i].Index == index {
			m.L0Files[i] = m.L0Files[len(m.L0Files)-1]
			m.L0Files = m.L0Files[:len(m.L0Files)-1]
			break
		}
	}
}

func (m *metadata) deleteL1Table(index uint32) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	for i := 0; i < len(m.L1Files); i++ {
		if m.L1Files[i].Index == index {
			m.L1Files[i] = m.L1Files[len(m.L1Files)-1]
			m.L1Files = m.L1Files[:len(m.L1Files)-1]
			break
		}
	}
}

func (m *metadata) copyL0() []tableMetadata {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.L0Files
}

func (m *metadata) copyL1() []tableMetadata {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.L1Files
}
