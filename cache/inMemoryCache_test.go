package cache

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
)

func clean() {
	os.Remove("./1.fza")
	os.Remove("./2.fza")
	os.Remove("./3.fza")
	os.Remove("./4.fza")
	os.Remove("./5.fza")
	os.Remove("./6.fza")
	os.Remove("./7.fza")
	os.Remove("./8.fza")
	os.Remove("./9.fza")
	os.Remove("./10.fza")
	os.Remove("./11.fza")
	os.Remove("./12.fza")
	os.Remove("./metadata")
	os.Remove("./filter")
}

func TestClean(t *testing.T) {
	clean()
}

func produceEntry(m *inMemoryCache, start, end int) {
	for i := start; i <= end; i++ {
		_ = m.Set(fmt.Sprintf("key %s", strconv.Itoa(i)), []byte(fmt.Sprintf("%d", i)))
	}
}

func TestInMemoryCache_Get(t *testing.T) {
	m := newInMemoryCache(30)
	produceEntry(m, 0, 1<<8)
	for i := 0; i <= 1<<8; i++ {
		val, _ := m.Get(fmt.Sprintf("key %s", strconv.Itoa(i)))
		if !bytes.Equal([]byte(fmt.Sprintf("%d", i)), val) {
			t.Fatalf("got unexpect value: %s", val)
		}
	}
}

func TestInMemoryCache_Concurrent(t *testing.T) {
	clean()
	var wg sync.WaitGroup
	m := newInMemoryCache(30)
	produceEntry(m, 0, 1<<8)
	wg.Add(32)
	for i := 0; i < 32; i++ {
		go func() {
			for j := 0; j <= 1<<8; j++ {
				val, _ := m.Get(fmt.Sprintf("key %s", strconv.Itoa(j)))
				if !bytes.Equal([]byte(fmt.Sprintf("%d", j)), val) {
					t.Fatalf("got unexpect value: %s", val)
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
}
