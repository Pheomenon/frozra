package cache

import (
	"fmt"
	"strconv"
	"testing"
)

func produceEntry(m *inMemoryCache, start, end int) {
	for i := start; i <= end; i++ {
		_ = m.Set(fmt.Sprintf("key %s", strconv.Itoa(i)), []byte(fmt.Sprintf("%d", i)))
	}
}

func TestInMemoryCache_Set(t *testing.T) {
	m := newInMemoryCache(60)
	produceEntry(m, 0, 1<<32)
}
