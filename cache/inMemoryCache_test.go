package cache

import (
	"fmt"
	"os"
	"strconv"
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

func TestInMemoryCache_Set(t *testing.T) {
	m := newInMemoryCache(60)
	produceEntry(m, 0, 1<<30)

	produceEntry(m, 1<<16, 1<<17)
}
