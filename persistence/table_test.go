package persistence

import (
	"bytes"
	"fmt"
	"os"
	"testing"
)

func TestTableGet(t *testing.T) {
	hashMap := NewHashMap(64 << 20)
	for i := 0; i < 65535; i++ {
		key := []byte(fmt.Sprintf("Phenom%d", i))
		value := []byte(fmt.Sprintf("Xonlab%d", i))
		hashMap.Set(key, value)
	}
	hashMap.persistence("./", 1)
	table := newTable("./", 1)
	v, exist := table.Get([]byte(fmt.Sprintf("Phenom%d", 65534)))
	if !exist {
		t.Fatal("key not found in the hashmap")
	}
	if bytes.Compare([]byte(fmt.Sprintf("Xonlab%d", 65534)), v) != 0 {
		t.Fatalf("expected value %s but got value %s", "nanbare65534", string(v))
	}
	os.Remove("./1.fza")
}
