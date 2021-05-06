package persistence

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestGetSet(t *testing.T) {
	hashMap := newHashMap(1024)
	key := []byte("Phenom")
	value := []byte("Xonlab")
	hashMap.Set(key, value)
	inv, exist := hashMap.Get(key)
	if !exist {
		t.Fatal("key not found in the hashmap")
	}
	if bytes.Compare(value, inv) != 0 {
		t.Fatalf("expected value %s but got value %s", string(value), string(inv))
	}
}

func TestGetSet65535(t *testing.T) {
	// default map buf size is 64M bit
	hashMap := newHashMap(64 << 20)
	for i := 0; i < 65535; i++ {
		key := []byte("Phenom" + string(rune(i)))
		value := []byte("Xonlab" + string(rune(i)))
		hashMap.Set(key, value)
		v, exist := hashMap.Get(key)
		if !exist {
			t.Fatal("key not found in the hashmap")
		}
		if bytes.Compare(value, v) != 0 {
			t.Fatalf("expected value %s but got value %s", string(value), string(v))
		}
	}
}

func TestSaveFile(t *testing.T) {
	hashMap := newHashMap(64 << 20)
	for i := 0; i < 65535; i++ {
		key := []byte("Phenom" + string(rune(i)))
		value := []byte("Xonlab" + string(rune(i)))

		hashMap.Set(key, value)
		v, exist := hashMap.Get(key)
		if !exist {
			t.Fatal("key not found in the hashmap")
		}
		if bytes.Compare(value, v) != 0 {
			t.Fatalf("expected value %s but got value %s", string(value), string(v))
		}
	}
	hashMap.persistence("./", 1)
	filePath, err := filepath.Abs("./")
	if err != nil {
		panic("unable to form path for flushing the disk")
	}

	if _, err := os.Stat(fmt.Sprintf("%s/%d.fza", filePath, 1)); os.IsNotExist(err) {
		panic("file not exist")
	}
	//os.Remove(fmt.Sprintf("%s/%d.fza", filePath, 1))
}
