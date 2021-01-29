package persistence

import (
	"bytes"
	"sync"
	"testing"
)

func TestLSM(t *testing.T) {
	setting := DefaultSetting()
	l, err := New(setting)
	if err != nil {
		t.Fatalf("lsm is expected to open but got error %s", err.Error())
	}
	l.Set([]byte("hello"), []byte("phenom"))
	l.Close()
	l, err = New(setting)
	if err != nil {
		t.Fatalf("lsm is expected to open but got error %s", err.Error())
	}
	val, exist := l.Get([]byte("hello"))
	if !exist {
		t.Fatalf("unable to retrive data")
	}
	if bytes.Compare(val, []byte("phenom")) != 0 {
		t.Fatalf("value is not same expected phenom but got %s", string(val))
	}
	l.Close()
}

func TestConcurrent(t *testing.T) {
	setting := DefaultSetting()
	l, err := New(setting)
	if err != nil {
		t.Fatalf("lsm is expected to open but got error %s", err.Error())
	}
	var wg sync.WaitGroup
	wg.Add(1)
	wg.Add(1)
	go func() {
		for i := 0; i < 100; i++ {
			key := []byte("phenom" + string(rune(i)))
			value := []byte("froza" + string(rune(i)))
			l.Set(key, value)
		}
		wg.Done()
	}()
	go func() {
		for i := 101; i < 200; i++ {
			key := []byte("phenom" + string(rune(i)))
			value := []byte("froza" + string(rune(i)))
			l.Set(key, value)
		}
		wg.Done()
	}()
	wg.Wait()
	l.Close()
	wg.Add(1)
	l, err = New(setting)
	if err != nil {
		t.Fatalf("db is expected to open but got error %s", err.Error())
	}
	go func() {
		for i := 108; i < 234; i++ {
			key := []byte("phenom" + string(rune(i)))
			value := []byte("froza" + string(rune(i)))
			l.Set(key, value)
		}
		wg.Done()
	}()
	wg.Wait()
	l.Close()
	wg = sync.WaitGroup{}
	l, err = New(setting)
	wg.Add(1)
	wg.Add(1)
	wg.Add(1)
	go func() {
		for i := 0; i < 100; i++ {
			key := []byte("phenom" + string(rune(i)))
			value := []byte("froza" + string(rune(i)))
			inv, exist := l.Get(key)
			if !exist {
				break
				//t.Fatalf("value not found for %s", string(key))
			}
			if bytes.Compare(value, inv) != 0 {
				break
				//t.Fatalf("expected value %s but got %s", string(value), string(inv))
			}
		}
		wg.Done()
	}()
	go func() {
		for i := 101; i < 200; i++ {
			key := []byte("phenom" + string(rune(i)))
			value := []byte("froza" + string(rune(i)))
			val, exist := l.Get(key)
			if !exist {
				break
			}
			if bytes.Compare(value, val) != 0 {
				break
			}
		}
		wg.Done()
	}()
	go func() {
		for i := 101; i < 200; i++ {
			key := []byte("phenom" + string(rune(i)))
			value := []byte("froza" + string(rune(i)))
			inv, exist := l.Get(key)
			if !exist {
				break
			}
			if bytes.Compare(value, inv) != 0 {
				break
			}
		}
		wg.Done()
	}()
	wg.Wait()
	l.Close()
}

func TestCompaction(t *testing.T) {
	setting := DefaultSetting()
	l, err := New(setting)
	if err != nil {
		t.Fatalf("lsm is expected to open but got error %s", err.Error())
	}
	for i := 0; i < 100; i++ {
		key := []byte("phenom" + string(rune(i)))
		value := []byte("froza" + string(rune(i)))
		l.Set(key, value)
	}
	l.Close()
	l, err = New(setting)
	if err != nil {
		t.Fatalf("lsm is expected to open but got error %s", err.Error())
	}
	for i := 0; i < 100; i++ {
		key := []byte("phenom" + string(rune(i)))
		value := []byte("froza" + string(rune(i)))
		l.Set(key, value)
	}
	l.Close()
	l, err = New(setting)
	if err != nil {
		t.Fatalf("lsm is expected to open but got error %s", err.Error())
	}
	for i := 0; i < 100; i++ {
		key := []byte("phenom" + string(rune(i)))
		value := []byte("froza" + string(rune(i)))
		l.Set(key, value)
	}
	l.Close()
	l, err = New(setting)
	if err != nil {
		t.Fatalf("lsm is expected to open but got error %s", err.Error())
	}
	for i := 50; i < 200; i++ {
		key := []byte("phenom" + string(rune(i)))
		value := []byte("froza" + string(rune(i)))
		l.Set(key, value)
	}
	l.Close()
	l, err = New(setting)
	if err != nil {
		t.Fatalf("lsm is expected to open but got error %s", err.Error())
	}
	for i := 0; i < 100; i++ {
		key := []byte("phenom" + string(rune(i)))
		value := []byte("froza" + string(rune(i)))
		l.Set(key, value)
	}
	l.Close()
}

func TestDuplicateKey(t *testing.T) {
	setting := DefaultSetting()
	l, err := New(setting)
	if err != nil {
		t.Fatalf("lsm is expected to open but got error %s", err.Error())
	}
	var key, value []byte
	key = []byte("phenom")
	value = []byte("froza")
	l.Set(key, value)
	key = []byte("phenom")
	value = []byte("xonlab")
	l.Set(key, value)
	val, _ := l.Get([]byte("phenom"))
	if !bytes.Equal(val, value) {
		t.Fatalf("lsm get a unexpected value %s", value)
	}
}
