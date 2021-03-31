package persistence

import (
	"bytes"
	"fmt"
	"github.com/sirupsen/logrus"
	"os"
	"sync"
	"testing"
	"xonlab.com/frozra/v1/conf"
)

func TestLSM(t *testing.T) {
	clean()
	setting := conf.LoadConfigure()
	l, err := New(setting.Persistence)
	if err != nil {
		t.Fatalf("Lsm is expected to open but got error %s", err.Error())
	}
	l.Set([]byte("hello"), []byte("phenom"))
	l.Close()
	l, err = New(setting.Persistence)
	if err != nil {
		t.Fatalf("Lsm is expected to open but got error %s", err.Error())
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

func produceEntry(l *Lsm, start, end int) {
	for i := start; i <= end; i++ {
		l.Set([]byte(fmt.Sprintf("key %d", i)), []byte(fmt.Sprintf("%d", i)))
	}
}

func TestClean(t *testing.T) {
	clean()
}

func TestConcurrent(t *testing.T) {
	clean()
	setting := conf.LoadConfigure()
	l, err := New(setting.Persistence)
	if err != nil {
		t.Fatalf("Lsm is expected to open but got error %s", err.Error())
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		for i := 0; i < 100; i++ {
			key := []byte("phenom" + string(rune(i)))
			value := []byte("froza" + string(rune(i)))
			l.Set(key, value)
		}
		wg.Done()
	}()
	go func() {
		for i := 100; i < 200; i++ {
			key := []byte("phenom" + string(rune(i)))
			value := []byte("froza" + string(rune(i)))
			l.Set(key, value)
		}
		wg.Done()
	}()
	wg.Wait()
	l.Close()
	wg.Add(1)
	l, err = New(setting.Persistence)
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
	l, err = New(setting.Persistence)
	wg.Add(1)
	wg.Add(1)
	wg.Add(1)
	go func() {
		for i := 0; i < 100; i++ {
			key := []byte("phenom" + string(rune(i)))
			value := []byte("froza" + string(rune(i)))
			v, exist := l.Get(key)
			if !exist {
				t.Fatalf("value not found for %s", string(key))
			}
			if bytes.Compare(value, v) != 0 {
				t.Fatalf("expected value %s but got %s", string(value), string(v))
			}
		}
		wg.Done()
	}()
	go func() {
		for i := 101; i < 200; i++ {
			key := []byte("phenom" + string(rune(i)))
			value := []byte("froza" + string(rune(i)))
			v, exist := l.Get(key)
			if !exist {
				t.Fatalf("value not found for %s", string(key))
			}
			if bytes.Compare(value, v) != 0 {
				t.Fatalf("expected value %s but got %s", string(value), string(v))
			}
		}
		wg.Done()
	}()
	go func() {
		for i := 101; i < 200; i++ {
			key := []byte("phenom" + string(rune(i)))
			value := []byte("froza" + string(rune(i)))
			v, exist := l.Get(key)
			if !exist {
				t.Fatalf("value not found for %s", string(key))
			}
			if bytes.Compare(value, v) != 0 {
				t.Fatalf("expected value %s but got %s", string(value), string(v))
			}
		}
		wg.Done()
	}()
	wg.Wait()
	l.Close()
}

func TestCompaction(t *testing.T) {
	clean()
	setting := conf.LoadConfigure()
	l, err := New(setting.Persistence)
	if err != nil {
		t.Fatalf("Lsm is expected to open but got error %s", err.Error())
	}
	produceEntry(l, 0, 100)
	l.Close()
	l, err = New(setting.Persistence)
	if err != nil {
		t.Fatalf("Lsm is expected to open but got error %s", err.Error())
	}
	produceEntry(l, 100, 200)
	l.Close()
	l, err = New(setting.Persistence)
	if err != nil {
		t.Fatalf("Lsm is expected to open but got error %s", err.Error())
	}
	produceEntry(l, 200, 300)
	l.Close()
	l, err = New(setting.Persistence)
	if err != nil {
		t.Fatalf("Lsm is expected to open but got error %s", err.Error())
	}
	produceEntry(l, 50, 200)
	l.Close()
	l, err = New(setting.Persistence)
	if err != nil {
		t.Fatalf("Lsm is expected to open but got error %s", err.Error())
	}
	for i := 0; i < 300; i++ {
		val, _ := l.Get([]byte(fmt.Sprintf("key %d", i)))
		if !bytes.Equal(val, []byte(fmt.Sprintf("%d", i))) {
			t.Fatalf("Lsm get a unexpected value %s", val)
		}
	}
	l.Close()
}

func TestDuplicateKey(t *testing.T) {
	clean()
	l := initLSM(t)
	var key, value []byte
	key = []byte("phenom")
	value = []byte("froza")
	l.Set(key, value)
	key = []byte("phenom")
	value = []byte("xonlab")
	l.Set(key, value)
	val, _ := l.Get([]byte("phenom"))
	if !bytes.Equal(val, value) {
		t.Fatalf("Lsm get a unexpected value %s", value)
	}
}

func initLSM(t *testing.T) *Lsm {
	setting := conf.LoadConfigure()
	l, err := New(setting.Persistence)
	if err != nil {
		t.Fatalf("Lsm is expected to open but got error %s", err.Error())
	}
	return l
}

func TestDuplicateKeyInL1(t *testing.T) {
	clean()
	l := initLSM(t)
	key := []byte("froza")
	for i := 0; i <= 1<<8; i++ {
		l.Set(key, []byte(fmt.Sprintf("%b", i)))
	}
	val, _ := l.Get([]byte("froza"))
	if !bytes.Equal(val, []byte(fmt.Sprintf("%b", 1<<8))) {
		t.Fatalf("Lsm get a unexpected value %s", val)
	}
}

func TestCompactL0(t *testing.T) {
	clean()
	l := initLSM(t)
	for i := 0; i < 100; i++ {
		l.Set([]byte(fmt.Sprintf("phenom%d", i)), []byte(fmt.Sprintf("froza%d", i)))
	}
	l.Close()
	l = initLSM(t)
	for i := 100; i < 200; i++ {
		l.Set([]byte(fmt.Sprintf("phenom%d", i)), []byte(fmt.Sprintf("froza%d", i)))
	}
	l.Close()
	l = initLSM(t)
	val, _ := l.Get([]byte("phenom66"))
	if !bytes.Equal(val, []byte("froza66")) {
		t.Fatalf("Lsm get a unexpected value %s", val)
	}
}

func TestLsm_GetInL0(t *testing.T) {
	clean()
	l := initLSM(t)
	for i := 0; i < 100; i++ {
		l.Set([]byte(fmt.Sprintf("key %d", i)), []byte(fmt.Sprintf("%d", i)))
	}
	l.Close()
	l = initLSM(t)
	val, _ := l.Get([]byte(fmt.Sprintf("key %d", 43)))
	if !bytes.Equal(val, []byte("43")) {
		t.Fatalf("Lsm get a unexpected value %s", val)
	}
}

func TestLsm_GetInL1(t *testing.T) {
	clean()
	l := initLSM(t)
	produceEntry(l, 0, 100)
	l.Close()
	l = initLSM(t)
	produceEntry(l, 100, 200)
	l.Close()
	l = initLSM(t)
	produceEntry(l, 200, 300)
	l.Close()
	l = initLSM(t)
	val, _ := l.Get([]byte(fmt.Sprintf("key %d", 32)))
	if !bytes.Equal(val, []byte("32")) {
		t.Fatalf("Lsm get a unexpected value %s", val)
	}
}

func TestLsm_Mixed(t *testing.T) {
	clean()
	l := initLSM(t)
	produceEntry(l, 0, 1<<24)
	l.Close()
	l = initLSM(t)
	for i := 0; i <= 1<<24; i++ {
		val, _ := l.Get([]byte(fmt.Sprintf("key %d", i)))
		logrus.Infof("got val %s", val)
		if !bytes.Equal(val, []byte(fmt.Sprintf("%d", i))) {
			t.Fatalf("except got %d, but got %s", i, val)
		}
	}
}

/*
go test -bench=. -benchtime=60s -run=none

go test -bench=. -benchtime=60s -cpuprofile=cpu.out -memprofile=mem.out -blockprofile=block.out -benchmem -run=none

go tool pprof -text cpu.out

go tool pprof -pdf cpu.out > cpu.pdf
go tool pprof -svg cpu.out > cpu.svg
*/
func BenchmarkLsm_Set(b *testing.B) {
	clean()
	setting := conf.LoadConfigure()
	l, _ := New(setting.Persistence)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Set([]byte(fmt.Sprintf("key %d", i)), []byte(fmt.Sprintf("%d", b.N)))
	}
}

func BenchmarkLsm_Get(b *testing.B) {
	clean()
	setting := conf.LoadConfigure()
	l, _ := New(setting.Persistence)
	produceEntry(l, 0, 1<<22)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Get([]byte(fmt.Sprintf("key %d", b.N)))
	}
}
