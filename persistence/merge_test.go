package persistence

import (
	"bytes"
	"fmt"
	"os"
	"testing"
)

func testTable(key, value string, begin, end int, idx uint32) *table {
	mem := newHashMap(64 << 20)
	for ; begin < end; begin++ {
		key := []byte(fmt.Sprintf("%s%d", key, begin))
		value := []byte(fmt.Sprintf("%s%d", value, begin))
		mem.Set(key, value)
	}
	mem.persistence("./", idx)
	return readTable("./", idx)
}

func testValueExist(key, value string, tb *table, begin, end int, t *testing.T) {
	for ; begin < end; begin++ {
		key := []byte(fmt.Sprintf("%s%d", key, begin))
		value := []byte(fmt.Sprintf("%s%d", value, begin))
		inv, exist := tb.Get(key)
		if !exist {
			t.Fatalf("%s value not found", string(value))
		}
		if bytes.Compare(value, inv) != 0 {
			t.Fatalf("expected value %s but got %s", string(value), string(inv))
		}
	}
}

func removeTestTable(idx uint32) {
	os.Remove(fmt.Sprintf("./%d.fza", idx))
}

//func TestBuilder(t *testing.T) {
//	t1 := testTable("hello", "xonlab", 1, 100, 1)
//	t2 := testTable("hello", "phenom", 101, 200, 2)
//	builder := newTableMerger(int(t1.size + t2.size))
//	t1.SeekBegin()
//	t2.SeekBegin()
//	builder.append(t1.fp, int64(t1.fileInfo.metaOffset))
//	builder.append(t2.fp, int64(t2.fileInfo.metaOffset))
//	builder.merge(t1.offsetMap, 0)
//	builder.merge(t2.offsetMap, uint32(t1.fileInfo.metaOffset))
//	buf := builder.finish()
//	fp, _ := os.Create("3.fza")
//	fp.Write(buf)
//	t3 := readTable("./", 3)
//	testValueExist("hello", "xonlab", t3, 1, 100, t)
//	testValueExist("hello", "phenom", t3, 101, 200, t)
//	removeTestTable(1)
//	removeTestTable(2)
//	removeTestTable(3)
//}
