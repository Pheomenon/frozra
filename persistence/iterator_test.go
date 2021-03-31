package persistence

import "testing"

func TestIterator(t *testing.T) {
	tb := testTable("phenom", "frozra", 1, 100, 1)
	iter := tb.iter()
	records := 0
	for iter.hasNext() {
		iter.next()
		records++
	}
	removeTestTable(1)
	if records != 99 {
		t.Fatalf("expected 99 records but got %d", records)
	}
}
