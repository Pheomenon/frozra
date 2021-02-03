package persistence

import "testing"

func TestLowerRange(t *testing.T) {
	tr := NewIndexer()
	tr.put(45, 1)
	tr.put(43, 12)
	tr.put(20, 2)
	tr.put(80, 3)
	tr.put(70, 4)
	tr.put(50, 5)
	n := tr.floor(72)
	if n.minimumKey != 70 {
		t.Fatalf("expected 70 but got %d", n.minimumKey)
	}
	n = tr.floor(20)
	if n.minimumKey != 20 {
		t.Fatalf("expected 20 but got %d", n.minimumKey)
	}
	n = tr.floor(92)
	if n.minimumKey != 80 {
		t.Fatalf("expected 80 but got %d", n.minimumKey)
	}
	n = tr.floor(69)
	if n.minimumKey != 50 {
		t.Fatalf("expected 50 but got %d", n.minimumKey)
	}
	n = tr.floor(2)
	if n != nil {
		t.Fatalf("expected nil node but got %v", n)
	}
}

func TestDeleteTable(t *testing.T) {
	tr := NewIndexer()
	tr.put(34, 1)
	tr.put(32, 5)
	tr.put(31, 4)
	tr.put(34, 20)
	tr.put(32, 24)
	tr.put(31, 10)
	tr.delete(34)
	tr.delete(32)
	tr.delete(31)
	tr.delete(20)
	tr.delete(24)
	tr.delete(10)
	if tr.root.right != nil || tr.root.left != nil {
		t.Fatalf("expected root to be nil but got %+v", tr)
	}
}
