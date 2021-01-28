package persistence

import "testing"

func TestLowerRange(t *testing.T) {
	tr := NewTree()
	tr.put(45, 1)
	tr.put(20, 2)
	tr.put(80, 3)
	tr.put(70, 4)
	tr.put(50, 5)
	n := tr.largestLowerRange(72)
	if n.lowerRange != 70 {
		t.Fatalf("expected 70 but got %d", n.lowerRange)
	}
	n = tr.largestLowerRange(20)
	if n.lowerRange != 20 {
		t.Fatalf("expected 20 but got %d", n.lowerRange)
	}
	n = tr.largestLowerRange(92)
	if n.lowerRange != 80 {
		t.Fatalf("expected 80 but got %d", n.lowerRange)
	}
	n = tr.largestLowerRange(69)
	if n.lowerRange != 50 {
		t.Fatalf("expected 50 but got %d", n.lowerRange)
	}
	n = tr.largestLowerRange(2)
	if n != nil {
		t.Fatalf("expected nil node but got %v", n)
	}

	ns := tr.allLargestRange(72)
	if len(ns) != 4 {
		t.Fatalf("expected 4 but got %d", len(ns))
	}
	for i := range ns {
		if i == 0 {
			continue
		}
		if ns[i].lowerRange > ns[i-1].lowerRange {
			t.Fatalf("expected in decrement order")
		}
	}
}

func TestFindAllLargestRange(t *testing.T) {
	tr := NewTree()
	tr.put(45, 1)
	tr.put(45, 2)
	tr.put(45, 3)
	res := tr.allLargestRange(46)
	if len(res[0].index) != 3 {
		t.Fatalf("expected 3 but got %d", len(res[0].index))
	}
}

func TestDeleteTable(t *testing.T) {
	tr := NewTree()
	tr.put(34, 1)
	tr.put(32, 5)
	tr.put(31, 4)
	tr.put(34, 20)
	tr.put(32, 24)
	tr.put(31, 10)
	tr.deleteTable(1)
	tr.deleteTable(5)
	tr.deleteTable(4)
	tr.deleteTable(20)
	tr.deleteTable(24)
	tr.deleteTable(10)
	if tr.root != nil {
		t.Fatalf("expected root to be nil but got %+v", tr.root)
	}
}
