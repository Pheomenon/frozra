package persistence

import "testing"

func TestStrategy(t *testing.T) {
	m := &metadata{
		L1Files: []tableMetadata{
			tableMetadata{MaxRange: 100, MinRange: 100},
		},
	}
	p := m.l1Status(tableMetadata{MaxRange: 100, MinRange: 100})
	if p.strategy != UNION {
		t.Fatalf("exptected UNION %d but got %d", UNION, p.strategy)
	}
	p = m.l1Status(tableMetadata{MaxRange: 400, MinRange: 300})
	if p.strategy != NOTUNION {
		t.Fatalf("exptected NOTUNION %d but got %d", NOTUNION, p.strategy)
	}
	m.L1Files = append(m.L1Files, tableMetadata{
		MaxRange: 300,
		MinRange: 200,
	})
	p = m.l1Status(tableMetadata{MaxRange: 450, MinRange: 250})
	if p.strategy != OVERLAPPING {
		t.Fatalf("exptected OVERLAPPING %d but got %d", OVERLAPPING, p.strategy)
	}
	p = m.l1Status(tableMetadata{MaxRange: 250, MinRange: 150})
	if p.strategy != OVERLAPPING {
		t.Fatalf("exptected OVERLAPPING %d but got %d", OVERLAPPING, p.strategy)
	}
}
