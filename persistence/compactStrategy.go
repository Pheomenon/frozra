package persistence

type l0PushDown int

const (
	UNION l0PushDown = iota
	OVERLAPPING
	NOTUNION
)

type compactionStrategy struct {
	strategy l0PushDown
	tableIDs []uint32
}

func (m *metadata) l1Status(victim tableMetadata) compactionStrategy {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	cs := compactionStrategy{
		tableIDs: make([]uint32, 0),
		strategy: NOTUNION,
	}

	for _, l1File := range m.L1Files {
		// If there is a union, will merge both direct
		if (l1File.MinRange <= victim.MinRange && l1File.MaxRange >= victim.MaxRange) || (l1File.MinRange >= victim.MinRange && l1File.MaxRange <= victim.MaxRange) {
			cs.strategy = UNION
			cs.tableIDs = append(cs.tableIDs, l1File.Index)
			return cs
		} else if (l1File.MinRange <= victim.MinRange && l1File.MaxRange > victim.MinRange) || (l1File.MinRange < victim.MaxRange && l1File.MaxRange >= victim.MaxRange) {
			cs.strategy = OVERLAPPING
			cs.tableIDs = append(cs.tableIDs, l1File.Index)
		}
	}
	return cs
}
