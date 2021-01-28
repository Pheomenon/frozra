package persistence

type Setting struct {
	L0FileCapacity int
	memTableSize   int
	path           string
	maxL1Size      int
}

func DefaultSetting() Setting {
	return Setting{
		L0FileCapacity: 3,
		memTableSize:   64 << 20, // 64MB
		path:           "./",
		maxL1Size:      64 << 21,
	}
}
