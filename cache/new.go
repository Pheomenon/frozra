package cache

import "log"

func New(typ string, ttl int) Cache {
	var c Cache
	if typ == "inmemory" {
		c = newInMemoryCache(ttl)
	}
	if typ == "rocksdb" {
		c = newRocksdbCache(ttl)
	}
	if c == nil {
		panic("Unknown cache type " + typ)
	}
	log.Println(typ, "ready to serve")
	return c
}
