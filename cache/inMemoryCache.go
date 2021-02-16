package cache

import (
	"github.com/sirupsen/logrus"
	"sync"
	"time"
	"xonlab.com/frozra/v1/conf"
	"xonlab.com/frozra/v1/persistence"
)

type inMemoryCache struct {
	c map[string]value
	Stat
	isFull bool
	ttl    time.Duration
	lsm    *persistence.Lsm
	mutex  sync.RWMutex
}

type value struct {
	v       []byte
	created time.Time // the time of the last call to set
}

type pair struct {
	k string
	v []byte
}

func newInMemoryCache(ttl int) *inMemoryCache {
	configure := conf.LoadConfigure()
	lsm, err := persistence.New(configure.Persistence)
	if err != nil {
		logrus.Fatalf("init: create lsm error: %v", err)
	}
	c := &inMemoryCache{
		c:     make(map[string]value),
		lsm:   lsm,
		Stat:  Stat{},
		ttl:   time.Duration(ttl) * time.Second,
		mutex: sync.RWMutex{},
	}
	if ttl > 0 {
		go c.expirer()
	}
	go monit(configure.MemoryThreshold, configure.Interval, c)
	return c
}

func (c *inMemoryCache) Set(k string, v []byte) error {
	if !c.isFull {
		c.mutex.Lock()
		defer c.mutex.Unlock()
		c.c[k] = value{
			v:       v,
			created: time.Now(),
		}
		c.add(k, v)
	} else {
		c.lsm.Set([]byte(k), v)
	}
	return nil
}

func (c *inMemoryCache) Get(k string) ([]byte, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	if c.c[k].v == nil {
		res, exist := c.lsm.Get([]byte(k))
		if exist {
			return res, nil
		}
	}
	return c.c[k].v, nil
}

func (c *inMemoryCache) Del(k string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	v, exist := c.c[k]
	if exist {
		delete(c.c, k)
		c.del(k, v.v)
	}
	return nil
}

func (c *inMemoryCache) GetStat() Stat {
	return c.Stat
}

func (c *inMemoryCache) NewScanner() Scanner {
	pairCh := make(chan *pair)
	closeCh := make(chan struct{})
	go func() {
		defer close(pairCh)
		c.mutex.RLock()
		for k, v := range c.c {
			c.mutex.RUnlock()
			select {
			case <-closeCh:
				return
			case pairCh <- &pair{k, v.v}:
			}
			c.mutex.RLock()
		}
		c.mutex.RUnlock()
	}()
	return &inMemoryScanner{
		pair{},
		pairCh,
		closeCh,
	}
}

func (c *inMemoryCache) expirer() {
	for {
		time.Sleep(c.ttl)
		c.mutex.RLock()
		for k, v := range c.c {
			c.mutex.RUnlock()
			if v.created.Add(c.ttl).Before(time.Now()) {
				c.Del(k)
			}
			c.mutex.RLock()
		}
		c.mutex.RUnlock()
	}
}

type monitor struct {
	memoryThreshold int
	cache           *inMemoryCache
}

func monit(memoryThreshold, interval int, cache *inMemoryCache) {
	m := &monitor{
		memoryThreshold: memoryThreshold,
		cache:           cache,
	}
	monitorTicker := time.NewTicker(time.Second * time.Duration(interval))
	for {
		select {
		case <-monitorTicker.C:
			if (cache.Stat.KeySize+cache.Stat.ValueSize)*8>>30 > int64(m.memoryThreshold) {
				cache.isFull = true
			} else {
				cache.isFull = false
			}
		}
	}
}

type inMemoryScanner struct {
	pair
	pairCh  chan *pair
	closeCh chan struct{}
}

func (s *inMemoryScanner) Close() {
	close(s.closeCh)
}

func (s *inMemoryScanner) Scan() bool {
	p, ok := <-s.pairCh
	if ok {
		s.k, s.v = p.k, p.v
	}
	return ok
}

func (s *inMemoryScanner) Key() string {
	return s.k
}

func (s *inMemoryScanner) Value() []byte {
	return s.v
}
