package cache

import (
	"sync"
	"time"
)

type inMemoryCache struct {
	c     map[string]value
	mutex sync.RWMutex
	Stat
	ttl time.Duration
}

type value struct {
	v       []byte
	created time.Time // the time of the last call to set
}

func (c *inMemoryCache) Set(k string, v []byte) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.c[k] = value{
		v:       v,
		created: time.Now(),
	}
	c.add(k, v)
	return nil
}

func (c *inMemoryCache) Get(k string) ([]byte, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
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
		c.mutex.RUnlock()
		for k, v := range c.c {
			c.mutex.RUnlock()
			if v.created.Add(c.ttl).Before(time.Now()) {
				c.Del(k)
			}
			c.mutex.RUnlock()
		}
		c.mutex.RUnlock()
	}
}

func newInMemoryCache(ttl int) *inMemoryCache {
	c := &inMemoryCache{
		c:     make(map[string]value),
		mutex: sync.RWMutex{},
		Stat:  Stat{},
	}
	if ttl > 0 {
		go c.expirer()
	}
	return c
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
