package pool

import (
	"errors"
	"fmt"
	"runtime"
	"sync"

	"github.com/Pheomenon/frozra/v1/executor/registry"
	"github.com/sirupsen/logrus"
)

const DefaultPoolCapacity = 32

type Pool interface {
	Submit(name string, args ...interface{}) error
	Shutdown()
}

func NewPool(cap int32) (Pool, error) {
	if cap <= 0 {
		cap = DefaultPoolCapacity
		logrus.Warningf("executor pool use default cap: [%v]", cap)
	}

	p := new(pool)
	p.capacity = cap
	p.executors = make([]*executor, cap, cap)

	return p, nil
}

type pool struct {
	mu sync.Mutex

	capacity int32
	current  int32

	executors []*executor

	shutdown bool
}

func (p *pool) Submit(name string, args ...interface{}) error {
	if p.shutdown {
		return errors.New("executor pool is shut")
	}

	f, err := registry.Default().Get(name)
	if err != nil {
		return fmt.Errorf("get function error: [%v]", err)
	}

	fn, err := newFunction(f, args...)
	if err != nil {
		return fmt.Errorf("new function error: [%v]", err)
	}

	e := p.getExecutor()
	e.run(fn)

	return nil
}

func (p *pool) Shutdown() {
	p.shutdown = true
	for p.current != 0 {
		runtime.Gosched()
	}
}

func (p *pool) getExecutor() *executor {
	var e *executor
	blocking := false

	p.mu.Lock()
	if p.current >= p.capacity-1 {
		blocking = true
	} else {
		e = p.executors[p.current]
		p.executors[p.current] = nil
		p.current++
	}
	p.mu.Unlock()

	if blocking {
		for {
			p.mu.Lock()
			if p.current >= p.capacity-1 {
				p.mu.Unlock()
				runtime.Gosched()
				continue
			}
			e = p.executors[p.current]
			p.executors[p.current] = nil
			p.current++
			p.mu.Unlock()
			break
		}
	}

	if e == nil {
		return newExecutor(p)
	}

	return e
}

func (p *pool) putExecutor(e *executor) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.executors[p.current] = e
	p.current--
}
