package registry

import (
	"errors"
	"sync"
)

var (
	defaultRegistry Registry
)

func init() {
	defaultRegistry = newRegistry()
}

func Default() Registry {
	return defaultRegistry
}

type Registry interface {
	Register(name string, fn interface{})
	Get(name string) (interface{}, error)
}

type registry struct {
	mu sync.Mutex
	r  map[string]interface{}
}

func newRegistry() Registry {
	r := new(registry)
	r.r = make(map[string]interface{})

	return r
}

func (r *registry) Register(name string, fn interface{}) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.r[name] = fn
}

func (r *registry) Get(name string) (interface{}, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	fn, ok := r.r[name]
	if !ok {
		return nil, errors.New("not found")
	}

	return fn, nil
}
