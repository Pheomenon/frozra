package executor

import "github.com/Pheomenon/frozra/v1/executor/registry"

func Register(name string, fn interface{}) {
	registry.Default().Register(name, fn)
}
