package pool

import (
	"runtime/debug"

	"github.com/sirupsen/logrus"
)

type executor struct {
	p *pool
}

func newExecutor(p *pool) *executor {
	e := new(executor)
	e.p = p

	return e
}

func (e *executor) run(fn *function) {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				logrus.Errorf("executor recovered from panic with error: [%v]", err)
				debug.PrintStack()
			}
			e.p.putExecutor(e)
		}()
		fn.call()
	}()
}
