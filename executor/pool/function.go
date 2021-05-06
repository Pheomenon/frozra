package pool

import (
	"errors"
	"reflect"
)

type function struct {
	fn   reflect.Value
	args []reflect.Value
}

func newFunction(fn interface{}, args ...interface{}) (*function, error) {
	fnVal := reflect.ValueOf(fn)
	if len(args) != fnVal.Type().NumIn() {
		return nil, errors.New("invalid parameter length")
	}

	f := new(function)
	f.fn = fnVal

	f.args = make([]reflect.Value, 0, len(args))
	for _, arg := range args {
		f.args = append(f.args, reflect.ValueOf(arg))
	}

	return f, nil
}

func (f *function) call() {
	f.fn.Call(f.args)
}
