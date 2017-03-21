package utils

import "sync"

type WrappedWaitGroup struct {
	sync.WaitGroup
}

func (w *WrappedWaitGroup) Wrap(fn func()) {
	w.Add(1)
	go func(){
		defer w.Done()
		fn()
	}()
}
