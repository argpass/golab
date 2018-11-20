package utils

import (
	"sync"
	
	"golang.org/x/net/context"
	"fmt"
)

type IsGroup interface {
	Wait() error
	Go(func()error)
	Cancel(err error)
	Context() context.Context
}

// A Group is a collection of goroutines working on subtasks that are part of
// the same overall task.
//
// A zero Group is valid and does not cancel on error.
type group struct {
	cancel func()
	ctx context.Context
	
	wg sync.WaitGroup
	
	errOnce sync.Once
	err     error
}

func (g *group) Cancel(err error) {
	g.errOnce.Do(func() {
		if err != nil {
			fmt.Printf("cancelled with err:%+v\n", err)
		}
		g.err = err
		if g.cancel != nil {
			g.cancel()
		}
	})
}

func (g *group) Context() context.Context {
	return g.ctx
}

// NewGroup returns a new Group and an associated Context derived from ctx.
//
// The derived Context is canceled the first time a function passed to Go
// returns a non-nil error or the first time Wait returns, whichever occurs
// first.
func NewGroup(ctx context.Context) (IsGroup) {
	ctx, cancel := context.WithCancel(ctx)
	return &group{cancel: cancel, ctx:ctx}
}

// Wait blocks until all function calls from the Go method have returned, then
// returns the first non-nil error (if any) from them.
func (g *group) Wait() error {
	g.wg.Wait()
	if g.cancel != nil {
		g.cancel()
	}
	return g.err
}

// Go calls the given function in a new goroutine.
//
// The first call to return a non-nil error cancels the group; its error will be
// returned by Wait.
func (g *group) Go(f func() error) {
	g.wg.Add(1)
	
	go func() {
		defer g.wg.Done()
		
		if err := f(); err != nil {
			g.Cancel(err)
		}
	}()
}

