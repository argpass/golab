package db

import (
	"context"
	"fmt"
	"sync"

	"github.com/elastic/beats/libbeat/common"
	"github.com/pkg/errors"
)

type IsEngine interface {
	Ensure(db string, cfg *common.Config) error

	// Open a connection
	Open(db string) (IsDBCon, error)
}

// EngineCreator create an instance of some Engine and enable it
type EngineCreator func(name string, cf *common.Config, ctx context.Context) (IsEngine, error)

var engines = &engineRegistry{engines: make(map[string]IsEngine)}

type engineRegistry struct {
	mu      sync.RWMutex
	engines map[string]IsEngine
}

func (r *engineRegistry) RegisterEngine(name string, engine IsEngine) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.engines[name]; !ok {
		r.engines[name] = engine
	} else {
		panic(errors.New(fmt.Sprintf("engine `%s` alreay exists", name)))
	}
}

func (r *engineRegistry) GetEngine(name string) (engine IsEngine, exists bool) {
	r.mu.RLock()
	engine, exists = r.engines[name]
	r.mu.RUnlock()
	return
}

//var (
//	engines = map[string]EngineCreator{}
//	mu      = &sync.RWMutex{}
//)
//
//func RegisterEngine(name string, fn EngineCreator) {
//	mu.Lock()
//	if _, ok := engines[name]; !ok {
//		engines[name] = fn
//	}
//	mu.Unlock()
//}
//
//func GetEngine(name string) (EngineCreator, bool) {
//	mu.RLock()
//	en, ok := engines[name]
//	mu.RUnlock()
//	return en, ok
//}
