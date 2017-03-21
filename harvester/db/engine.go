package db

import (
	"context"
	"sync"
	"github.com/elastic/beats/libbeat/common"
)

type IsEngine interface {
	
	Ensure(db string, cfg *common.Config) error
	
	Open(db string) (IsDBCon, error)
}

// EngineCreator create an instance of some Engine and enable it
type EngineCreator func(name string, cf *common.Config, ctx context.Context) (IsEngine, error)

var (
	creators    = map[string] EngineCreator{}
	mu          = &sync.RWMutex{}
)

func RegisterEngine(name string, fn EngineCreator) {
	mu.Lock()
	if _, ok := creators[name]; !ok {
		creators[name] = fn
	}
	mu.Unlock()
}

func GetEngine(name string) (EngineCreator, bool) {
	mu.RLock()
	en, ok := creators[name]
	mu.RUnlock()
	return en, ok
}
