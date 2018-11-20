package shipper

import (
	"github.com/elastic/beats/libbeat/common"
	"sync"
	"context"
	"github.com/argpass/golab/harvester/libs"
	"fmt"
)

// IsShipper
type IsShipper interface {
	
	// ShipOn ships entries to the `sendC`
	// never to block on the method
	ShipOn(sendC chan<- *libs.Entry, ctx context.Context) error
}

// Creator is factory method of a shipper
type Creator func(name string, cfg *common.Config) (IsShipper, error)

// registry holds all registered shipper Creator
// mu is the lock of registry
var registry = map[string]Creator {}
var mu = &sync.Mutex{}

// RegisterShipper registers a Factory method for the shipper of `name`
// if shipper of `name` already exists, the first one remains
func RegisterShipper(name string, fn Creator) {
	mu.Lock()
	defer mu.Unlock()
	if _, ok := registry[name]; !ok {
		registry[name] = fn
	}
}

// GetShipperCreator by name,
// if not exist, ok is false
func GetShipperCreator(name string) (fn Creator, ok bool) {
	mu.Lock()
	if fn, ok = registry[name]; ok {
		mu.Unlock()
		return fn, ok
	}
	mu.Unlock()
	return nil, false
}

type shipperService struct {
	rawConfig   *common.Config
	sendC       chan <- *libs.Entry
	
	ctx         context.Context
}

func (s *shipperService) Start(ctx context.Context) error {
	//parentWG := ctx.Value(constant.KEY_P_WG).(*utils.WrappedWaitGroup)
	s.ctx = ctx
	err := s.enableShippers()
	if err != nil {
		return err
	}
	return nil
}

func (s *shipperService) enableShippers() error {
	for _, sname := range s.rawConfig.GetFields() {
		cf, err := s.rawConfig.Child(sname, -1)
		if err != nil {
			return err
		}
		creator, ok := GetShipperCreator(sname)
		if !ok {
			return fmt.Errorf("no such shipper %s registered", sname)
		}
		sp, err := creator(sname, cf)
		if err != nil {
			return err
		}
		err = sp.ShipOn(s.sendC, s.ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

// New create a shipperService instance
func New(cfg *common.Config, sendC chan <- *libs.Entry) (libs.Starter, error) {
	return &shipperService{rawConfig:cfg, sendC:sendC,}, nil
}

