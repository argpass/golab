package harvesterd

import (
	"context"
	"github.com/argpass/golab/harvester/libs/constant"
	"github.com/elastic/beats/libbeat/common"
	"go.uber.org/zap"
	"sync"
	"github.com/argpass/golab/harvester/db"
	"fmt"
	"github.com/argpass/golab/harvester/libs"
	"github.com/pkg/errors"
	"github.com/argpass/golab/pending/utils"
)

type Harvesterd struct {
	wg          utils.IsGroup
	entriesC    <-chan *libs.Entry
	rawConfig   *common.Config
	cfg         *Config
	lock        sync.RWMutex
	dbCons      map[string]db.IsDBCon
	dbService   *db.DBService
	fatalC      chan error

	// filtersMap: filters per entry type
	filtersMap  map[string] []IsFilter
	logger      *zap.Logger
	ctx         context.Context
}

func New(
	cfg *common.Config,
	recvC <-chan *libs.Entry,
	dbService *db.DBService) (*Harvesterd, error)  {
	
	var cf Config
	err := cfg.Unpack(&cf)
	//fmt.Printf("cf:%+v, %v\n", cf, cfg.HasField("type_routing"))
	if err != nil {
		return nil, err
	}
	return &Harvesterd{
		rawConfig:cfg, entriesC:recvC, cfg: &cf,
		dbCons:make(map[string]db.IsDBCon),
		dbService:dbService,
		fatalC:make(chan error, 1),
	}, nil
}

func (h *Harvesterd) Fatal(err error)  {
	h.lock.Lock()
	select {
	case h.fatalC <- err:
	case <-h.ctx.Done():
	}
	h.lock.Unlock()
}

func (h *Harvesterd) running() error {
	
	h.wg.Go(func() error {
		select {
		case <-h.ctx.Done():
			return h.ctx.Err()
		case err := <-h.fatalC:
			return err
		}
	})
	
	h.wg.Go(func() error {
		return h.pumping()
	})
	
	// wait any routine to return an error
	err := h.wg.Wait()
	h.logger.Info("bye")
	// raise the err to parents
	return err
}

func (h *Harvesterd) handle(et *libs.Entry) {
	var err error
	h.lock.RLock()
	cf, ok := h.cfg.TypeRouting[et.Type]
	if !ok {
		h.lock.RUnlock()
		h.Fatal(errors.New(fmt.Sprintf("no such doctype (%s) config", et.Type)))
		return
	}
	
	// resolve connection
	con, ok := h.dbCons[cf.SaveToDb]
	if !ok {
		h.lock.RUnlock()
		
		// create a new connection
		con, err = h.dbService.Open(cf.SaveToDb)
		if err != nil {
			h.Fatal(errors.New(fmt.Sprintf("fail to open connection for db %s", cf.SaveToDb)))
			return
		}
		h.lock.Lock()
		h.dbCons[cf.SaveToDb] = con
		h.lock.Unlock()
		
		// continue to hold read lock
		h.lock.RLock()
	}
	
	err = h.dbCons[cf.SaveToDb].Save(et)
	if err != nil {
		h.lock.RUnlock()
		
		// delete invalid connection
		h.lock.Lock()
		delete(h.dbCons, cf.SaveToDb)
		h.lock.Unlock()
		
		h.Fatal(errors.Wrap(err, "fail to save entry"))
		return
	}
	
	// todo: filters and do filter
	h.lock.RUnlock()
}

func (h *Harvesterd) pumping()  error {
	defer h.logger.Info("pumping bye")
	
	for {
		select {
		case <-h.ctx.Done():
			return h.ctx.Err()
		case et := <-h.entriesC:
			h.handle(et)
		}
	}
}

// Start the Harvesterd service
// todo: start multi workers to consume the entries
func (h *Harvesterd) Start(ctx context.Context) error {
	parentWG := ctx.Value(constant.KEY_P_WG).(utils.IsGroup)
	h.logger = ctx.Value(constant.KEY_LOGGER).(*zap.Logger).
		With(zap.String("mod", "harvesterd"))
	h.wg = utils.NewGroup(ctx)
	h.ctx = h.wg.Context()
	parentWG.Go(func() error {
		return h.running()
	})
	return nil
}

