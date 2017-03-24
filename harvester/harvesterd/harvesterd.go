package harvesterd

import (
	"github.com/dbjtech/golab/pending/utils"
	"context"
	"github.com/dbjtech/golab/harvester/libs/constant"
	"github.com/elastic/beats/libbeat/common"
	"go.uber.org/zap"
	"sync"
	"github.com/dbjtech/golab/harvester/db"
	"fmt"
	"github.com/dbjtech/golab/harvester/libs"
)

type Harvesterd struct {
	wg          utils.WrappedWaitGroup
	entriesC    <-chan *libs.Entry
	rawConfig   *common.Config
	cfg         *Config
	lock        sync.RWMutex
	dbCons      map[string]db.IsDBCon
	dbService   *db.DBService

	// filtersMap: filters per entry type
	filtersMap  map[string] []IsFilter
	logger      *zap.Logger
	errC        chan <- libs.Error
	ctx         context.Context
}

func New(cfg *common.Config, recvC <-chan *libs.Entry, dbService *db.DBService) (*Harvesterd, error)  {
	var cf Config
	err := cfg.Unpack(&cf)
	if err != nil {
		return nil, err
	}
	return &Harvesterd{
		rawConfig:cfg, entriesC:recvC, cfg: &cf,
		dbCons:make(map[string]db.IsDBCon),
		dbService:dbService,
	}, nil
}

func (h *Harvesterd) running() {
	h.wg.Wrap(func(){
		h.pumping()
	})
	
	h.wg.Wait()
	h.logger.Info("bye")
}

func (h *Harvesterd) Fatal(err error, msg string) {
	select {
	case <-h.ctx.Done():
	case h.errC <- libs.Error{Err:err, Message:msg, IsFatal:true}:
		h.logger.Error(fmt.Sprintf("fatal err:%v, msg:%s", err, msg))
	}
}

func (h *Harvesterd) handle(et *libs.Entry) {
	var err error
	h.lock.RLock()
	cf, ok := h.cfg.TypeRouting[et.Type]
	if !ok {
		h.lock.RUnlock()
		h.Fatal(nil, fmt.Sprintf("no such doctype (%s) config", et.Type))
		return
	}
	
	// resolve connection
	con, ok := h.dbCons[cf.SaveToDb]
	if !ok {
		h.lock.RUnlock()
		
		// create a new connection
		con, err = h.dbService.Open(cf.SaveToDb)
		if err != nil {
			h.Fatal(err, fmt.Sprintf("fail to open connection for db %s", cf.SaveToDb))
			return
		}
		h.lock.Lock()
		h.dbCons[cf.SaveToDb] = con
		h.lock.Unlock()
		
		// continue to hold read lock
		h.lock.RLock()
	}
	
	err = con.Save(et)
	if err != nil {
		h.lock.RUnlock()
		
		// delete invalid connection
		h.lock.Lock()
		delete(h.dbCons, cf.SaveToDb)
		h.lock.Unlock()
		
		h.Fatal(err, "fail to save entry")
		return
	}
	
	// todo: filters and do filter
	h.lock.RUnlock()
}

func (h *Harvesterd) pumping()  {
	for {
		select {
		case <-h.ctx.Done():
			goto exit
		case et := <-h.entriesC:
			h.handle(et)
		}
	}
exit:
	h.logger.Info("pumping bye")
}

// Start the Harvesterd service
// todo: start multi workers to consume the entries
func (h *Harvesterd) Start(ctx context.Context) error {
	parentWG := ctx.Value(constant.KEY_P_WG).(*utils.WrappedWaitGroup)
	h.ctx = ctx
	h.logger = ctx.Value(constant.KEY_LOGGER).(*zap.Logger).
		With(zap.String("mod", "harvesterd"))
	h.errC = ctx.Value(constant.KEY_ERRORS_W_CHAN).(chan <- libs.Error)
	parentWG.Wrap(func() {
		h.running()
	})
	return nil
}

