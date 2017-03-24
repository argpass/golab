package fb

import (
	"github.com/dbjtech/golab/harvester/shipper"
	"github.com/elastic/beats/libbeat/common"
	"context"
	"github.com/dbjtech/golab/harvester/libs/constant"
	"github.com/dbjtech/golab/harvester/libs"
	"github.com/dbjtech/golab/pending/utils"
	"net"
	"fmt"
	"go.uber.org/zap"
)

func init()  {
	shipper.RegisterShipper("fb", newFbShipper)
}

func newFbShipper(name string, cfg *common.Config) (shipper.IsShipper, error) {
	var myCf Config
	err := cfg.Unpack(&myCf)
	if err != nil {
		return nil, err
	}
	var s shipper.IsShipper = newBeatsShipper(name, &myCf)
	return s, nil
}

type BeatsShipper struct {
	name        string
	cfg         *Config
	wg          utils.WrappedWaitGroup
	ctx         context.Context
	logger      *zap.Logger
	stop        context.CancelFunc
	sendC       chan []*libs.Entry
	errorC      chan <-libs.Error
}

func (s *BeatsShipper) running()  {
	s.wg.Wait()
	s.logger.Info("bye")
}

func (s *BeatsShipper) ShipOn(
		sendC chan <- *libs.Entry,
		ctx context.Context) error {
	
	// init
	parentWG := ctx.Value(constant.KEY_P_WG).(*utils.WrappedWaitGroup)
	logger := ctx.Value(constant.KEY_LOGGER).(*zap.Logger)
	logger = logger.With(zap.String("mod", s.name))
	ctx = context.WithValue(ctx, constant.KEY_LOGGER, logger)
	s.errorC = ctx.Value(constant.KEY_ERRORS_W_CHAN).(chan <- libs.Error)
	s.logger = logger
	s.ctx, s.stop = context.WithCancel(ctx)
	
	s.wg.Wrap(func(){
		for {
			select {
			case <-ctx.Done():
				goto exit
			case ets := <-s.sendC:
				// send entries to the sendC
				for _, et := range ets {
					select {
					case <-ctx.Done():
						goto exit
					case sendC <- et:
						// pass
					}
				}
			}
		}
		exit:
	})
	
	// start tcp server for `filebeats`
	tcpListener, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		s.logger.Error(fmt.Sprintf("listen err:%s", err))
		return err
	}
	handler := &tcpServer{sendC:s.sendC}
	s.wg.Wrap(func() {
		select {
		case <-s.ctx.Done():
			err := tcpListener.Close()
			s.logger.Info(fmt.Sprintf("listener closed, err:%+v", err))
		}
	})
	s.wg.Wrap(func(){
		RunTCPServer(s.ctx, tcpListener.(*net.TCPListener), handler)
	})
	
	parentWG.Wrap(func(){
		s.running()
	})
	
	return nil
}

func newBeatsShipper(name string, cf *Config) *BeatsShipper {
	s := &BeatsShipper{
		cfg:cf,
		name: name,
		sendC:make(chan []*libs.Entry, 100),
	}
	return s
}

