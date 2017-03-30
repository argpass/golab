package fb

import (
	"github.com/dbjtech/golab/harvester/shipper"
	"github.com/elastic/beats/libbeat/common"
	"context"
	"github.com/dbjtech/golab/harvester/libs/constant"
	"github.com/dbjtech/golab/harvester/libs"
	"net"
	"fmt"
	"go.uber.org/zap"
	"github.com/dbjtech/golab/pending/utils"
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
	wg          utils.IsGroup
	ctx         context.Context
	logger      *zap.Logger
	sendC       chan []*libs.Entry
}

func (s *BeatsShipper) running() error {
	err := s.wg.Wait()
	s.logger.Info("bye")
	return err
}

func (s *BeatsShipper) ShipOn(
		sendC chan <- *libs.Entry,
		ctx context.Context) error {
	
	// init
	parentWG := ctx.Value(constant.KEY_P_WG).(utils.IsGroup)
	logger := ctx.Value(constant.KEY_LOGGER).(*zap.Logger)
	logger = logger.With(zap.String("mod", s.name))
	ctx = context.WithValue(ctx, constant.KEY_LOGGER, logger)
	s.logger = logger
	
	s.wg = utils.NewGroup(ctx)
	s.ctx = s.wg.Context()
	
	s.wg.Go(func() error {
		for {
			select {
			case <-s.ctx.Done():
				return s.ctx.Err()
			case ets := <-s.sendC:
				// send entries to the sendC
				for _, et := range ets {
					select {
					case <-s.ctx.Done():
						return s.ctx.Err()
					case sendC <- et:
						// pass
					}
				}
			}
		}
		return nil
	})
	
	// start tcp server for `filebeats`
	tcpListener, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		s.logger.Error(fmt.Sprintf("listen err:%s", err))
		return err
	}
	handler := &tcpServer{sendC:s.sendC}
	s.wg.Go(func() error {
		select {
		case <-s.ctx.Done():
			err := tcpListener.Close()
			s.logger.Info(fmt.Sprintf("listener closed, err:%+v", err))
		}
		return nil
	})
	s.wg.Go(func() error {
		return RunTCPServer(s.ctx, tcpListener.(*net.TCPListener), handler)
	})
	
	parentWG.Go(func() error {
		return s.running()
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

