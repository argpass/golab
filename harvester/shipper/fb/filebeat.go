package fb

import (
	"github.com/dbjtech/golab/harvester/harvesterd"
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
	shipper.RegisterShipper("filebeat", newFbShipper)
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
	sendC       chan <-[]*harvesterd.Entry
	errorC      chan <-libs.Error
}

func (s *BeatsShipper) ShipOn(
		sendC chan <-[]*harvesterd.Entry, ctx context.Context) error {
	
	logger := ctx.Value(constant.KEY_LOGGER).(*zap.Logger)
	logger = logger.With(zap.String("shipper", s.name))
	ctx = context.WithValue(ctx, constant.KEY_LOGGER, logger)
	s.errorC = ctx.Value(constant.KEY_ERRORS_W_CHAN).(chan <- libs.Error)
	s.logger = logger
	s.sendC = sendC
	s.ctx, s.stop = context.WithCancel(ctx)
	
	// handle done signal
	go func(){
		select {
		case <-ctx.Done():
			s.stop()
			s.wg.Wait()
		case <-s.ctx.Done():
		}
	}()
	
	// start tcp server for `filebeats`
	tcpListener, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		s.logger.Error(fmt.Sprintf("listen err:%s", err))
		return err
	}
	handler := &tcpServer{sendC:s.sendC}
	s.wg.Wrap(func(){
		RunTCPServer(s.ctx, tcpListener.(*net.TCPListener), handler)
	})
	return nil
}

func newBeatsShipper(name string, cf *Config) *BeatsShipper {
	s := &BeatsShipper{
		cfg:cf,
		name: name,
	}
	return s
}

