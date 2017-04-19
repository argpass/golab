package mock

import (
	"github.com/dbjtech/golab/harvester/shipper"
	"github.com/elastic/beats/libbeat/common"
	"github.com/dbjtech/golab/harvester/libs"
	"context"
	"time"
	"fmt"
	"github.com/dbjtech/golab/harvester/libs/constant"
	"go.uber.org/zap"
	"math/rand"
	"github.com/pkg/errors"
)

func init()  {
	shipper.RegisterShipper("mock", func(name string, cfg *common.Config) (shipper.IsShipper, error) {
			
		config := NewDefaultConfig()
		err := cfg.Unpack(&config)
		if err != nil {
			return nil, errors.Wrap(err, "unpack config")
		}
		
		rand.Seed(time.Now().UnixNano())
		mocker, err := NewLogMocker(config.MockFile, config.MaxRowCount)
		if err != nil {
			return nil, err
		}
		status, err := NewStatusAware(config.DumpFile)
		if err != nil {
			return nil, err
		}
		return &mockShipper{mocker:mocker, cfg: &config, status:status}, nil
	})
}

type mockShipper struct {
	mocker *LogMocker
	cfg *Config
	status *StatusAware
}

func (m *mockShipper) ShipOn(
	sendC chan<- *libs.Entry, ctx context.Context) error {
	
	logger := ctx.Value(constant.KEY_LOGGER).
		(*zap.Logger).With(zap.String("mod", "mock shipper"))
	
	go func() {
		time.Sleep(8 * time.Second)
		logger.Debug(fmt.Sprintf("start with config:%+v", *m.cfg))
		defer logger.Info("bye")
		
		for {
			var et *libs.Entry
			t_start := time.Now().Unix()
			rawSize := 0
			batchNum := m.cfg.BatchNum
			for i:=0; i < batchNum; i++ {
				et = m.mocker.RandomEntry()
				select {
				case <-ctx.Done():
					return
				case sendC<-et:
				}
				rawSize += len(et.Body)
			}
			t_end := time.Now().Unix()
			cost := t_end - t_start
			m.status.status.Num += batchNum
			m.status.status.Size += rawSize
			m.status.status.Cost += int(cost)
			logger.Info(
				fmt.Sprintf("mock batch done with %+v", *m.status.status))
			err := m.status.Flush()
			if err != nil {
				fmt.Printf("flush err:%+v\n", err)
				return
			}
		}
	}()
	return nil
}
 

