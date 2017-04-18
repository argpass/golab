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
)

func init()  {
	shipper.RegisterShipper("mock", func(name string, cfg *common.Config) (shipper.IsShipper, error) {
		return &mockShipper{}, nil
	})
}

type mockShipper struct {
}

func (*mockShipper) ShipOn(sendC chan<- *libs.Entry, ctx context.Context) error {
	logger := ctx.Value(constant.KEY_LOGGER).(*zap.Logger).With(zap.String("mod", "mock shipper"))
	go func() {
		logger.Debug("start")
		defer logger.Info("bye")
		var et *libs.Entry
		t_start := time.Now().Unix()
		for i:=0; i < 800000; i++ {
			t := time.Now().UnixNano()
			sn := fmt.Sprintf("sn_%d", time.Now().Unix())
			
			et = &libs.Entry{
				Timestamp:uint64(time.Now().Unix()),
				Body:fmt.Sprintf("[I %d 20170808] hello from mock data asfefawef, " +
					"jwefjawejfalksjfkwheifahkeflkqefajwejfaejfkajfkaejfsjfkajklewjflkadsj" +
					"fahwefjalksejflakjefjwekfjwkejflkawejflkjfjherhwajefkdje [[[]]]] wefwf\nwefwef\nwefwefwfw" +
					"flkeklfjkwjfa" +
					"wjeioawiefjaw", t),
				Type:"live_eventer",
				Fields:map[string]libs.Value{},
			}
			et.AddStringTag("mock")
			et.AddStringField("sn", sn)
			select {
			case <-ctx.Done():
				return
			case sendC<-et:
			}
		}
		t_end := time.Now().Unix()
		logger.Debug(fmt.Sprintf("mock done with cost:%d", t_end - t_start))
		
	}()
	return nil
}
 

