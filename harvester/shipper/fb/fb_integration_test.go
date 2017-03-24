package fb

import (
	"testing"
	"github.com/dbjtech/golab/harvester/shipper"
	"github.com/elastic/beats/libbeat/common"
	"github.com/dbjtech/golab/harvester/harvesterd"
	"context"
	"net"
	"github.com/dbjtech/golab/harvester/shipper/fb/protocol/v1"
	"github.com/dbjtech/golab/harvester/libs/constant"
	"go.uber.org/zap"
	"github.com/dbjtech/golab/harvester/libs"
)

func mock_beat_client(addr string, ctx context.Context, msgC <-chan v1.Message) error {
	con, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	for {
		select {
		case msg := <-msgC:
			data, err := msg.Pack()
			if err != nil {
				return err
			}
			_, err = con.Write(data)
			if err != nil {
				return err
			}
		case <-ctx.Done():
			goto exit
		}
	}
exit:
	return nil
}

//func TestBeatsShipper_ShipOn(t *testing.T) {
//	logger := zap.NewNop()
//	errC := make(chan libs.Error)
//	fn, ok := shipper.GetShipperCreator("filebeat")
//	if !ok {
//		t.Log("expect to get filebeat creator, got nil")
//		t.Fail()
//	}
//	addr := "localhost:9876"
//	cfMap := map[string]interface{}{
//		"addr": addr,
//	}
//	cfg, err := common.NewConfigFrom(cfMap)
//	if err != nil {
//		panic(err)
//	}
//	fbShipper, err := fn("test_fb", cfg)
//	if err != nil {
//		t.Logf("fail to create fb shipper with err:%v\n", err)
//		t.Fail()
//	}
//	ch := make(chan []*harvesterd.Entry, 1)
//	ctx, stop := context.WithCancel(context.Background())
//	ctx = context.WithValue(ctx, constant.KEY_LOGGER, logger)
//	ctx = context.WithValue(ctx, constant.KEY_ERRORS_W_CHAN, (chan<-libs.Error)(errC))
//	// start shipping
//	err = fbShipper.ShipOn(ch, ctx)
//	if err != nil {
//		t.Logf("fail to start ship, err:%v\n", err)
//		stop()
//		t.Fail()
//	}
//	// start beating
//	msgC := make(chan v1.Message)
//	go func() {
//		err = mock_beat_client(addr, ctx, msgC)
//		t.Log("mock bye")
//		if err != nil {
//			t.Logf("mock err:%v\n", err)
//			t.Fail()
//		}
//	}()
//}
