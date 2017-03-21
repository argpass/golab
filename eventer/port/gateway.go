package port

import (
	"github.com/dbjtech/golab/pending/wrappers"
	"github.com/dbjtech/golab/eventer/config"
)

// GatewayPort
type GatewayPort struct {
	redis *wrappers.MyRedis
	cfg *config.Config
}

func NewGateWayPort(cfg *config.Config) (*GatewayPort, error) {
	redisCf := cfg.GW.REDIS
	client, err := wrappers.NewRedisClient(
		redisCf.Host, redisCf.Port, redisCf.Password, redisCf.DB)
	if err != nil {
		return nil, err
	}
	gw := &GatewayPort{redis:client, cfg:cfg}
	return gw, nil
}

// PollPacket polls to get packet from the Gateway RPC Queue
func (gw *GatewayPort) PollPacket(timeout int) ([]byte, error) {
	// todo: timeout will occurs an error ?
	result, err := gw.redis.Blpop(gw.cfg.GW.QUEUE_NAME, timeout)
	if err != nil {
		return nil, err
	}
	raw := result[1]
	return raw, nil
}

// SendResp put response packet to the Gateway RPC Queue
func (gw *GatewayPort) SendResp(resp_queue_name string, resp []byte) error {
	err := gw.redis.Rpush(resp_queue_name, resp)
	return err
}
