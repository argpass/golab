package ari

import (
	"github.com/elastic/beats/libbeat/outputs"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/outputs/mode"
	"github.com/elastic/beats/libbeat/common/op"
	"github.com/elastic/beats/libbeat/outputs/mode/modeutil"
	"time"
	"github.com/elastic/beats/libbeat/outputs/transport"
	"fmt"
)

const (
	defaultWaitRetry = 2 * time.Second
	defaultMaxWaitRetry = 60 * time.Second
)

func init() {
	outputs.RegisterOutputPlugin("ari", new)
}

var _ outputs.Outputer = &AriOut{}

// AriOut is `Outputer` for `Ari`
type AriOut struct {
	mode 		mode.ConnectionMode
	beatName 	string
}

func (a *AriOut) Close() error {
	return a.mode.Close()
}

func (a *AriOut) PublishEvent(sig op.Signaler, opts outputs.Options, data outputs.Data) error {
	return a.mode.PublishEvent(sig, opts, data)
}

func (a *AriOut) BulkPublish(sig op.Signaler, opts outputs.Options, data []outputs.Data) error {
	return a.mode.PublishEvents(sig, opts, data)
}

func (a *AriOut) init(cfg *common.Config, topologyExpire int) error {
	// resolve config
	// copy defaults
	config := defaultConfig
	// merge with customized cfg
	if err := cfg.Unpack(&config); err != nil {
		return err
	}
	// make clients
	transpCfg := &transport.Config{
		Timeout:config.Timeout,
		Proxy:nil, // no proxy
		TLS: nil, // no tls
		Stats:&transport.IOStats{
			Read:statReadBytes,
			Write:statWriteBytes,
			ReadErrors:statReadErrors,
			WriteErrors:statWriteErrors,
		},
	}
	clients, err := modeutil.MakeClients(cfg, func(host string) (mode.ProtocolClient, error){
		// create a transport for the new client
		t, err := transport.NewClient(transpCfg, transportProtocol, host, config.DefaultPort)
		if err != nil {
			return nil, err
		}
		fmt.Printf("make client with host %s cfg:%v\n", host, cfg)
		// make client
		return newClient(t), nil
	})
	if err != nil {
		return err
	}
	// resolve connection mode
	m, err := modeutil.NewConnectionMode(clients, modeutil.Settings{
		Failover: !config.LoadBalance,
		MaxAttempts: 1,
		Timeout: config.Timeout,
		WaitRetry: defaultWaitRetry,
		MaxWaitRetry: defaultMaxWaitRetry,
	})
	if err != nil {
		return err
	}
	a.mode = m
	return nil
}

// new implements `OutputBuilder` for the ari client
func new(beatName string, cfg *common.Config, topologyExpire int) (outputs.Outputer, error)  {
	a := &AriOut{
		beatName:beatName,
	}
	if err := a.init(cfg, topologyExpire); err != nil {
		return nil, err
	}
	return a, nil
}
