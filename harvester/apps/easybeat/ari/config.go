package ari

import (
	"time"
	"expvar"
)

type AriConfig struct {
	Timeout 	    time.Duration	`config:"timeout"`
	LoadBalance 	bool		    `config:"loadbalance"`
	DefaultPort 	int 		    `config:"default_port"`
}

func (c *AriConfig) Validate() error {
	return nil
}

var (
	defaultConfig = AriConfig{
		Timeout: 10 * time.Second,
		LoadBalance: false,
		DefaultPort: 9736,
	}

	statReadBytes 		= expvar.NewInt("libbeat.ari.pub.read_bytes")
	statWriteBytes 		= expvar.NewInt("libbeat.ari.pub.write_bytes")
	statReadErrors 		= expvar.NewInt("libbeat.ari.pub.read_errors")
	statWriteErrors 	= expvar.NewInt("libbeat.ari.pub.write_errors")

	transportProtocol 	= "tcp"
)

