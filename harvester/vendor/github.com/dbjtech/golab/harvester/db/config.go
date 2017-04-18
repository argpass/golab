package db

import "github.com/elastic/beats/libbeat/common"

type DatabaseConfig struct {
	Name        string              `config:"name"`
	Engine      string              `config:"engine"`
	EgConfig    *common.Config      `config:"engine_options"`
}

type Config struct {
	Databases   []DatabaseConfig    `config:"databases"`
}
