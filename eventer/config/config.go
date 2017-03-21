package config

import (
	"encoding/json"
	"errors"
	"io/ioutil"
)

// GWPort is the Gateway RPC redis configuration
type GWPort struct {
	REDIS RedisConfig `json:"redis"`
	QUEUE_NAME string `json:"queue_name"`
}

type DBConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	UserName string `json:"username"`
	Password string `json:"password"`
	DB       string `json:"database"`
}

type RedisConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Password string `json:"password"`
	DB       int    `json:"db"`
}

type SystemConfig struct {
	// 协程数量(0:cpu数量)
	CoNum int `json:"co_num"`
	// 运行模式(debug|deploy)
	Mode string `json:"mode"`
	// 日志级别(debug|info|warn|critical)
	LogLevel string `json:"level"`
	// eventer in queue
	QueueIn string `json:"queue_in"`
}

type Config struct {
	DB     DBConfig     	`json:"db"`
	REDIS  RedisConfig  	`json:"redis"`
	SYSTEM SystemConfig 	`json:"system"`
	GW     GWPort  		`json:"gw"`
}

func newDefaultConfig() *Config {
	cfg := Config{
		DBConfig{"localhost", 3306, "", "", ""},
		RedisConfig{"localhost", 6973, "", 0},
		// TODO[!] resolve queueIn
		SystemConfig{0, "debug", "debug", "gateway:001"},
	}
	return &cfg
}

func checkConfig(c *Config) error {
	if c.DB.UserName == "" || c.DB.Password == "" {
		return errors.New("Bad db config with empty username or password")
	}
	return nil
}

// ParseConfigFrom parses config object with config file path
func ParseConfigFrom(path string) (*Config, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	c := newDefaultConfig()
	err = json.Unmarshal(data, c)
	if err != nil {
		return nil, err
	}

	err = checkConfig(c)
	if err != nil {
		return nil, err
	}

	return c, nil
}
