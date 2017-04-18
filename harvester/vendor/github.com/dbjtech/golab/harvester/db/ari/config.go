package ari

import (
	"time"
	"regexp"
	"strconv"
	"fmt"
	"github.com/pkg/errors"
	"math"
)

var durationUnits = map[byte]time.Duration {
	'w': 7 * 24 * time.Hour,
	'd': 24 * time.Hour,
	'h': 1 * time.Hour,
}

func parseDuration(data string) (du time.Duration, err error) {
	p := regexp.MustCompile(`(\d+\w)+?`)
	for _, d := range p.FindAllString(data, -1) {
		unit := d[len(d) - 1]
		vs := d[0: len(d) - 1]
		i, er := strconv.ParseInt(vs, 10, 32)
		if er != nil {
			err = errors.Wrap(er, "parse duration")
			return
		}
		d, ok := durationUnits[unit]
		du += d * time.Duration(i)
		if !ok {
			err = errors.New(fmt.Sprintf("no such unit %s, data:%s", string(unit), data))
			return
		}
	}
	err = nil
	return
}

type Policy struct {
	MaxDuration     time.Duration
	// unit: `GB`
	MaxIndexSize    int
}


type ShardPolicy struct {
	
	// MaxDu maximum_duration units:w(week),d(day),h(hour)
	// if "", no max duration
	// Example: 1w2d3h
	MaxDu       string              `config:"maximum_duration" json:"maximum_duration"`
	
	// MaxIdx maximum_idx_size units(gb), if "", no max size
	MaxIdx      string              `config:"maximum_idx_size" json:"maximum_idx_size"`
}

func (s ShardPolicy) ParsePolicy() (Policy, error) {
	var po Policy
	du, err := parseDuration(s.MaxDu)
	if err != nil {
		return po, err
	}
	po.MaxDuration = du
	i, err := strconv.ParseInt(s.MaxIdx, 10, 32)
	if err != nil {
		return po, errors.Wrap(err, "parse max index size error")
	}
	po.MaxIndexSize = int(i)
	if po.MaxIndexSize < 1 {
		po.MaxIndexSize = math.MaxInt32
	}
	return po, nil
}

type DBOptions struct {
	ShardPolicy ShardPolicy         `config:"shard_policy" json:"shard_policy"`
}

// Validate the db options
func (p *DBOptions) Validate() error {
	_, err := p.ShardPolicy.ParsePolicy()
	if err != nil {
		return errors.Wrap(err, "validate options")
	}
	return nil
}

type HDFSConfig struct {
	Addr        string              `config:"addr"`
}

type ESConfig struct {
	Addrs []string                  `config:"addrs"`
}

// Config is the engine config
type Config struct {
	HDFS        HDFSConfig          `config:"hdfs"`
	ES          ESConfig            `config:"es"`
	GOptions    DBOptions           `config:"db_options"`
}

var (
	defaultConfig = Config{
		HDFS:HDFSConfig{Addr:"localhost:9000"},
		ES:ESConfig{Addrs:[]string {"localhost:9200"}},
		GOptions:DBOptions{ShardPolicy:ShardPolicy{MaxDu:"", MaxIdx:""}},
	}
)
