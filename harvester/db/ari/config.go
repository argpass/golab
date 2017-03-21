package ari

type ShardPolicy struct {
	// MaxDu maximum_duration units:m(month),w(week),d(day),h(hour)
	// if "", no max duration
	// Example: 1w2d3h
	MaxDu       string              `config:"maximum_duration"`
	// MaxIdx maximum_idx_size units(gb), if "", no max size
	MaxIdx      string              `config:"maximum_idx_size"`
}

type DBOptions struct {
	ShardPolicy ShardPolicy         `config:"shard_policy"`
}

type HDFSConfig struct {
	Addr        string              `config:"addr"`
}

type ESConfig struct {
	Addrs []string                  `config:"addr"`
}

// Config is the engine config
type Config struct {
	HDFS        HDFSConfig          `config:"hdfs"`
	ES          ESConfig            `config:"hdfs"`
	GOptions    DBOptions           `config:"db_options"`
}

var (
	defaultConfig = Config{
		HDFS:HDFSConfig{Addr:"localhost:9000"},
		ES:ESConfig{Addrs:[]string{"localhost:9200"}},
		GOptions:DBOptions{ShardPolicy:ShardPolicy{MaxDu:"", MaxIdx:""}},
	}
)
