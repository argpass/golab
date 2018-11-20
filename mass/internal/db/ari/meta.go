package ari

import (
	"fmt"
	"math"
	"time"
)

// ShardMeta holds meta information of a shard
type ShardMeta struct {
	Name     string `json:"name"`
	CreateAt int64  `json:"create_at"`

	// FdLocked map[fd]node_id
	FdLocked map[int]string `json:"fd_locked"`
	MaxFd    int            `json:"max_fd"`
	IdxSize  int64          `json:"idx_size"`
	DocCount int64          `json:"doc_count"`
}

// DbMeta records db related meta information
type DbMeta struct {
	// Shards, {shard_name=>ShardMeta}
	Shards map[string]*ShardMeta `json:"shards"`

	// HotShard is the newest created shard
	HotShard string `json:"hot_shard"`

	// Db name
	Name     string `json:"name"`
	CreateAt int64  `json:"create_at"`
	// Options shouldn't be changed after db created
	Options DBOptions `json:"options"`
}

func NewShardMeta() *ShardMeta {
	createAt := time.Now().Unix()
	name := fmt.Sprintf("%d", createAt)
	s := &ShardMeta{
		Name: name, CreateAt: createAt,
		FdLocked: map[int]string{}, MaxFd: math.MaxUint16,
	}
	return s
}

func GetInitialDbMeta(db string, ops DBOptions) (*DbMeta, error) {
	err := ops.Validate()
	if err != nil {
		return nil, err
	}
	defaultShard := NewShardMeta()
	m := &DbMeta{
		Shards:   map[string]*ShardMeta{defaultShard.Name: defaultShard},
		HotShard: defaultShard.Name,
		Name:     db,
		CreateAt: defaultShard.CreateAt,
		Options:  ops,
	}
	return m, nil
}

// Meta holds engine meta information
// etcd key: {cluster}.ari
type Meta struct {
	Dbs map[string]*DbMeta `json:"dbs"`
}
