package wrappers

import (
	"github.com/alphazero/Go-Redis"
	"encoding/json"
	"time"
)

// MyRedis is a wrapper of `redis.Client`
type MyRedis struct {
	redis.Client
}

func (m *MyRedis) GetValue(key string, v interface{}) error {
	data, err := m.Get(key)
	if err != nil {
		return err
	}
	er := json.Unmarshal(data, &v)
	if er != nil {
		return er
	}
	return nil
}

func (m *MyRedis) SetValue(key string, v interface{}) error {
	return m.SetValueDetail(key, v, -1, -1, false)
}

func (m *MyRedis) SetValueDetail(key string, v interface{},
		expire int64, expireAt int64, onlyNotExist bool) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if onlyNotExist {
		ok, err := m.Setnx(key, data)
		if err != nil {
			return err
		}
		if !ok {
			// key exists, return
			return nil
		}
	}else {
		err = m.Set(key, data)
		if err != nil {
			return err
		}
	}

	// expire related
	if expire != -1 {
		_, err = m.Expire(key, expire)
		if err!= nil {
			return err
		}
	}
	if expireAt != -1 {
		now := time.Now().Unix()
		delta := expireAt - now
		if delta >= 0 {
			_, err = m.Expire(key, delta)
		}
	}
	return err
}

// NewRedisClient builds an instance of `MyRedis`
func NewRedisClient(host string, port int, password string, db int) (*MyRedis, error) {
	spec := redis.DefaultSpec().Db(db).Host(host).Port(port).Password(password)
	client, err := redis.NewSynchClientWithSpec(spec)
	if err != nil {
		return nil, err
	}
	myRedis := &MyRedis{client}
	return myRedis, nil
}
