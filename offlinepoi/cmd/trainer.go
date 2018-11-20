package main

import (
	"context"
	"fmt"
	"runtime"
	"sync"

	"github.com/argpass/golab/offlinepoi/internal"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type trainer struct {
	db     internal.IsKeyValueDB
	policy internal.IsPolicy
	batchC chan struct {
		hash      string
		locations []internal.Location
	}
}

func newTrainner() (*trainer, error) {
	t := &trainer{
		batchC: make(chan struct {
			hash      string
			locations []internal.Location
		}),
	}
	return t, nil
}

func (c *trainer) startRunning(ctx context.Context) error {
	var err error
	wg := &sync.WaitGroup{}
	errOnce := &sync.Once{}
	for i := 0; i < runtime.NumCPU()*2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			trainingErr := c.training(ctx, idx, c.batchC)
			if trainingErr != nil {
				errOnce.Do(func() {
					err = trainingErr
				})
			}
		}(i + 1)
	}
	wg.Wait()
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (c *trainer) feed(ctx context.Context, locations []internal.Location) error {
	geoMap := make(internal.GeoMap, minInt(7000, len(locations)))
	// split locations by geo hash to geoMap
	for _, loc := range locations {
		locs, ok := geoMap[loc.GeoHash]
		if !ok {
			locs = make([]internal.Location, 0, minInt(40, len(locations)))
		}
		locs = append(locs, loc)
		geoMap[loc.GeoHash] = locs
	}
	for hash, locs := range geoMap {
		select {
		case c.batchC <- struct {
			hash      string
			locations []internal.Location
		}{hash: hash, locations: locs}:
			// ok
		case <-ctx.Done():
			return nil
		}
	}
	return nil
}

func (c *trainer) training(ctx context.Context, idx int, batchC <-chan struct {
	hash      string
	locations []internal.Location
}) error {
	logger := ctx.Value("logger").(*zap.Logger).With(
		zap.String("G", fmt.Sprintf("C%d", idx)))
	logger.Debug("running")
	defer logger.Info("bye")
	for {
		select {
		case batch := <-batchC:
			if len(batch.locations) == 0 {
				continue
			}
			key := batch.hash

			// 加载该bucket中的点合并传入的点进行计算并存储结果
			err := func() error {
				data, err := c.db.Get(ctx, key)
				if err != nil {
					return errors.WithStack(err)
				}
				policy, err := c.policy.Decode(data)
				if err != nil {
					return errors.WithStack(err)
				}
				err = policy.Merge(batch.locations)
				if err != nil {
					return errors.WithStack(err)
				}
				data, err = policy.Encode()
				if err != nil {
					return errors.WithStack(err)
				}
				err = c.db.Put(ctx, key, data)
				if err != nil {
					return errors.WithStack(err)
				}
				return nil
			}()
			// 处理收集到的error
			if err != nil {
				logger.Error("err on calculating",
					zap.String("hash", batch.hash),
					zap.String("err", fmt.Sprintf("%+v", err)))
			}
		case <-ctx.Done():
			return nil
		}
	}
	return nil
}
