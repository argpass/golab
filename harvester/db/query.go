package db

import (
	"context"
	"github.com/dbjtech/golab/harvester/harvesterd"
)

type IsIterator interface {
	Next() bool
	Get() (*harvesterd.Entry, error)
}

// fixme:
type Query struct {
}

func (q *Query) HasTag(tag string) (*Query) {
	return q
}

func (q *Query) Field(field string, equal string) (*Query)  {
	return q
}

func (q *Query) Do(ctx context.Context) (IsIterator, error) {
	return nil, nil
}
