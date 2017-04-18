package db

import (
	"context"
)

type Doc struct {
	Id      string  `json:"id"`
	Body    string  `json:"body"`
}

// IsQueryResult uses a read only chan to send docs to caller.
// You should check error when the chan closed.
// Remember to close the result if you don't read it any more.
//
//	defer result.Close()
//	for {
//		select {
//      case <-ctx.Done():
//          // cancelled
//          goto exit
//      case <-result.Done():
//          // no more
//          goto exit
//		case doc := <-result.ResultChan():
//			// handle the doc
//		}
//	}
// exit:
//
type IsQueryResult interface {
	ResultChan() <- chan Doc
	Done() <- chan struct{}
	Err() error
	Close()
}

// Query interface
type Query interface {
	
	// WithTag fetch docs with some tag
	WithTag(tag string) Query
	
	// FieldEqual fetch docs that `field`=`equal`
	FieldEqual(field string, equal string) Query
	
	// Do query process
	Do(ctx context.Context) (IsQueryResult, error)
}

