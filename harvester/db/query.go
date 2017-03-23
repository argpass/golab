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
//		case doc, ok := <-result.ResultChan():
//			if !ok {
//				// check error
//				if result.Err() != nil {
//					// handle err
//				}else{
//					// now no more docs
//					break
//				}
//			}
//			// handle the doc
//		}
//	}
//
type IsQueryResult interface {
	ResultChan() <- chan Doc
	Err() error
	Close()
}

type Query interface {
	WithTag(tag string) (Query)
	FieldEqual(field string, equal string) (Query)
	Do(ctx context.Context) (IsQueryResult, error)
}

