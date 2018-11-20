package client

import "golang.org/x/net/context"

type massDoc struct {
	fields map[string]interface{}
	tags   []string
	body   string
}

// Batch
type Batch struct {
	docs []massDoc
}

// NewBatch create an empty `Batch`
func NewBatch() *Batch {
	return &Batch{}
}

// Add put document to the batch
func (b *Batch) Add(fields map[string]interface{}, tags []string, body string) {
	b.docs = append(b.docs, massDoc{fields: fields, tags: tags, body: body})
}

// Size size of the batch, size of empty batch is zero
func (b *Batch) Size() int {
	return len(b.docs)
}

type DBOption struct {
}

type IsDBConnection interface {
	SaveDoc(ctx context.Context, fields map[string]interface{}, tags []string, body string) error
	DoBatch(ctx context.Context, batch *Batch) error
}
type IsDBClient interface {
	CreateIfNotExists(db string, option DBOption) (notExist bool, err error)
	Open(db string) (IsDBConnection, error)
}
