package ari

import (
	"github.com/dbjtech/golab/harvester/harvesterd"
	"strconv"
	"github.com/olivere/elastic"
	"context"
	"os"
	"fmt"
	"io/ioutil"
	"github.com/pkg/errors"
	"bytes"
	"text/template"
)

func appendValue(buf []byte, value harvesterd.Value) []byte {
	if value.Type == harvesterd.ValueTypes.STR {
		buf = append(buf, '"')
		buf = append(buf, value.SVal...)
		buf = append(buf, '"')
	}else if value.Type == harvesterd.ValueTypes.INT {
		buf = strconv.AppendInt(buf, value.IVal, 10)
	}else if value.Type == harvesterd.ValueTypes.FLOAT {
		buf = strconv.AppendFloat(buf, value.FVal, 'f', -1, 64)
	}
	return buf
}

// DumpEntry dumps the entry to ES Document json string
func DumpEntry(entry *harvesterd.Entry) []byte {
	var buf = make([]byte, 0, 1024)
	buf = append(buf, '{')
	// write fields
	count := 0
	entry.Fields["_timestamp"] = harvesterd.
		Value{IVal:int64(entry.Timestamp),
			Type:harvesterd.ValueTypes.INT}
	for key, value := range entry.Fields {
		if count > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, '"')
		buf = append(buf, key...)
		buf = append(buf, '"')
		buf = append(buf, ':')
		buf = appendValue(buf, value)
		count++
	}
	// write tags
	buf = append(buf, ",\"_tag\":"...)
	buf = append(buf, '[')
	for i, tag := range entry.Tags {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = appendValue(buf, tag)
	}
	buf = append(buf, ']')
	// write body
	buf = append(buf, ",\"_body\":\""...)
	// todo:binary bytes may be rejected by es, use base64 instead
	buf = append(buf, entry.Body...)
	buf = append(buf, "\""...)
	buf = append(buf, '}')
	return buf
}

type ESBulker struct {
	index 		string
	docType 	string
	routing 	string
	bulks   	*elastic.BulkService
}

func NewEsBulker(es *elastic.Client, index string) *ESBulker {
	return &ESBulker{index:index,docType:"log", bulks:es.Bulk()}
}

func (b *ESBulker) Add(doc []byte) {
	req := elastic.NewBulkIndexRequest().Index(b.index).Type(b.docType).Doc(string(doc))
	if b.routing != "" {
		req = req.Routing(b.routing)
	}
	b.bulks.Add(req)
}

func (b *ESBulker) Do(ctx context.Context) error {
	_, err := b.bulks.Do(ctx)
	if err != nil {
		return err
	}
	return nil
}

// EnsureIndex ensure index of `db` exist
// if index doesn't exist, create it and alias `{db}_r` to the index
func EnsureIndex(ctx context.Context, db string, index string, c *elastic.Client) error {
	var bf bytes.Buffer
	// todo: pass tlp by flag or config
	tlp := "es.template.json"
	t, err := template.ParseFiles(tlp)
	if err != nil {
		return errors.Wrap(err, "parse es template")
	}
	err = t.Execute(&bf, map[string]string{"Db": db})
	if err != nil {
		return errors.Wrap(err, "execute template")
	}
	_, err = c.CreateIndex(index).Body(string(bf.Bytes())).Do(ctx)
	if err != nil {
		if nerr, ok := err.(*elastic.Error); ok {
				if nerr.Details.Type == "index_already_exists_exception" {
					return nil
				}
		}
		return errors.Wrap(err, "create index")
	}
	return nil
}