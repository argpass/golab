package ari

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/argpass/golab/harvester/libs"
	"github.com/argpass/golab/harvester/libs/constant"
	"github.com/argpass/golab/pending/utils"
	"github.com/olivere/elastic"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type ESDoc struct {
	Data  string
	Index string
}

type ESPort struct {
	*elastic.Client
	Addrs []string
	docsC chan []ESDoc
	docC  chan ESDoc
	wg    utils.IsGroup

	logger *zap.Logger
}

func NewESPort(addrs ...string) (*ESPort, error) {
	ch := make(chan []ESDoc)
	p := &ESPort{docsC: ch, docC: make(chan ESDoc)}
	c, err := elastic.NewClient(elastic.SetURL(addrs...))
	if err != nil {
		return nil, errors.Wrap(err, "new es client")
	}
	p.Client = c
	p.Addrs = append(p.Addrs, addrs...)
	return p, nil
}

func (p *ESPort) Start(ctx context.Context) {
	p.logger = ctx.Value(constant.KEY_LOGGER).(*zap.Logger).
		With(zap.String("mod", "esport"))
	pWG := ctx.Value(constant.KEY_P_WG).(utils.IsGroup)
	p.wg = utils.NewGroup(ctx)
	pWG.Go(func() error {
		return p.running()
	})
}

func (p *ESPort) running() error {
	defer p.logger.Info("bye")
	p.wg.Go(func() error {
		return p.savingDocs()
	})
	err := p.wg.Wait()
	if err != nil {
		return err
	}
	return nil
}

func (p *ESPort) savingDocs() error {
	maxBulkSize := 2000
	// max delay seconds
	maxDelay := 5
	var bulker *ESBulker
	var err error
	for {
		if bulker == nil {
			bulker = NewEsBulker(p.Client)
		}
		select {
		case p.wg.Context():
			err = p.wg.Context().Err()
			goto exit
		case doc := <-p.docC:
			bulker.Add(doc.Index, doc.Data)
		case time.After(time.Duration(maxDelay) * time.Second):
		}
	}

exit:
	p.logger.Debug(fmt.Sprintf("savingDocs bye with err:%v", err))
	return err
}

func (p *ESPort) ReadIndexInfo(index string) (IdxInfo, error) {
	// todo: load balances
	return readIdxInfo(index, p.Addrs[0])
}

func (p *ESPort) SaveDocs(ctx context.Context, docs []ESDoc) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case p.docsC <- docs:
		return nil
	}
	return nil
}

func appendValue(buf []byte, value libs.Value) []byte {
	if value.Type == libs.ValueTypes.STR {
		buf = append(buf, '"')
		buf = append(buf, value.SVal...)
		buf = append(buf, '"')
	} else if value.Type == libs.ValueTypes.INT {
		buf = strconv.AppendInt(buf, value.IVal, 10)
	} else if value.Type == libs.ValueTypes.FLOAT {
		buf = strconv.AppendFloat(buf, value.FVal, 'f', -1, 64)
	}
	return buf
}

// DumpEntry dumps the entry to ES Document json string
func DumpEntry(entry *libs.Entry) []byte {
	var buf = make([]byte, 0, 1024)
	buf = append(buf, '{')
	// write fields
	count := 0
	if entry.Fields == nil {
		entry.Fields = map[string]libs.Value{}
	}
	entry.Fields["@timestamp"] = libs.
		Value{IVal: int64(entry.Timestamp),
		Type: libs.ValueTypes.INT}
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
	buf = append(buf, ",\"@tag\":"...)
	buf = append(buf, '[')
	for i, tag := range entry.Tags {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = appendValue(buf, tag)
	}
	buf = append(buf, ']')
	// write body
	buf = append(buf, ",\"@body\":\""...)
	buf = append(buf, entry.Body...)
	buf = append(buf, "\""...)
	buf = append(buf, '}')
	return buf
}

type ESBulker struct {
	docType string
	routing string
	bulks   *elastic.BulkService
}

func NewEsBulker(es *elastic.Client) *ESBulker {
	return &ESBulker{docType: "log", bulks: es.Bulk()}
}

func (b *ESBulker) Add(index string, doc string) {
	req := elastic.NewBulkIndexRequest().Index(index).Type(b.docType).Doc(doc)
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

func (b *ESBulker) GetSize() int64 {
	return b.bulks.EstimatedSizeInBytes()
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

type IdxInfo struct {
	Name       string
	Hash       string
	RepoNum    int
	PriNum     int
	DocCount   int
	DelCount   int
	StoreSizeG float32
	PriSizeG   float32
}

// readIdxInfo reads index base info
func readIdxInfo(index string, urlBase string) (IdxInfo, error) {
	var info IdxInfo
	var err error
	url := fmt.Sprintf("%s/_cat/indices/%s", urlBase, index)
	resp, err := http.Get(url)
	if err != nil {
		return info, errors.Wrap(err, "cat index")
	}
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return info, errors.Wrap(err, "read body")
	}

	// result format example:
	//
	// health status index                                 uuid                   pri rep docs.count docs.deleted store.size pri.store.size
	// yellow open   ari.db_live_evt.%!s(int64=1490337739) 3xEg4oRmT1Wy7MxtSlxNLw   4   1          0            0       636b           636b
	sp := strings.Split(string(data), " ")
	info.Name, info.Hash = sp[2], sp[3]
	PriNum, _ := strconv.ParseInt(sp[4], 10, 32)
	RepoNum, _ := strconv.ParseInt(sp[5], 10, 32)
	docCount, _ := strconv.ParseInt(sp[6], 10, 32)
	delCount, _ := strconv.ParseInt(sp[7], 10, 32)
	storeSize, err := roundSizeGB(sp[8])
	if err != nil {
		return info, err
	}
	priSize, err := roundSizeGB(sp[9])
	if err != nil {
		return info, err
	}

	info.PriNum, info.RepoNum, info.DocCount, info.DelCount = int(PriNum),
		int(RepoNum), int(docCount), int(delCount)
	info.StoreSizeG, info.PriSizeG = storeSize, priSize

	return info, nil
}

// roundSizeGB parses size string to int
// all `mb` `kb` `b` units will be ignored
// 1.8gb => 1.8
func roundSizeGB(s string) (float32, error) {
	if strings.HasSuffix(s, "gb") {
		s = strings.TrimSuffix(s, "gb")
		size, err := strconv.ParseFloat(s, 32)
		if err != nil {
			return 0, errors.Wrap(err, "parse size")
		}
		return float32(size), nil
	}
	return 0, nil
}
