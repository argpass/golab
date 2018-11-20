package ari

import (
	"github.com/argpass/golab/harvester/harvesterd"
	"context"
	"sync/atomic"
	"github.com/argpass/golab/pending/utils"
	"time"
	"github.com/argpass/golab/harvester/libs/constant"
	"go.uber.org/zap"
	"github.com/argpass/golab/harvester/db"
	"fmt"
	"github.com/argpass/golab/harvester/libs"
	"math"
	"github.com/olivere/elastic"
	"sync"
	"encoding/json"
	"github.com/pkg/errors"
	"strings"
	"runtime"
)

const (
	DEFAULT_SCROLL_SIZE = 100
)

type queryResult struct {
	closeOnce sync.Once
	doneC     chan struct{}
	logger    *zap.Logger
	eg        utils.IsGroup
	sl        *elastic.ScrollService
	ctx       context.Context
	writeC    chan <- db.Doc
	readC      <- chan db.Doc
	err       error
	waiter    *DbWaiter
}

func newQueryResult(
	ctx context.Context,
	sl *elastic.ScrollService,
	waiter *DbWaiter) *queryResult {
	
	ch := make(chan db.Doc)
	q := &queryResult{
		sl:     sl,
		writeC: (chan<-db.Doc)(ch),
		readC: (<-chan db.Doc)(ch),
		waiter: waiter,
		doneC:  make(chan struct{}),
	}
	pWG := ctx.Value(constant.KEY_P_WG).(utils.IsGroup)
	q.logger = ctx.Value(constant.KEY_LOGGER).(*zap.Logger).With(zap.String("mod", "query-result"))
	q.eg = utils.NewGroup(ctx)
	q.ctx = q.eg.Context()
	
	pWG.Go(func() error {
		err := q.running()
		if err != nil {
			q.logger.Error(fmt.Sprintf("query result err:%+v", err))
		}
		// i will never to cancel parent
		return nil
	})
	
	return q
}

func (q *queryResult) running() error {
	q.eg.Go(func() error {
		return q.scrolling()
	})
	
	q.logger.Debug("running")
	err := q.eg.Wait()
	// to close myself when scrolling done
	q.Close()
	if err != nil {
		q.err = err
	}
	q.logger.Debug("bye")
	return err
}

func (q *queryResult) resolvingOne(hit *elastic.SearchHit) error  {
	// get `_body`
	s := []byte(*hit.Source)
	var sourceMap map[string]interface{}
	err := json.Unmarshal(s, &sourceMap)
	if err != nil {
		err = errors.Wrap(err, "unmarshal sourceMap")
		return err
	}
	body, ok := sourceMap["@body"]
	if !ok {
		err = errors.New("invalid doc witout `@body` field")
		return err
	}
	
	// body is a row key, unpack it
	rk, err := UnpackRowKeyByBase64(body.(string))
	if err != nil {
		return errors.Wrap(err, "unpack row key by base64")
	}
	
	// fetch hdfs
	dbName, shard, err := unpackIdxName(hit.Index)
	if err != nil {
		return err
	}
	dirpath := q.waiter.a.Hdfs.
		GetDirPath(q.waiter.a.Node.ClusterName.String(), dbName, shard)
	var rowMap = map[RowKey][]byte{}
	rowMap[rk]= nil
	q.logger.Debug(fmt.Sprintf(
		"resolverows dirpath:%s, rowMap:%+v, esId:%s",
		dirpath, rowMap, hit.Id))
	err = q.waiter.a.Hdfs.ResolveRows(dirpath, rowMap)
	if err != nil {
		return err
	}
	raw := rowMap[rk]
	
	// wrap as `Doc`
	doc := db.Doc{Id:hit.Id, Body:string(raw)}
	
	// send to the docs chan
	select {
	case q.writeC <- doc:
	case <- q.ctx.Done():
		return q.ctx.Err()
	}
	return nil
}

// resolving resolves docs for `hits` and send them to the docsChan
// caller will get docs by reading the docs chan
func (q *queryResult) resolving(hits []*elastic.SearchHit) error {
	count := 0
	batchWG := utils.NewGroup(q.ctx)
	for _, hit := range hits {
		
		// start go routine to resolve the hit
		func(hit *elastic.SearchHit){
			batchWG.Go(func() error {
				return q.resolvingOne(hit)
			})
		}(hit)
		count++
		if count < 10 {
			continue
		}
		
		// batch full, wait all task to be done
		err := batchWG.Wait()
		if err != nil {
			return err
		}
		count = 0
	}
	
	// wait remains to be done
	err := batchWG.Wait()
	if err != nil {
		return err
	}
	
	return nil
}

func (q *queryResult) scrolling() error {
	for {
		select {
		case <-q.ctx.Done():
			goto exit
		default:
		}
		
		resp, err := q.sl.Do(q.ctx)
		if err != nil {
			// no more documents
			if strings.EqualFold(err.Error(), "EOF") {
				goto exit
			}else{
				err = errors.Wrap(err, "scroll err")
				return err
			}
		}
		
		// resolve hits to docs and send them.
		// to resolve the hits instead of only one
		err = q.resolving(resp.Hits.Hits)
		if err != nil {
			return err
		}
	}
exit:
	return nil
}

func (q *queryResult) ResultChan() <-chan db.Doc {
	return q.readC
}

func (q *queryResult) Done() <-chan struct {} {
	return q.doneC
}

func (q *queryResult) Close() {
	q.closeOnce.Do(func(){
		// cancel sub go routines
		close(q.doneC)
		q.writeC = nil
		q.eg.Cancel(nil)
	})
}

func (q *queryResult) Err() error {
	return q.err
}

type simpleQ struct {
	con *NativeConnection
	tags []interface{}
	fields map[string]string
}

func (q *simpleQ) WithTag(tag string) (db.Query) {
	q.tags = append(q.tags, tag)
	return q
}

func (q *simpleQ) FieldEqual(field string, equal string) (db.Query) {
	if q.fields == nil {
		q.fields = map[string]string{}
	}
	q.fields[field] = equal
	return q
}

func (q *simpleQ) Do(ctx context.Context) (db.IsQueryResult, error) {
	logger := ctx.Value(constant.KEY_LOGGER).(*zap.Logger).With(zap.String("mod", "Q"))
	index := getESRDIdx(q.con.waiter.db)
	logger.Debug(fmt.Sprintf("query idx %s, tags:%v, fields:%v", index, q.tags, q.fields))
	var qr []elastic.Query
	if q.tags != nil {
		tagQuery := elastic.NewTermsQuery("@tag", q.tags...)
		qr = append(qr, tagQuery)
	}
	if q.fields != nil {
		for fi, v := range q.fields {
			qr = append(qr, elastic.NewTermQuery(fi, v))
		}
	}
	sc := elastic.NewFetchSourceContext(true).Include("@body", "@tag")
	sl := q.con.es.Scroll(index).
		Query(elastic.NewBoolQuery().
		Must(qr...)).
		FetchSourceContext(sc).
		Size(DEFAULT_SCROLL_SIZE)
	return newQueryResult(ctx, sl, q.con.waiter), nil
}

// NativeConnection is a native implement of `harvesterd.DBConnection` to `ARI`
// instead of send data by net, the connection send data by a chan
type NativeConnection struct {
	es *elastic.Client
	waiter *DbWaiter
	sendC chan <- *libs.Entry
	ctx context.Context
	id uint64
	closed uint32
}

func (c *NativeConnection) Query() (db.Query) {
	return &simpleQ{con:c}
}

func newNativeConnection(
		sendC chan <- *libs.Entry,
		ctx context.Context, id uint64,
		client *elastic.Client,
		waiter *DbWaiter) *NativeConnection {
	
	c := &NativeConnection{
		es:client,
		sendC:sendC,
		id:id,
		ctx:ctx,
		waiter:waiter,
	}
	go func(){
		select {
		case <-ctx.Done():
			// close the connection
			atomic.StoreUint32(&c.closed, uint32(1))
			c.sendC = nil
		}
	}()
	return c
}

// IsClosed checks if the connection is closed
func (c *NativeConnection) IsClosed() bool {
	return atomic.LoadUint32(&c.closed) == uint32(1)
}

// Save save the entry to the `ARI`
func (c *NativeConnection) Save(entry *libs.Entry) error {
	if c.IsClosed() {
		return harvesterd.Error{Code:libs.E_DB_CLOSED}
	}

	var ch chan <-*libs.Entry = c.sendC
	//attemptMaximum := 3
	//attempt := 0
	for {
		select {
		case ch <- entry:
			goto exit
		case <-c.ctx.Done():
			return harvesterd.Error{Code: libs.E_DB_CLOSED}
		//default:
		//	attempt++
		//	if attempt >= attemptMaximum {
		//		return harvesterd.Error{Code: libs.E_MAXIMUM_ATTEMPT}
		//	}
		//	runtime.Gosched()
		//	continue
		}
	}
exit:
	return nil
}

// DbWaiter manages db connections,
// batch write and query db
type DbWaiter struct {
	lock        sync.RWMutex
	wg          utils.IsGroup
	a           *Ari
	db          string
	dbMeta      DbMeta
	entriesC    chan *libs.Entry
	cons        []*NativeConnection
	
	ctx         context.Context
	logger      *zap.Logger
	
	writer      *writer
}

func newWaiter(db string, a *Ari, meta DbMeta) *DbWaiter {
	w := &DbWaiter{
		db:db,
		a:a,
		dbMeta:meta,
		entriesC:make(chan *libs.Entry),
	}
	return w
}

func (w *DbWaiter) CheckDbMeta(meta DbMeta) {
	w.logger.Debug(fmt.Sprintf("db meta change to:%+v, current:%+v", meta, w.dbMeta))
	w.lock.Lock()
	w.dbMeta = meta
	w.lock.Unlock()
}

func (w *DbWaiter) running() error {
	// start servicing
	recvC := (<-chan *libs.Entry) (w.entriesC)
	
	// on exiting
	w.wg.Go(func() error {
		select {
		case <-w.ctx.Done():
		}
		w.lock.Lock()
		if w.writer != nil {
			err := w.writer.Close()
			w.logger.Debug(fmt.Sprintf("close hdfs writer, err:%+v", err))
		}
		w.lock.Unlock()
		return nil
	})
	
	w.wg.Go(func() error {
		return w.pumpingBatch(recvC)
	})
	// wait my sub goroutines to exit
	err := w.wg.Wait()
	w.logger.Info("bye")
	
	// raise err
	if err != nil {
		return err
	}
	return nil
}

func (w *DbWaiter) Start(ctx context.Context) error {
	pWg := ctx.Value(constant.KEY_P_WG).(utils.IsGroup)
	
	// init
	w.wg = utils.NewGroup(ctx)
	w.ctx = w.wg.Context()
	w.logger = ctx.Value(constant.KEY_LOGGER).(*zap.Logger).
		With(zap.String("name", fmt.Sprintf("waiter-%s", w.db)))
	w.logger.Debug("start")
	
	// add to parent wait group
	pWg.Go(func() error {
		return w.running()
	})
	
	return nil
}

// NewConnection allocate a db connection
// todo: to reuse idle connections instead of creating a new one
func (w *DbWaiter) NewConnection() (*NativeConnection) {
	w.logger.Debug("new connection")
	conId := uint64(time.Now().UnixNano())
	
	con := newNativeConnection(
		(chan <-*libs.Entry)(w.entriesC),
		w.ctx,
		conId,
		w.a.ES.Client,
		w,
	)
	
	w.cons = append(w.cons, con)
	return con
}

func (w *DbWaiter) handlingBatch(ctx context.Context,
	batch []*libs.Entry, stat map[string] interface{}) error  {
	
	if len(batch) == 0 {
		return nil
	}
	
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		writer, err := w.GetWriter()
		if err != nil {
			return errors.Wrap(err, "get writer")
		}
		t := time.Now()
		err = writer.Write(ctx, batch, stat)
		stat["writer_write"] = time.Since(t)
		if err != nil {
			if err == writerClosedErr {
				runtime.Gosched()
				w.logger.Debug("batch writer closed, continue to get next writer to write the batch")
				continue
			}
			return errors.Wrap(err, "on batch writer writing")
		}
		// write successfully
		return nil
	}
	return nil
}

// pumpingBatch receives data and makes batch to send to writer
func (w *DbWaiter) pumpingBatch(ch <-chan *libs.Entry) error {
	var entry *libs.Entry
	var needWrap bool
	// fixme: should with timeout to handle a batch ?
	// todo: configure the args
	ctx := w.ctx
	workerNum := 30
	batchMaximum := 3000
	// max size: 1mb
	batchSizeMaximum := 1 * 1024 * 1024
	batchTimeDelayMaximum := 10 * time.Second
	buf := make([]*libs.Entry, 0, batchMaximum)
	size := 0
	
	defer func() {
		w.logger.Info("pumpingBatch exit")
	}()
	
	batchC := make(chan []*libs.Entry)
	// start new goroutines to handle the batchs
	for i:=0; i < workerNum; i++ {
		w.wg.Go(func() error {
			for {
				select {
				case <-w.ctx.Done():
					return w.ctx.Err()
				case batch := <-batchC:
					t_start := time.Now().Unix()
					var stat = map[string] interface{}{}
					err := w.handlingBatch(ctx, batch, stat)
					t_end := time.Now().Unix()
					cost := t_end - t_start
					if t_end - t_start > 2 {
						w.logger.Debug(fmt.Sprintf("handle batch cost %d, stat:%v", cost, stat))
					}
					if err != nil {
						return err
					}
				}
			}
			return nil
		})
	}

	for {
		if needWrap && len(buf) > 0 {
			// make batch
			batch := make([]*libs.Entry, len(buf))
			copy(batch, buf[0:])

			send:
			select {
			case <-w.ctx.Done():
				return w.ctx.Err()
			case batchC <-batch:
				// reset buf
				buf = buf[:0]
				needWrap = false
				size = 0
			case <-time.After(2 * time.Second):
				// todo: config the timeout arg
				w.logger.Debug(
					"there's a batch waitting to be handled, timeout 2 seconds")
				goto send
			}
		}

		select {
		case entry = <- ch:
			buf = append(buf, entry)
			size += len(entry.Body)
			if len(buf) >= batchMaximum {
				needWrap = true
				continue
			}
			if size >= batchSizeMaximum {
				needWrap = true
				continue
			}
		case <-time.After(batchTimeDelayMaximum):
		
			if len(buf) > 0 {
				needWrap = true
				continue
			}
		case <-w.ctx.Done():
			return w.ctx.Err()
		}
	}
	return nil
}

// createBatchWriter creates a new writer
func (w *DbWaiter) createBatchWriter() (*writer, error) {
	
	// allocate fd of current hot shard
	fd, err := w.a.AllocateFd(w.db, w.dbMeta.HotShard)
	if err != nil {
		return nil, err
	}
	
	// create hdfs appender
	appender, err := NewHdfsAppender(
		w.a.Node.ClusterName.String(),
		w.db, w.dbMeta.HotShard, fd, w.a.Hdfs, func(fd uint16) error {
			return w.a.FreeFd(w.db, w.dbMeta.HotShard, fd)
		})
	if err != nil {
		return nil, err
	}
	
	return newBatchWriter(w.db, w.dbMeta.HotShard,appender, w.a.ES.Client), nil
}

// GetWriter returns a writer to write the entries
// there is only one writer per db waiter this version
//
// 1. a waiter has only one writer;
// 2. if current hot shard of db changed,
//    waiter should create a new writer (holds new shard name and hdfs fd)
func (w *DbWaiter) GetWriter() (*writer, error) {
	// get or create a new writer
	w.lock.Lock()
	defer w.lock.Unlock()
	
	if w.writer == nil {
		// create writer
		wt, err := w.createBatchWriter()
		if err != nil {
			return nil, err
		}
		w.logger.Debug("writer has been created")
		w.writer = wt
		
	} else {
		// if hot shard changed, replace the writer with a new one
		if w.writer.shard != w.dbMeta.HotShard {
			// create a new one
			wt, err := w.createBatchWriter()
			if err != nil {
				return nil, err
			}
			
			// close the old one
			old := w.writer
			w.writer = wt
			w.logger.Debug("writer has been reseted to newest hot shard")
			err = old.Close()
			if err != nil {
				return nil, err
			}
		}
	}
	return w.writer, nil
}

// writer writes a batch of entries to the HDFS and Elasticsearch
type writer struct {
	lock    sync.RWMutex
	db      string
	closed  bool
	
	// shard is current hot shard of `db`
	shard   string
	
	hdfsAppender *HdfsAppender
	es           *elastic.Client
}

func newBatchWriter(
	db string, shard string,
	appender *HdfsAppender,
	es *elastic.Client) *writer {
	
	return &writer{
		db:           db,
		shard:        shard,
		hdfsAppender: appender,
		es:           es,
	}
}

// Close the writer
// close hdfs appender
func (b *writer) Close() error {
	b.lock.Lock()
	defer b.lock.Unlock()
	if b.closed  {
		return nil
	}
	b.closed = true
	err := b.hdfsAppender.Close()
	if err != nil {
		return err
	}
	return nil
}

func (b *writer) getEsBulker() *ESBulker  {
	index := getEsRawIdx(b.db, b.shard)
	return NewEsBulker(b.es, index)
}

var writerClosedErr = errors.New("batch writer closed")

// Write batch to db
func (b *writer) Write(ctx context.Context, batch []*libs.Entry, stat map[string] interface{}) error {
	//logger := ctx.Value(constant.KEY_LOGGER).(*zap.Logger)
	if len(batch) == 0 {
		return nil
	}
	
	if b.closed {
		return writerClosedErr
	}
	
	b.lock.Lock()
	off, err := b.hdfsAppender.CurrentOff()
	if err != nil {
		b.lock.Unlock()
		return err
	}
	fd := b.hdfsAppender.GetFD()
	ckBuilder := NewChunkBuilder()
	if len(batch) >= math.MaxUint16 {
		b.lock.Unlock()
		return errors.New("the batch size is too large")
	}
	esDocs := make([][]byte, len(batch))
	for i, et := range batch {
		ckBuilder.AddRow([]byte(et.Body))
		rk := MakeRowKey(fd, off, uint16(i))
		et.Body = rk.EncodeBase64()
		esDocs[i] = DumpEntry(et)
	}
	
	t := time.Now()
	chunk := ckBuilder.Build()
	stat["chunk_build"] = time.Since(t)
	data := chunk.Bytes()
	t = time.Now()
	_, err = b.hdfsAppender.Append(data)
	b.lock.Unlock()
	
	stat["hdfs_append"] = time.Since(t)
	if err != nil {
		return errors.Wrap(err, "append chunk")
	}
	
	err = b.saveToEs(esDocs)
	if err != nil {
		return err
	}
	// todo: give entries back to the Entry Pool
	return err
}

func (b *writer) saveToEs(esDocs [][]byte) error {
	// todo: save to es
	return nil
}

