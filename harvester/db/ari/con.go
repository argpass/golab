package ari

import (
	"github.com/dbjtech/golab/harvester/harvesterd"
	"context"
	"runtime"
	"sync/atomic"
	"github.com/dbjtech/golab/pending/utils"
	"time"
	"github.com/dbjtech/golab/harvester/libs/constant"
	"go.uber.org/zap"
	"github.com/dbjtech/golab/harvester/db"
	"fmt"
	"github.com/dbjtech/golab/harvester/libs"
	"encoding/base64"
	"math"
	"github.com/pkg/errors"
	"syscall"
	"github.com/olivere/elastic"
	"sync"
)

// NativeConnection is a native implement of `harvesterd.DBConnection` to `ARI`
// instead of send data by net, the connection send data by a chan
type NativeConnection struct {
	sendC chan <- *harvesterd.Entry
	ctx context.Context
	id uint64
	closed uint32
}

func (*NativeConnection) Query() (*db.Query) {
	// fixme:
	panic("implement me")
}

func newNativeConnection(
		sendC chan <- *harvesterd.Entry,
		ctx context.Context, id uint64) *NativeConnection {
	
	c := &NativeConnection{
		sendC:sendC,
		id:id,
		ctx:ctx,
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
	return atomic.LoadUint32(&c.closed) == uint32(0)
}

// Save save the entry to the `ARI`
func (c *NativeConnection) Save(entry *harvesterd.Entry) error {
	if c.IsClosed() {
		return harvesterd.Error{Code:libs.E_DB_CLOSED}
	}

	var ch chan <-*harvesterd.Entry = c.sendC
	attemptMaximum := 3
	attempt := 0
	for {
		select {
		case ch <- entry:
			break
		case <-c.ctx.Done():
			return harvesterd.Error{Code:libs.E_DB_CLOSED}
		default:
			attempt++
			if attempt >= attemptMaximum {
				return harvesterd.Error{Code:libs.E_MAXIMUM_ATTEMPT}
			}
			runtime.Gosched()
			continue
		}
	}
	return nil
}

// DbWaiter manages db connections,
// batch write and query db
type DbWaiter struct {
	lock        sync.RWMutex
	wg          utils.WrappedWaitGroup
	a           *Ari
	db          string
	dbMeta      DbMeta
	entriesC    chan *harvesterd.Entry
	cons        []*NativeConnection
	
	ctx         context.Context
	cancel      context.CancelFunc
	logger      *zap.Logger
	
	writer      *writer
}

func newWaiter(db string, a *Ari, meta DbMeta) *DbWaiter {
	w := &DbWaiter{
		db:db,
		a:a,
		entriesC:make(chan *harvesterd.Entry),
	}
	w.CheckDbMeta(meta)
	return w
}

func (w *DbWaiter) CheckDbMeta(meta DbMeta) {
	//old := w.dbMeta.HotShard
	w.lock.Lock()
	w.dbMeta = meta
	w.lock.Unlock()
	// hot shard changed, notify
	//if w.dbMeta.HotShard != old {
	//
	//}
}

func (w *DbWaiter) running() {
	// start servicing
	recvC := (<-chan *harvesterd.Entry) (w.entriesC)
	w.wg.Wrap(func(){
		w.pumpingBatch(recvC)
	})
	// wait my sub goroutines to exit
	w.wg.Wait()
	w.a.logger.Warn("bye")
}

func (w *DbWaiter) Start(ctx context.Context) error {
	pWg := ctx.Value(constant.KEY_P_WG).(*utils.WrappedWaitGroup)
	
	// init
	w.ctx, w.cancel = context.WithCancel(ctx)
	w.ctx = context.WithValue(w.ctx, constant.KEY_P_WG, &w.wg)
	w.logger = ctx.Value(constant.KEY_LOGGER).(*zap.Logger).
		With(zap.String("name", fmt.Sprintf("waiter-%s", w.db)))
	
	// add to parent wait group
	pWg.Wrap(func(){
		w.running()
	})
	
	return nil
}

// NewConnection allocate a db connection
// todo: to reuse idle connections instead of creating a new one
func (w *DbWaiter) NewConnection() (*NativeConnection) {
	conId := uint64(time.Now().UnixNano())
	con := newNativeConnection((chan <-*harvesterd.Entry)(w.entriesC), w.ctx, conId)
	w.cons = append(w.cons, con)
	return nil
}

func (w *DbWaiter) handleBatch(ctx context.Context,
	batch []*harvesterd.Entry)  {
	
	if len(batch) == 0 {
		return
	}
	
	// fixme: handle err
	writer, err := w.GetWriter()
	if err != nil {
		w.logger.Error(fmt.Sprintf("got batch writer err:%v", err))
	}
	err = writer.Write(ctx, batch)
	if err != nil {
		w.logger.Error(fmt.Sprintf("batch writer write err:%v", err))
	}
}

// pumpingBatch receives data and makes batch to send to writer
func (w *DbWaiter) pumpingBatch(ch <-chan *harvesterd.Entry) {
	var entry *harvesterd.Entry
	var needWrap bool
	// todo: should with timeout ?
	// todo: limit maximum handle batch goroutines ?
	ctx := w.ctx
	batchMaximum := 1000
	batchSizeMaximum := 1 * 1024 * 1024
	batchTimeDelayMaximum := 30 * time.Second
	ticker := time.NewTicker(batchTimeDelayMaximum)
	buf := make([]*harvesterd.Entry, batchMaximum)
	cur := 0
	size := 0

	for {
		if needWrap && cur > 0 {
			// make batch
			batch := make([]*harvesterd.Entry, cur)
			copy(batch, buf[:cur])

			// start new goroutine to handle the batch
			w.wg.Wrap(func(){
				w.handleBatch(ctx, batch)
			})
			// reset buf
			cur = 0
			buf = buf[:0]
			needWrap = false
		}

		select {
		case entry = <- ch:
			buf[cur] = entry
			cur++
			size += len(entry.Body)
			if cur >= batchMaximum {
				needWrap = true
				continue
			}
			if size >= batchSizeMaximum {
				needWrap = true
				continue
			}
		case <-ticker.C:
			if cur > 0 {
				needWrap = true
				continue
			}
		case <-w.ctx.Done():
			break
		}
	}
	ticker.Stop()
	w.a.logger.Warn("pumpingBatch exit")
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
	
	return newBatchWriter(w.db, w.dbMeta.HotShard,appender, w.a.ES), nil
}

// GetWriter returns a writer to write the entries
// there is only one writer per db waiter this version
//
// 1. a waiter has only one writer;
// 2. if current hot shard of db changed,
//    waiter should create a new writer (holds new shard name and hdfs fd)
func (w *DbWaiter) GetWriter() (*writer, error) {
	// get or create a new writer
	
	if w.writer == nil {
		// create writer
		wt, err := w.createBatchWriter()
		if err != nil {
			return nil, err
		}
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
			err = w.writer.Close()
			if err != nil {
				return nil, err
			}
			w.writer = wt
		}
	}
	return w.writer, nil
}

// writer writes a batch of entries to the HDFS and Elasticsearch
type writer struct {
	lock    sync.RWMutex
	db      string
	
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
	err := b.hdfsAppender.Close()
	if err != nil {
		return err
	}
}

func (b *writer) getEsBulker() *ESBulker  {
	index := fmt.Sprintf("ari.%s.%s", b.db, b.shard)
	return NewEsBulker(b.es, index)
}

// Write batch to db
func (b *writer) Write(ctx context.Context, batch []*harvesterd.Entry) error {
	if len(batch) == 0 {
		return nil
	}
	
	off, err := b.hdfsAppender.CurrentOff()
	if err != nil {
		return err
	}
	fd := b.hdfsAppender.GetFD()
	bulker := b.getEsBulker()
	ckBuilder := NewChunkBuilder()
	for i, et := range batch {
		if i >= math.MaxInt16 {
			return errors.New("the batch size is too large")
		}
		ckBuilder.AddRow([]byte(et.Body))
		rk := MakeRowKey(fd, off, uint16(i))
		et.Body = rk.EncodeBase64()
		bulker.Add(DumpEntry(et))
	}
	chunk := ckBuilder.Build()
	
	// lock to write
	b.lock.Lock()
	_, err = b.hdfsAppender.Append(chunk.Bytes())
	if err != nil {
		return err
	}
	err = bulker.Do(ctx)
	b.lock.Unlock()
	return err
}
