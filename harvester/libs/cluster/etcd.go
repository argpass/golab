package cluster

import (
	"context"
	"github.com/coreos/etcd/clientv3"
	"github.com/dbjtech/golab/pending/utils"
	"sync/atomic"
	"sync"
	"github.com/coreos/etcd/mvcc/mvccpb"
)

const (
	EVT_UPDATE = "UPDATE"
	EVT_CREATE = "CREATE"
	EVT_DELETE = "DELETE"
)

// Watcher watchs a key or a key range
// to keep be aware of changes on keys
type Watcher interface {
	// Bind method should never to be blocked
	Watch(ctx context.Context, resp *clientv3.GetResponse, ch clientv3.WatchChan) error
}

//type singleValueAware struct {
//	value interface{}
//	key string
//	decodeFn func([]byte, v interface{}) error
//	encodeFn func(v interface{}) ([]byte, error)
//}
//
//func (v *singleValueAware) Bind(resp *clientv3.GetResponse, ch clientv3.WatchChan) error {
//	for _, r := range resp.Kvs {
//		if string(r.Key) == v.key {
//
//			r.Value
//		}
//	}
//	if len(resp.Kvs) > 0 {
//	}
//}


type Etcd3 struct {
	*clientv3.Client
}

func (e *Etcd3) Watch(ctx context.Context, key string, watcher Watcher) (error) {
	resp, err := e.Get(ctx, key)
	if err != nil {
		return  err
	}
	if err != nil {
		return err
	}
	start_rev := resp.Header.Revision
	wc := e.Client.Watch(ctx, key,
		clientv3.WithRev(start_rev),
		clientv3.WithPrevKV(),
		clientv3.WithPrefix())
	err = watcher.Watch(ctx, resp, wc)
	if err != nil {
		return err
	}
	return nil
}

func MakeValueViewer(decodeFn func([]byte) (interface{}, error)) (*ValueViewer, error) {
	return &ValueViewer{decodeFn:decodeFn}, nil
}

type ValueGetter interface {
	Get() (val interface{}, deleted bool)
}

type ValueViewer struct {
	lock        sync.RWMutex
	isDeleted   bool
	value       atomic.Value
	decodeFn    func([]byte)(interface{}, error)
}

func (v *ValueViewer) Get() (val interface{}, deleted bool) {
	v.lock.RLock()
	deleted = v.isDeleted
	val = v.value.Load()
	v.lock.RUnlock()
	return
}

func (v *ValueViewer) OnCreate(data []byte) (error) {
	v.lock.Lock()
	val, err := v.decodeFn(data)
	if err != nil {
		v.lock.Unlock()
		return err
	}
	v.lock.Unlock()
	v.value.Store(val)
	return nil
}

func (v *ValueViewer) OnDelete() (error) {
	v.lock.Lock()
	v.isDeleted = true
	v.value.Store(nil)
	v.lock.Unlock()
	return nil
}

func (v *ValueViewer) OnUpdate(data []byte) (error) {
	v.lock.Lock()
	val , err := v.decodeFn(data)
	if err != nil {
		v.lock.Unlock()
		return err
	}
	v.value.Store(val)
	v.lock.Unlock()
	return nil
}

type ValueEvtViewer struct {
	*ValueViewer
	NotifyFn func(evt string)
}

func (v *ValueEvtViewer) OnCreate(data []byte) (error) {
	err := v.ValueViewer.OnCreate(data)
	v.NotifyFn("CREATE")
	return err
}

func (v *ValueEvtViewer) OnUpdate(data []byte) (error)  {
	err := v.ValueViewer.OnUpdate(data)
	v.NotifyFn("UPDATE")
	return err
}

func (v *ValueEvtViewer) OnDelete() (error) {
	err := v.ValueViewer.OnDelete()
	v.NotifyFn("DELETE")
	return err
}

type IsKeyWatcher interface {
	OnCreate([]byte) (error)
	OnDelete() (error)
	OnUpdate([]byte) (error)
}

type ValueWatcher struct {
	lock        sync.RWMutex
	keyWatchers map[string]IsKeyWatcher
	wg          utils.WrappedWaitGroup
	errFn       func(error)
	keyPrefix string
	
	ctx         context.Context
}

func NewValueWatcher(
		keyPrefix string,
		errFn func(error),
		keyWatchers map[string]IsKeyWatcher,
		) *ValueWatcher {
	
	m := &ValueWatcher{
		errFn:errFn,
		keyPrefix:keyPrefix,
		keyWatchers: keyWatchers,
	}
	return m
}

func (m *ValueWatcher) init(resp *clientv3.GetResponse) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	for _, r := range resp.Kvs {
		w, ok := m.keyWatchers[string(r.Key)]
		if !ok {
			continue
		}
		err := w.OnUpdate(r.Value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *ValueWatcher) Watch(
		ctx context.Context,
		resp *clientv3.GetResponse,
		ch clientv3.WatchChan) error {
	
	// init value
	err := m.init(resp)
	if err != nil {
		return err
	}
	// loop watch chan,
	// in case of blocking current goroutine,
	// we start a new goroutine to run the loop
	m.wg.Wrap(func(){
		for {
			select {
			case evts := <-ch:
				for _, evt := range evts.Events {
					m.lock.Lock()
					keyWacher, ok := m.keyWatchers[string(evt.Kv.Key)]
					m.lock.Unlock()
					if !ok {
						continue
					}
					if evt.Type.EnumDescriptor()[1] == clientv3.EventTypeDelete {
						err = keyWacher.OnDelete()
					}else if evt.IsCreate() {
						err = keyWacher.OnCreate(evt.Kv.Value)
					}else{
						err = keyWacher.OnUpdate(evt.Kv.Value)
					}
					if err != nil {
						if m.errFn != nil {
							m.errFn(err)
						}
					}
				}
			case <-m.ctx.Done():
				break
			}
		}
	})
	return nil
}

type keysWatcher struct {
	evtNotifyFn func(evt string, v *mvccpb.KeyValue)
	initFn func(kvs []*mvccpb.KeyValue)
}

func NewKeysWatcher(
	init func(kvs []*mvccpb.KeyValue),
	cb func(evt string, value *mvccpb.KeyValue)) *keysWatcher {
	
	w := &keysWatcher{evtNotifyFn: cb, initFn:init}
	return w
}

func (m *keysWatcher) Watch(
	ctx context.Context,
	resp *clientv3.GetResponse,
	ch clientv3.WatchChan) error {
	
	// init value
	m.initFn(resp.Kvs)
	// loop watch chan,
	// in case of blocking current goroutine,
	// we start a new goroutine to run the loop
	go func(){
		for {
			select {
			case evts := <-ch:
				for _, evt := range evts.Events {
					if evt.Type.EnumDescriptor()[1] == clientv3.EventTypeDelete {
						m.evtNotifyFn(EVT_DELETE, evt.Kv)
					}else if evt.IsCreate() {
						m.evtNotifyFn(EVT_CREATE, evt.Kv)
					}else{
						m.evtNotifyFn(EVT_UPDATE, evt.Kv)
					}
				}
			case <-ctx.Done():
				break
			}
		}
	}()
	return nil
}

