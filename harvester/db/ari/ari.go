package ari

import (
	"sync"
	"context"
	"fmt"
	"github.com/dbjtech/golab/pending/utils"
	"github.com/olivere/elastic"
	"github.com/colinmarc/hdfs"
	"github.com/elastic/beats/libbeat/common"
	"github.com/dbjtech/golab/harvester/db"
	"github.com/dbjtech/golab/harvester/libs/constant"
	"go.uber.org/zap"
	"github.com/dbjtech/golab/harvester/libs/cluster"
	"github.com/dbjtech/golab/harvester/libs/cluster/pb"
	"github.com/pkg/errors"
	"github.com/dbjtech/golab/harvester/db/ari/pb"
	"github.com/gogo/protobuf/proto"
	"encoding/json"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"strings"
	"strconv"
)

func init()  {
	// register ari engine creator
	db.RegisterEngine("ari",
		func(name string,
		cfg *common.Config,
		ctx context.Context) (db.IsEngine, error){
			a, err := New(name, cfg)
			if err != nil {
				return nil, err
			}
			err = a.Start(ctx)
			if err != nil {
				return nil, err
			}
			return a, nil
	})
}

// Ari is a db engine based on the `HDFS` and `Elasticsearch`.
// the HDFS stores raw log data and index data stores in the `Elasticsearch`.
// The Ari runs as a cluster with `etcd` service,
// there is a master in the cluster to manage locks and meta data
type Ari struct {
	wg 		    utils.WrappedWaitGroup
	lock 		sync.RWMutex
	name 		string
	rawConfig   *common.Config
	cfg         Config
	
	// metaGetter is used to get databases meta info
	// metaViewer
	metaGetter  cluster.ValueGetter
	metaViewer  *cluster.ValueEvtViewer
	
	// dbOptsMap holds options of all databases
	dbOptsMap   map[string]*DBOptions
	
	// waiters are used to manage db connections for every db
	waiters     map[string]*DbWaiter
	
	ctx    context.Context
	cancel context.CancelFunc
	logger *zap.Logger
	Node   *cluster.Node
	ES     *elastic.Client
	Hdfs   *HdfsPort
	
	masterCtx   context.Context
	masterStop  context.CancelFunc
	
}

///////////////////////////////// Lifecycle ///////////////////////////////

func New(name string, cfg *common.Config) (*Ari, error)  {
	// copy defaults and unpack from cfg
	cf := defaultConfig
	err := cfg.Unpack(&cf)
	if err != nil {
		return nil, err
	}
	vv, err := cluster.MakeValueViewer(func(data []byte)(interface{}, error) {
		return nil, nil
	})
	if err != nil {
		return nil, err
	}
	a := &Ari{
		name:       name,
		rawConfig:  cfg,
		cfg:        cf,
		metaGetter: vv,
		dbOptsMap:  map[string]*DBOptions{},
		waiters:    map[string]*DbWaiter{},
	}
	a.metaViewer = &cluster.ValueEvtViewer{
		ValueViewer:vv,
		NotifyFn:func(evt string) {a.onMetaChanged(evt)},
	}
	return a, nil
}

// initHandlers registers master handlers
// todo:这代码又臭又长，后面改造
func (a *Ari) initHandlers()  {
	
	// allocate a fd
	a.Node.OnCallMaster(
		"ari",
		"allocate_fd",
		func(req *pb.Req) (*pb.Resp){
			var resp pb.Resp = pb.Resp{Status:0, Msg:""}
			var ar ari_call.AllocateFdReq
			err := proto.Unmarshal(req.ReqBuf, &ar)
			if err != nil {
				resp.Status = -1
				resp.Msg = fmt.Sprintf("decode buf err:%v", err)
			}
			
			metas := a.ShouldGetMeta()
			if metas == nil {
				resp.Status = 1
				resp.Msg = "meta absent, engine fatal"
				return &resp
			}
			
			dbMeta, ok := metas.Dbs[ar.Db]
			if !ok {
				resp.Status = 11
				resp.Msg = "no such db"
				return &resp
			}
			
			sMeta, ok := dbMeta.Shards[ar.Shard]
			if !ok {
				resp.Status = 12
				resp.Msg = "no such shard"
				return &resp
			}
			
			var fd int
			var success bool
			for i := 0 ; i < sMeta.MaxFd; i++ {
				if _, ok := sMeta.FdLocked[i]; !ok {
					fd = i
					success = true
				}
			}
			if !success {
				resp.Status = 13
				resp.Msg = "no fd can be allocated"
				return &resp
			}
			
			data := ari_call.AllocateFdResp{Db:ar.Db, Shard:ar.Shard, Fd:uint32(fd)}
			buf, err := proto.Marshal(&data)
			if err != nil {
				resp.Status = -1
				resp.Msg = "encode response fail"
				return &resp
			}
			
			// commit meta change to etcd
			sMeta.FdLocked[fd] = req.Node.NodeId
			metaJs, err := json.Marshal(metas)
			if err != nil {
				resp.Status = -1
				resp.Msg = fmt.Sprintf("encode metas fail, err:%v", err)
				return &resp
			}
			dbMetaKey := fmt.Sprintf("%s.ari.db", a.Node.ClusterName.String())
			
			// never to bind the key to some lease,
			// metas is global
			_, err = a.Node.Etcd3.Put(a.ctx, dbMetaKey, string(metaJs))
			if err != nil {
				resp.Msg = fmt.Sprintf("update metas fail, err:%v", err)
				resp.Status = -1
				return &resp
			}
			
			// response to caller
			resp.RespBuf = buf
			return &resp
		},
	)
	
	// free a fd
	a.Node.OnCallMaster(
		"ari",
		"free_fd",
		func(req *pb.Req) (*pb.Resp){
			var resp pb.Resp = pb.Resp{Status:0, Msg:""}
			var ar ari_call.FreeFdReq
			err := proto.Unmarshal(req.ReqBuf, &ar)
			if err != nil {
				resp.Status = -1
				resp.Msg = fmt.Sprintf("decode buf err:%v", err)
			}
			
			metas := a.ShouldGetMeta()
			if metas == nil {
				resp.Status = 1
				resp.Msg = "meta absent, engine fatal"
				return &resp
			}
			
			dbMeta, ok := metas.Dbs[ar.Db]
			if !ok {
				resp.Status = 11
				resp.Msg = "no such db"
				return &resp
			}
			
			sMeta, ok := dbMeta.Shards[ar.Shard]
			if !ok {
				resp.Status = 12
				resp.Msg = "no such shard"
				return &resp
			}
			_, ok = sMeta.FdLocked[int(ar.Fd)]
			if !ok {
				return &resp
			}
			
			delete(sMeta.FdLocked, int(ar.Fd))
			// commit meta change to etcd
			metaJs, err := json.Marshal(metas)
			if err != nil {
				resp.Status = -1
				resp.Msg = fmt.Sprintf("encode metas fail, err:%v", err)
				return &resp
			}
			dbMetaKey := fmt.Sprintf("%s.ari.db", a.Node.ClusterName.String())
			
			// never to bind the key to some lease,
			// metas is global
			_, err = a.Node.Etcd3.Put(a.ctx, dbMetaKey, string(metaJs))
			if err != nil {
				resp.Msg = fmt.Sprintf("update metas fail, err:%v", err)
				resp.Status = -1
				return &resp
			}
			
			return nil
		},
	)
}

// Start the engine
func (a *Ari) Start(ctx context.Context) error {
	var err error
	// init
	a.logger = ctx.Value(constant.KEY_LOGGER).(*zap.Logger)
	a.Node = ctx.Value(constant.KEY_NODE).(*cluster.Node)
	a.ctx, a.cancel = context.WithCancel(ctx)
	//pWg := ctx.Value(constant.KEY_P_WG).(*utils.WrappedWaitGroup)
	
	// keep viewing db meta
	clusterName := a.Node.ClusterName.String()
	dbMetaKey := fmt.Sprintf("%s.ari.db", clusterName)
	w := cluster.NewValueWatcher(
		// prefix
		dbMetaKey,
		// handle watch errors
		func(err error){
			a.Fatal(err, "watch db meta err")
		},
		map[string]cluster.IsKeyWatcher{
			dbMetaKey:a.metaViewer,
		})
	err = a.Node.Etcd3.Watch(a.ctx, dbMetaKey, w)
	if err != nil {
		return err
	}
	
	a.initHandlers()
	
	// register master tasks
	a.Node.RegMasterTask(
		cluster.MakeMasterTask(
			a.ctx, "ari_master",
			func(ctx context.Context){
				a.runningInMasterTask(ctx)
			},
		),
	)
	
	// es, hdfs client
	a.ES, err = elastic.NewClient(elastic.SetURL(a.cfg.ES.Addrs...))
	if err != nil {
		a.logger.Error(fmt.Sprintf("fail to new client of es, err:%v", err))
		return err
	}
	hd, err := hdfs.New(a.cfg.HDFS.Addr)
	if err != nil {
		a.logger.Error(fmt.Sprintf("fail to new client of hdfs, err:%v", err))
		return err
	}
	a.Hdfs = &HdfsPort{Client: hd}
	
	return nil
}

func (a *Ari) Fatal(err error, msg string)  {
	a.logger.Error(fmt.Sprintf("fatal err:%s, msg:%s", err.Error(), msg))
	// cancel self
	a.cancel()
}

///////////////////////////////// db meta ///////////////////////////////

func (a *Ari) onMetaChanged(evt string) {
	// if meta deleted, all db dropped, close all waiters
	if evt == "DELETE" {
		a.Fatal(errors.New("meta deleted"), "")
		return
	}
	meta := a.GetMeta()
	// ignore nil meta
	if meta == nil {
		return
	}
	
	if evt == "CREATE" || evt == "UPDATE" {
		// notify all waiters to check db meta
		a.lock.Lock()
		for dbName, wt := range a.waiters {
			m, ok := meta.Dbs[dbName]
			if ok {
				wt.CheckDbMeta(m)
			}
			// todo: what to do if the db meta deleted ?
		}
		a.lock.Unlock()
	}
}

func (a *Ari) GetMeta() *Meta {
	v, deleted := a.metaGetter.Get()
	if deleted {
		return nil
	}
	m, ok := v.(*Meta)
	if !ok {
		// use check type, if not expected type, log error and fatal
		a.Fatal(fmt.Errorf("Get Unspected meta type:%s not (*Meta)", m), "")
		return nil
	}
	return m
}

// ShouldGetMeta get and check meta if meta is nil, engine will be fatal
func (a *Ari) ShouldGetMeta() *Meta {
	metas := a.GetMeta()
	if metas == nil {
		a.Fatal(errors.New("meta is absent"), "")
		return nil
	}
	return metas
}

///////////////////////////////// master calling ///////////////////////////////

func unpackFdKey(prefix string, rawKey string) (db string, shard string, fd int, err error) {
	var fdS string
	var fd64 int64
	key := string(rawKey)[len(prefix):]
	ks := strings.Split(key, ".")
	db, shard, fdS = ks[0], ks[1], ks[2]
	fd64, err = strconv.ParseInt(fdS, 10, 16)
	if err != nil {
		return
	}
	fd = int(fd64)
	return
}

func (a *Ari) runningInMasterTask(ctx context.Context)  {
	// keep metas valid
	// busy fds: {cluster}.ari.busy_fd.{db}.{shard}.{fd}
	clusterName := a.Node.ClusterName.String()
	busyFdPrefix := fmt.Sprintf("%s.ari.busy_fd.", clusterName)
	dbMetaKey := fmt.Sprintf("%s.ari.db", clusterName)
	w := cluster.NewKeysWatcher(
		
		// check meta with busy fds in level init
		func(kvs []*mvccpb.KeyValue){
			var fdMap = map[string]map[string]map[int]string{}
			for _, kv := range kvs {
				dbName, shard, fd, err := unpackFdKey(busyFdPrefix, string(kv.Key))
				if err != nil {
					continue
				}
				if _, ok := fdMap[dbName]; !ok {
					fdMap[dbName]=map[string]map[int]string{}
				}
				if _, ok := fdMap[shard]; !ok {
					fdMap[dbName][shard]=map[int]string{}
				}
				fdMap[dbName][shard][fd]=string(kv.Value)
			}
			
			metas := a.ShouldGetMeta()
			if metas == nil {
				return
			}
			for _, dbMeta := range metas.Dbs {
				for shardName := range dbMeta.Shards {
					var fdLocked = map[int]string{}
					if _, ok := fdMap[dbMeta.Name]; ok {
						if m, ok := fdMap[dbMeta.Name][shardName]; ok {
							dbMeta.Shards[shardName].FdLocked = m
							continue
						}
					}
					dbMeta.Shards[shardName].FdLocked = fdLocked
				}
			}
			
			// update to etcd
			data, _ := json.Marshal(metas)
			_, err := a.Node.Etcd3.Put(ctx, dbMetaKey, string(data))
			if err != nil {
				// todo: handle err
				a.logger.Error(fmt.Sprintf("put meta to etcd err:%v", err))
			}
		},
		
		// watch busy fds, when busy fds deleted,
		// collect them to update meta info
		// only track delete event
		func(evt string, value *mvccpb.KeyValue){
			if evt != cluster.EVT_DELETE {
				return
			}
			
			// fd expired, release it
			key := string(value.Key)[len(busyFdPrefix):]
			ks := strings.Split(key, ".")
			// check key
			if len(ks) != 3 {
				a.logger.Error(fmt.Sprintf("invalid fd key:%s", value.Key))
				return
			}
			dbName, shard, fd, err := unpackFdKey(busyFdPrefix, key)
			if err != nil {
				a.logger.Error(fmt.Sprintf("unpack busy fd key err:%v", err))
			}
			metas := a.ShouldGetMeta()
			if metas == nil {
				a.logger.Error("got nil meta in tracing busy fds")
				return
			}
			// fixme: may be panic
			delete(metas.Dbs[dbName].Shards[shard].FdLocked, fd)
			
			// update to etcd
			data, _ := json.Marshal(metas)
			_, err = a.Node.Etcd3.Put(ctx, dbMetaKey, string(data))
			if err != nil {
				// todo: handle err
				a.logger.Error(fmt.Sprintf("put meta to etcd err:%v", err))
			}
		},
	)
	err := a.Node.Etcd3.Watch(ctx, busyFdPrefix, w)
	if err != nil {
		a.Fatal(err, "")
	}
}

// AllocateFd allocate a hdfs fd of a shard
func (a *Ari) AllocateFd(db string, shard string) (uint16, error) {
	req := &ari_call.AllocateFdReq{Db:db, Shard:shard}
	rd, err := proto.Marshal(req)
	if err != nil {
		return 0, err
	}
	resp, err := a.Node.CallMaster(a.ctx, "ari", "allocate_fd", rd)
	if err != nil {
		return 0, err
	}
	if resp.Status != int32(0) {
		a.logger.Warn(
			fmt.Sprintf("[allocate-fd] call master status:%d, msg:%s",
				resp.Status, resp.Msg,
			),
		)
		return 0, errors.New("allocate fd fail")
	}
	var rp ari_call.AllocateFdResp
	err = proto.Unmarshal(resp.RespBuf, &rp)
	if err != nil {
		return 0, err
	}
	return uint16(rp.Fd), nil
}

// FreeFd frees a hdfs fd of a shard
// re freeing a fd is ok
func (a *Ari) FreeFd(db string, shard string, fd uint16) error  {
	req := &ari_call.FreeFdReq{Db:db, Shard:shard, Fd:uint32(fd)}
	rd, err := proto.Marshal(req)
	if err != nil {
		return err
	}
	resp, err := a.Node.CallMaster(a.ctx, "ari", "free_fd", rd)
	if err != nil {
		return err
	}
	if resp.Status != int32(0) {
		a.logger.Warn(fmt.Sprintf("[free-fd] call master status:%d, msg:%s",
			resp.Status, resp.Msg))
		return errors.New("free fd fail")
	}
	return nil
}

///////////////////////////////// IsEngine ///////////////////////////////

// Ensure registers db with options
// this method should be called before `Open`
func (a *Ari) Ensure(db string, cfg *common.Config) error {
	a.lock.Lock()
	var opts DBOptions
	err := cfg.Unpack(&opts)
	if err != nil {
		a.lock.Unlock()
		return err
	}
	a.dbOptsMap[db] = &opts
	a.lock.Unlock()
	return nil
}

// Open a db connection for `db`
func (a *Ari) Open(db string) (db.IsDBCon, error) {
	// if no such db in metas, fail
	metas := a.GetMeta()
	if metas == nil {
		return nil, errors.New("no meta info")
	}
	m, ok := metas[db]
	if !ok {
		return nil, errors.New("no such db configured")
	}
	
	var wt *DbWaiter
	
	a.lock.RLock()
	wt, exists := a.waiters[db]
	a.lock.RUnlock()
	
	// if waiter doesn't exist, create it
	if !exists {
		wt = newWaiter(db, a, m)
		err := wt.Start(a.ctx)
		if err != nil {
			return nil, err
		}
		
		a.GetMeta()
		a.lock.Lock()
		a.waiters[db] = wt
		a.lock.Unlock()
	}
	
	// allocate a connection using the waiter
	return wt.NewConnection(), nil
}

