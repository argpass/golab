package ari

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/colinmarc/hdfs"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/argpass/golab/harvester/db"
	"github.com/argpass/golab/harvester/db/ari/pb"
	"github.com/argpass/golab/harvester/libs/cluster"
	"github.com/argpass/golab/harvester/libs/cluster/pb"
	"github.com/argpass/golab/harvester/libs/constant"
	"github.com/argpass/golab/pending/utils"
	"github.com/elastic/beats/libbeat/common"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

func init() {
	// register ari engine creator
	db.RegisterEngine("ari",
		func(name string,
			cfg *common.Config,
			ctx context.Context) (db.IsEngine, error) {

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
	lock      sync.RWMutex
	name      string
	rawConfig *common.Config
	cfg       Config

	// metaGetter is used to get databases meta info
	// metaViewer
	metaGetter cluster.ValueGetter
	metaViewer *cluster.ValueEvtViewer

	// dbOptsMap holds options of all databases
	dbOptsMap map[string]*DBOptions

	// waiters are used to manage db connections for every db
	waiters map[string]*DbWaiter

	wg     utils.IsGroup
	ctx    context.Context
	logger *zap.Logger
	Node   *cluster.Node
	ES     *ESPort
	Hdfs   *HdfsPort

	masterCtx  context.Context
	masterStop context.CancelFunc
}

///////////////////////////////// Lifecycle ///////////////////////////////

func New(name string, cfg *common.Config) (*Ari, error) {
	// copy defaults and unpack from cfg
	cf := defaultConfig
	err := cfg.Unpack(&cf)
	if err != nil {
		return nil, errors.Wrap(err, "unpack ari engine config")
	}

	// make value viewer for the meta info
	vv, err := cluster.MakeValueViewer(func(data []byte) (interface{}, error) {
		var metas Meta
		err := json.Unmarshal(data, &metas)
		if err != nil {
			return nil, errors.Wrap(err, "unmarshal meta")
		}
		return &metas, nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "make value viewer for meta")
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
		ValueViewer: vv,
		NotifyFn:    func(evt string) { a.onMetaChanged(evt) },
	}
	return a, nil
}

func (a *Ari) cleanMeta() error {
	a.logger.Info("=======> clean meta <=========")
	key := a.getMetaKey()
	_, err := a.Node.Etcd3.Delete(a.ctx, key)
	if err != nil {
		return errors.Wrap(err, "delete meta")
	}
	a.logger.Info("delete meta success")
	return nil
}

// initHandlers registers master handlers
// todo:这代码又臭又长，后面改造
func (a *Ari) initHandlers() {

	// allocate a fd
	a.Node.RegCallMasterHandle(
		"ari",
		"allocate_fd",
		func(req *pb.Req) *pb.Resp {
			var resp pb.Resp = pb.Resp{Status: 0, Msg: ""}
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
			for i := 0; i < sMeta.MaxFd; i++ {
				if _, ok := sMeta.FdLocked[i]; !ok {
					fd = i
					success = true
					break
				}
			}
			if !success {
				resp.Status = 13
				resp.Msg = "no fd can be allocated"
				return &resp
			}

			data := ari_call.AllocateFdResp{Db: ar.Db, Shard: ar.Shard, Fd: uint32(fd)}
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
	a.Node.RegCallMasterHandle(
		"ari",
		"free_fd",
		func(req *pb.Req) *pb.Resp {
			var resp pb.Resp = pb.Resp{Status: 0, Msg: ""}
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

			return &resp
		},
	)
}

// Start the engine
func (a *Ari) Start(ctx context.Context) error {
	pWG := ctx.Value(constant.KEY_P_WG).(utils.IsGroup)
	var err error
	// init
	a.logger = ctx.Value(constant.KEY_LOGGER).(*zap.Logger)
	a.Node = ctx.Value(constant.KEY_NODE).(*cluster.Node)
	a.wg = utils.NewGroup(ctx)
	a.ctx = a.wg.Context()
	a.ctx = context.WithValue(a.wg.Context(), constant.KEY_P_WG, a.wg)

	a.logger.Debug("start...")

	//// todo-remove
	//return a.cleanMeta()

	// keep viewing db meta
	clusterName := a.Node.ClusterName.String()
	dbMetaKey := fmt.Sprintf("%s.ari.db", clusterName)
	w := cluster.NewValueWatcher(
		// prefix
		dbMetaKey,
		// handle watch errors
		func(err error) {
			a.Fatal(err, "watch db meta err")
		},
		map[string]cluster.IsKeyWatcher{
			dbMetaKey: a.metaViewer,
		})
	// now w initialized, a.metaViewer is also initialized
	err = a.Node.Etcd3.Watch(a.ctx, dbMetaKey, w)
	if err != nil {
		return errors.Wrap(err, "watch db meta key")
	}

	a.initHandlers()
	a.logger.Debug("init handlers done")

	// ensure metas initialized
	defaultMeta := Meta{}
	data, _ := json.Marshal(defaultMeta)
	resp, _ := a.Node.Etcd3.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(dbMetaKey), ">", 0)).
		Else(clientv3.OpPut(dbMetaKey, string(data))).Commit()
	if !resp.Succeeded {
		a.logger.Debug("i init the meta")

		// update local meta immediately
		// instead of waiting for etcd notification
		a.metaViewer.OnCreate(data)
	}

	// es port, hdfs port
	a.ES, err = NewESPort(a.cfg.ES.Addrs...)
	//a.logger.Warn(fmt.Sprintf("es addrs:%v", a.cfg.ES.Addrs))
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
	a.logger.Debug("es hdfs connected")

	// register master tasks
	// task will run immediately if i'm the master node
	a.Node.RegMasterTask(
		cluster.MakeMasterTask(
			a.ctx, "ari_master",
			func(ctx context.Context) {
				a.checkBusyFds(ctx)
			},
		),
	)
	a.logger.Debug("reg ari_master")
	a.Node.RegMasterTask(
		cluster.MakeMasterTask(
			a.ctx, "ari_sharding",
			func(ctx context.Context) {
				a.shardTask(ctx)
			},
		),
	)
	a.logger.Debug("reg master task done")

	pWG.Go(func() error {
		return a.running()
	})
	a.logger.Debug("startted")
	return nil
}

func (a *Ari) running() error {
	a.logger.Debug("running")
	a.wg.Go(func() error {
		select {
		case <-a.ctx.Done():
			return a.ctx.Err()
		}
	})
	err := a.wg.Wait()
	a.logger.Info("bye")
	return err
}

func (a *Ari) Fatal(err error, msg string) {
	// cancel self
	a.logger.Error(fmt.Sprintf("fatal err:%+v\n, msg:%s", err, msg))
	a.wg.Cancel(err)
}

///////////////////////////////// db meta ///////////////////////////////

func (a *Ari) onMetaChanged(evt string) {
	a.logger.Debug(fmt.Sprintf("meta changed evt:%s", evt))
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
		a.logger.Debug("notify all waiters to update metas")
		a.lock.Lock()
		for dbName, wt := range a.waiters {
			m, ok := meta.Dbs[dbName]
			if ok {
				wt.CheckDbMeta(*m)
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

// splitNewIndex creates a new shard index, add to meta and change the HotShard name
func (a *Ari) splitNewIndex(ctx context.Context, meta *DbMeta) error {
	shardMet := NewShardMeta()
	index := getEsRawIdx(meta.Name, shardMet.Name)
	err := EnsureIndex(ctx, meta.Name, index, a.ES.Client)
	if err != nil {
		return err
	}
	meta.Shards[shardMet.Name] = shardMet
	meta.HotShard = shardMet.Name
	return nil
}

func (a *Ari) shardTask(ctx context.Context) {
	wg := utils.NewGroup(ctx)
	wg.Go(func() error {
		return a.shardingTask(wg.Context())
	})

	a.wg.Go(func() error {
		err := wg.Wait()
		return err
	})
}

// shardTask check sharding condition and auto sharding
func (a *Ari) shardingTask(ctx context.Context) (err error) {
	a.logger.Debug("sharding task start")
	defer a.logger.Info("sharding task bye")
	for {
		// wait a moment
		runtime.Gosched()
		select {
		case <-ctx.Done():
			return
		case <-time.After(30 * time.Second):
			// to check per 10 seconds
		}

		// check once
		meta := a.GetMeta()
		if meta == nil {
			continue
		}

		var needUpdate bool
		for dbName, dbMeta := range meta.Dbs {
			if dbMeta == nil {
				continue
			}
			po, err := dbMeta.Options.ShardPolicy.ParsePolicy()
			if err != nil {
				a.Fatal(err, "invalid policy format")
				return err
			}
			hotShardMeta, ok := dbMeta.Shards[dbMeta.HotShard]
			if !ok {
				return errors.New(fmt.Sprintf(
					"no such hot shard %s in db %s, invalid db meta:%+v",
					dbMeta.HotShard, dbMeta.Name, *dbMeta))
			}

			// check duration
			if po.MaxDuration > 0 {
				t := time.Unix(hotShardMeta.CreateAt, 0)
				if time.Now().After(t.Add(po.MaxDuration)) {
					a.logger.Info(fmt.Sprintf(
						"db `%s` shard `%s` max duration exceed",
						dbMeta.Name, dbMeta.HotShard))
					err := a.splitNewIndex(ctx, dbMeta)
					if err != nil {
						a.Fatal(err, "split new index on max duration exceed")
						return err
					}
					needUpdate = true
				}
			}

			// check max index size
			if po.MaxIndexSize > 0 {
				idxName := getEsRawIdx(dbName, hotShardMeta.Name)
				info, err := a.ES.ReadIndexInfo(idxName)
				if err != nil {
					a.Fatal(err, "read index info")
					return err
				}
				if info.PriSizeG >= float32(po.MaxIndexSize) {
					a.logger.Info(fmt.Sprintf(
						"db %s shard `%s` max size exceed",
						dbMeta.Name, dbMeta.HotShard))
					err := a.splitNewIndex(ctx, dbMeta)
					if err != nil {
						a.Fatal(err, "split new index on max size exceed")
						return err
					}
					needUpdate = true
				}
			}
		}

		// check if need to update meta
		if needUpdate {
			dbMetaKey := a.getMetaKey()
			// update to etcd
			data, _ := json.Marshal(meta)
			_, err := a.Node.Etcd3.Put(ctx, dbMetaKey, string(data))
			if err != nil {
				a.Fatal(errors.Wrap(err, "[sharding] put meta to etcd"), "")
				return err
			}
		}
	}
}

func (a *Ari) checkBusyFds(ctx context.Context) {

	clusterName := a.Node.ClusterName.String()
	busyFdPrefix := fmt.Sprintf("%s.ari.busy_fd.", clusterName)
	dbMetaKey := a.getMetaKey()

	// keep metas valid,
	// check busy fds: {cluster}.ari.busy_fd.{db}.{shard}.{fd}
	w := cluster.NewKeysWatcher(

		// check meta with busy fds init level
		func(kvs []*mvccpb.KeyValue) {
			var fdMap = map[string]map[string]map[int]string{}
			for _, kv := range kvs {
				dbName, shard, fd, err := unpackFdKey(busyFdPrefix, string(kv.Key))
				if err != nil {
					continue
				}
				if _, ok := fdMap[dbName]; !ok {
					fdMap[dbName] = map[string]map[int]string{}
				}
				if _, ok := fdMap[shard]; !ok {
					fdMap[dbName][shard] = map[int]string{}
				}
				fdMap[dbName][shard][fd] = string(kv.Value)
			}

			metas := a.GetMeta()
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
				//a.logger.Error(fmt.Sprintf("put meta to etcd err:%v", err))
				a.Fatal(errors.Wrap(err, "[check-busyfd] put meta to etcd"), "")
				return
			}
		},

		// watch busy fds, when busy fds deleted,
		// collect them to update meta info
		// only track delete event
		func(evt string, value *mvccpb.KeyValue) {
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

			// delete the fd from locked map
			dbMeta, ok := metas.Dbs[dbName]
			if !ok {
				a.logger.Error(fmt.Sprintf("no such db %s", dbName))
				return
			}
			ss, ok := dbMeta.Shards[shard]
			if !ok {
				a.logger.Error(fmt.Sprintf("no such shard %s", shard))
				return
			}
			delete(ss.FdLocked, fd)

			// update to etcd
			data, _ := json.Marshal(metas)
			_, err = a.Node.Etcd3.Put(ctx, dbMetaKey, string(data))
			if err != nil {
				//a.logger.Error(fmt.Sprintf("put meta to etcd err:%v", err))
				a.Fatal(errors.Wrap(err, "[check-busyfd2] put meta to etcd"), "")
				return
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
	req := &ari_call.AllocateFdReq{Db: db, Shard: shard}
	rd, err := proto.Marshal(req)
	if err != nil {
		return 0, err
	}
	resp, err := a.Node.CallMaster(a.ctx, "ari", "allocate_fd", rd)
	if err != nil {
		return 0, err
	}
	if resp.Status != int32(0) {
		a.logger.Info(
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
	a.logger.Info(fmt.Sprintf("[allocate-fd]db:%s shard:%s fd:%d",
		db, shard, rp.Fd))
	return uint16(rp.Fd), nil
}

// FreeFd frees a hdfs fd of a shard
// re freeing a fd is ok
func (a *Ari) FreeFd(db string, shard string, fd uint16) error {
	defer a.logger.Info(fmt.Sprintf("[free-fd]:db %s shard %s fd %d\n", db, shard, fd))
	req := &ari_call.FreeFdReq{Db: db, Shard: shard, Fd: uint32(fd)}
	rd, err := proto.Marshal(req)
	if err != nil {
		return err
	}
	resp, err := a.Node.CallMaster(a.ctx, "ari", "free_fd", rd)
	if err != nil {
		return err
	}
	a.logger.Debug("call master to free fd")
	if resp.Status != int32(0) {
		a.logger.Info(fmt.Sprintf("[free-fd] call master status:%d, msg:%s",
			resp.Status, resp.Msg))
		return errors.New("free fd fail")
	}
	return nil
}

///////////////////////////////// IsEngine ///////////////////////////////

// Ensure registers db with options
// this method should be called before `Open`
func (a *Ari) Ensure(db string, cfg *common.Config) error {
	a.logger.Debug(fmt.Sprintf("ensure db %s", db))
	var opts DBOptions
	err := cfg.Unpack(&opts)
	if err != nil {
		return err
	}

	a.lock.Lock()
	if _, ok := a.dbOptsMap[db]; ok {
		a.lock.Unlock()
		return nil
	}
	a.dbOptsMap[db] = &opts
	a.lock.Unlock()

	metas := a.ShouldGetMeta()
	if metas == nil {
		return errors.New("get nil metas")
	}
	_, ok := metas.Dbs[db]
	if ok {
		return nil
	}

	// there isn't such db, so we should initialize it right now
	dbMeta, err := GetInitialDbMeta(db, opts)
	if err != nil {
		return err
	}
	rawIdx := getEsRawIdx(dbMeta.Name, dbMeta.HotShard)
	err = EnsureIndex(a.ctx, dbMeta.Name, rawIdx, a.ES.Client)
	if err != nil {
		return err
	}
	if metas.Dbs == nil {
		metas.Dbs = map[string]*DbMeta{}
	}
	metas.Dbs[db] = dbMeta
	// update metas to etcd
	dbMetaKey := a.getMetaKey()
	data, _ := json.Marshal(metas)
	_, err = a.Node.Etcd3.Put(a.ctx, dbMetaKey, string(data))
	if err != nil {
		return errors.Wrap(err, "put metas to etcd")
	}

	return nil
}

func (a *Ari) getMetaKey() string {
	clusterName := a.Node.ClusterName.String()
	dbMetaKey := fmt.Sprintf("%s.ari.db", clusterName)
	return dbMetaKey
}

// Open a db connection for `db`
func (a *Ari) Open(db string) (db.IsDBCon, error) {
	// if no such db in metas, fail
	metas := a.GetMeta()
	if metas == nil {
		return nil, errors.New("no meta info")
	}
	m, ok := metas.Dbs[db]
	if !ok {
		return nil, errors.New("no such db configured")
	}

	var wt *DbWaiter

	a.lock.RLock()
	wt, exists := a.waiters[db]
	a.lock.RUnlock()

	// if waiter doesn't exist, create it
	if !exists {
		wt = newWaiter(db, a, *m)
		waitersNum.Add(1)
		err := wt.Start(a.ctx)
		if err != nil {
			return nil, err
		}
		a.logger.Debug(fmt.Sprintf("new waiter for db %s", db))

		a.GetMeta()
		a.lock.Lock()
		a.waiters[db] = wt
		a.lock.Unlock()
	}

	// allocate a connection using the waiter
	return wt.NewConnection(), nil
}
