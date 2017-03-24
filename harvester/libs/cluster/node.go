package cluster

import (
	"context"
	"go.uber.org/zap"
	"github.com/dbjtech/golab/harvester/libs/constant"
	"fmt"
	"sync"
	"github.com/dbjtech/golab/pending/utils"
	"sync/atomic"
	"github.com/coreos/etcd/clientv3"
	"github.com/dbjtech/golab/harvester/libs/cluster/pb"
	"google.golang.org/grpc"
	"github.com/pkg/errors"
	"net"
	context2 "golang.org/x/net/context"
)

// ClusterName
// all cluster related keys should use ClusterName as namespace
type ClusterName string

func (s ClusterName) KeyMasterLock()  string {
	return fmt.Sprintf("%s._master", s.String())
}

func (s ClusterName) String() string {
	return string(s)
}

func (s ClusterName) KeyRes() string {
	return fmt.Sprintf("%s._res", s.String())
}

func (s ClusterName) KeyResTable(nodeId string) string  {
	return fmt.Sprintf("%s._res.%s", s.String(), nodeId)
}

func (s ClusterName) KeyChannel() string {
	return fmt.Sprintf("%s._chan", s.String())
}

const (
	StateYellow State = 0
	StateGreen  State = 1
	StateRed    State = 2
)

// State is node state
type State int

func (s State) String () string {
	switch s {
	case StateYellow:
		return "YELLOW"
	case StateGreen:
		return "GREEN"
	case StateRed:
		return "RED"
	}
	return "UNKNOWN"
}

type StateAware struct {
	lock sync.RWMutex
	state State
	subscribers     map[State] chan struct{}
}

// ChangeTo other state
func (s *StateAware) ChangeTo(v State) {
	s.lock.Lock()
	// trigger change event
	if s.state != v {
		s.state = v
		s.notify(v)
	}
	s.lock.Unlock()
}

func (s *StateAware) notify(v State)  {
	if ch, ok := s.subscribers[v]; ok {
		// notify all subscribers
		close(ch)
		// delete ch
		delete(s.subscribers, v)
	}
}

// Wait the State changed to `v`
// caller will recv change notification by the returned chan
func (s *StateAware) Wait(v State) <-chan struct{} {
	s.lock.Lock()
	if _, ok := s.subscribers[v]; !ok {
		s.subscribers[v] = make(chan struct{})
	}
	ch := s.subscribers[v]
	if s.state == v {
		s.notify(v)
	}
	s.lock.Unlock()
	return (<-chan struct{})(ch)
}

type IsMasterTask interface {
	
	// Attach the task to master when current node changes to the master
	// never to block on the method
	Attach()
	
	// Detach the task to master when current node changes to the master
	// never to block on the method
	Detach()
	
	// Id of the task
	Id() string
}

type allocReq struct {
	reAlloc     bool
	wChan       chan <-interface{}
	resKey         string
}

// Node of the Cluster
type Node struct {
	state       StateAware
	wg          utils.WrappedWaitGroup
	lock        sync.RWMutex
	// nodeId is also the net addr of node
	nodeId      string
	ClusterName ClusterName
	masterTasks map[string]IsMasterTask
	Etcd3       *Etcd3
	heartbeat   int64
	allocC      chan *allocReq
	msCaller    *masterCaller
	callHandles map[string]func(*pb.Req)*pb.Resp
	
	listener net.Listener
	LeaseId  clientv3.LeaseID
	isMaster uint32
	logger   *zap.Logger
	ctx      context.Context
	stop     context.CancelFunc
	nodeInfo pb.NodeInfo
	
	resTable    *ResTable
}

func NewNode(cluster string, listenAddr string, etcd3 *clientv3.Client) (*Node) {
	n := &Node{
		state:       StateAware{state:StateYellow},
		ClusterName: ClusterName(cluster),
		Etcd3:       &Etcd3{Client:etcd3},
		nodeId:      listenAddr,
		masterTasks: make(map[string]IsMasterTask),
		// heartbeat 10s
		heartbeat:10,
		allocC:make(chan *allocReq, 1024),
		msCaller:newMasterCaller(),
		callHandles: make(map[string]func(*pb.Req)*pb.Resp),
	}
	return n
}

func (n *Node) Fatal(err error, msg string) {
	n.logger.Error(fmt.Sprintf("node fatal with msg:%s, err:%v", msg, err))
	// revoke my lease
	n.Etcd3.Revoke(n.ctx, clientv3.LeaseID(n.LeaseId))
	// notify all sub goroutines created by me to exit
	n.stop()
	// start a routine to wait all routines to exit
	go func(){
		n.wg.Wait()
		n.logger.Info("bye")
	}()
	// todo: maybe i should use cluster state to notify other nodes that i'm down
}

// ChangeToState changes cluster state to v
func (n *Node) ChangeToState(v State) {
	n.state.ChangeTo(v)
	n.logger.Info(fmt.Sprintf("node change to state [%s]", v.String()))
}

// WaitState wait cluster state changed to v
func (n *Node) WaitState(v State) (<- chan struct{}){
	return n.state.Wait(v)
}

func (n *Node) IsMaster() bool {
	return atomic.LoadUint32(&n.isMaster) == uint32(1)
}

// RegTask registers master task
func (n *Node) RegMasterTask(task IsMasterTask) bool {
	n.lock.Lock()
	defer n.lock.Unlock()
	if _, exists := n.masterTasks[task.Id()]; exists {
		return false
	}
	n.masterTasks[task.Id()] = task
	// if i'm the master, attach the task right now
	if n.IsMaster() {
		task.Attach()
	}
	return true
}

// activeMaster active master tasks
func (n *Node) activeMaster() {
	for _, task := range n.masterTasks {
		task.Attach()
	}
}

// deactiveMaster deactive master tasks
func (n *Node) deactiveMaster()  {
	for _, task := range n.masterTasks {
		task.Detach()
	}
}

// touchMasterLockOnce to ensure master lock exist
func (n *Node) touchMasterLockOnce() error {
	n.logger.Debug("touch master lock")
	key_lock := n.ClusterName.KeyMasterLock()
	r, err := n.Etcd3.Txn(n.ctx).
	// if true, lock exists
	// else i create the lock with my lease
		If(clientv3.Compare(clientv3.CreateRevision(key_lock), ">", 0)).
		Else(clientv3.OpPut(key_lock, n.nodeId, clientv3.WithLease(n.LeaseId))).
		Commit()
	if err != nil {
		return err
	}
	if !r.Succeeded {
		// lock hasn't been created yet, so i create it
		n.logger.Debug("i create the master lock")
	}else {
		// lock exists already, check if i'm the master
		// if i'm the master, i active master tasks
		resp, err := n.Etcd3.Get(n.ctx, key_lock)
		if err != nil {
			return err
		}
		for _, kv := range resp.Kvs {
			if string(kv.Key) == key_lock {
				if string(kv.Value) == n.nodeId {
					n.activeMaster()
				}
			}
		}
	}
	return nil
}

// racingMaster watch master lock to keep the node state right
func (n *Node) raceMaster()  error {
	key_lock := n.ClusterName.KeyMasterLock()
	watchStart := make(chan struct{})
	watchCtx, stop := context.WithCancel(n.ctx)
	
	// start watch to watch lock's value
	// if value changed and value is my nodeId,
	// i change to master else i change to node
	n.wg.Wrap(func(){
		wC := n.Etcd3.Client.Watch(watchCtx, key_lock)
		handleEvt := func(evt *clientv3.Event) {
			// only watch the lock key
			if string(evt.Kv.Key) != key_lock {
				return
			}
			
			// lock was deleted, try to create it once again
			if evt.Type == clientv3.EventTypeDelete {
				// expire master caller connection
				n.msCaller.Invalid()
				err := n.touchMasterLockOnce()
				if err != nil {
					// todo: how to recovery the cluster state ?
					// todo: try to restart racingMaster?
					// racing routine gets fatal err, stop racing routine
					// cluster is invalid
					n.ChangeToState(StateYellow)
					stop()
					n.logger.Error(fmt.Sprintf("fail to ensure master lock, err:%v", err))
					return
				}
			}
			
			// any changes on the lock,
			// i should notify master caller to reconnect
			masterId := string(evt.Kv.Value)
			err := n.msCaller.Reconnect(masterId)
			if err != nil {
				n.msCaller.Invalid()
				n.logger.Info(fmt.Sprintf("fail to dial master on addr:%s, err:%v", masterId, err))
			}
			
			if string(evt.Kv.Value) == n.nodeId {
				// I have gotten the master lock
				// now i'm the master node
				if atomic.CompareAndSwapUint32(&n.isMaster, 0, 1) {
					n.activeMaster()
				}
			}else if string(evt.Kv.Value) != n.nodeId {
				// now i'm not the master node
				if atomic.CompareAndSwapUint32(&n.isMaster, 1, 0) {
					n.deactiveMaster()
				}
			}
		}
		close(watchStart)
		for {
			select {
			case <-watchCtx.Done():
				goto exit
			case resp := <-wC:
				err := resp.Err()
				if err != nil {
					n.logger.Error(fmt.Sprintf("master lock watch err:%v, " +
						"watch stop", err))
					goto exit
				}
				if resp.Canceled {
					// watch canceled
					n.logger.Info("master lock watch canceled")
					goto exit
				}
				for _, evt := range resp.Events {
					handleEvt(evt)
				}
			}
		}
		exit:
	})
	// wait for watcher to start
	<-watchStart
	// ensure that the master lock exists
	// watcher starts before, so PUT action on the lock is watched
	return n.touchMasterLockOnce()
}

func (n *Node) running() {
	// start keeping lease alive
	n.wg.Wrap(func(){
		n.Etcd3.KeepAlive(n.ctx, n.LeaseId)
	})
	
	// rpc server
	s := grpc.NewServer()
	pb.RegisterCallMasterServer(s, n)
	n.wg.Wrap(func() {
		select {
		case <-n.ctx.Done():
			err := n.listener.Close()
			n.logger.Info(fmt.Sprintf("listener closed, err:%+v", err))
		}
	})
	n.wg.Wrap(func() {
		err := s.Serve(n.listener)
		if err != nil {
			select {
			case <-n.ctx.Done():
				// pass
			default:
				n.logger.Error(fmt.Sprintf("rpc serve err:%+v", err))
			}
		}
	})
	// wait all sub goroutines to exit
	n.wg.Wait()
	n.logger.Info("bye")
}

// Start the cluster
func (n *Node) Start(ctx context.Context) error {
	pWg := ctx.Value(constant.KEY_P_WG).(*utils.WrappedWaitGroup)
	
	// init instance
	n.logger = ctx.Value(constant.KEY_LOGGER).(*zap.Logger).
		With(zap.String("node_id", n.nodeId))
	n.ctx, n.stop = context.WithCancel(ctx)
	
	// make and keep lease alive
	// the lease will be used to attach node related keys,
	// include the master lock created by me
	// if node down, the lease dies, all related keys would be expired
	resp, err := n.Etcd3.Grant(n.ctx, n.heartbeat)
	if err != nil {
		return err
	}
	n.LeaseId = resp.ID
	n.nodeInfo.NodeId = n.nodeId
	n.nodeInfo.LeaseId = int64(n.LeaseId)
	// start racing master
	err = n.raceMaster()
	if err != nil {
		return err
	}
	// create node listener
	listener, err := net.Listen("tcp", n.nodeId)
	if err != nil {
		return err
	}
	n.listener = listener
	
	pWg.Wrap(func(){
		n.running()
	})
	
	return nil
}

// RegCallMasterHandle registers handlers for master to response nodes' requests
func (n *Node) RegCallMasterHandle(
		namespace string,
		key string,
		fn func(req *pb.Req)*pb.Resp) {
	n.lock.Lock()
	n.callHandles[fmt.Sprintf("%s.%s", namespace, key)] = fn
	n.lock.Unlock()
}

////////////////////////////// call master rpc //////////////////////////////

func (n *Node) getNodeInfo() (*pb.NodeInfo) {
	return &n.nodeInfo
}

// CallMaster calls master by rpc client of node
func (n *Node) CallMaster(
	ctx context.Context,
	namespace string, key string, reqBuf []byte) (*pb.Resp, error) {
	
	c := n.msCaller.GetClient()
	if c == nil {
		return nil, errors.New("invalid rpc client")
	}
	req := &pb.Req{Namespace:namespace,Key:key,ReqBuf:reqBuf, Node:&n.nodeInfo}
	return c.Call(ctx, req)
}

// Call implement rpc server.
// to keep consistent with the rpc server interface always.
func (n *Node) Call(ctx context2.Context, req *pb.Req) (*pb.Resp, error) {
	if !n.IsMaster() {
		return &pb.Resp{Status:-1, Msg:"i'm not master"},
			errors.New("call on nonmaster node")
	}
	k := fmt.Sprintf("%s.%s", req.Namespace, req.Key)
	n.lock.RLock()
	handle, ok := n.callHandles[k]
	n.lock.RUnlock()
	if !ok {
		// no handler found to handle this request
		return &pb.Resp{Status:-1,
			Msg:fmt.Sprintf("no handler to accept this req on k:%s", k)},
			errors.New("call on nonmaster node")
	}
	return handle(req), nil
}

type masterCaller struct {
	lock sync.RWMutex
	
	client pb.CallMasterClient
	masterAddr string
}

func newMasterCaller() *masterCaller {
	return &masterCaller{}
}

func (c *masterCaller) Reconnect(masterAddr string) error {
	con, err := grpc.Dial(masterAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c.lock.Lock()
	c.masterAddr = masterAddr
	c.client = pb.NewCallMasterClient(con)
	c.lock.Unlock()
	return nil
}

func (c *masterCaller) Invalid()  {
	c.lock.Lock()
	c.client = nil
	c.masterAddr = ""
	c.lock.Unlock()
}

func (c *masterCaller) GetClient() (pb.CallMasterClient) {
	return c.client
}

