package cluster

import (
	"context"
)

// MakeMasterTask wraps a master task for `fn`
func MakeMasterTask(
		ctx context.Context,
		id string,
		fn func(ctx context.Context)) IsMasterTask {
	
	t := &masterTaskTlp{
		rootCtx: ctx,
		stop:nil,
		fn:fn,
		ctx:nil,
		id:id,
	}
	return t
}

type masterTaskTlp struct {
	rootCtx context.Context
	ctx context.Context
	stop context.CancelFunc
	fn func(ctx context.Context)
	id string
}

func (t *masterTaskTlp) Attach() {
	if t.ctx == nil {
		t.ctx, t.stop = context.WithCancel(t.rootCtx)
		t.fn(t.ctx)
	}
}

func (t *masterTaskTlp) Detach() {
	if t.stop != nil {
		t.stop()
		t.ctx = nil
		t.stop = nil
	}
}

func (t *masterTaskTlp) Id() string {
	return t.id
}

//type ResTables struct {
//	lock sync.RWMutex
//	tables map[string]*ResTable
//}
//
//func (t *ResTables) Update(tb *ResTable) {
//	t.lock.Lock()
//	t.tables[tb.NodeId] = tb
//	t.lock.Unlock()
//}
//
//func (t *ResTables) Delete(nodeId string) *ResTable {
//	t.lock.Lock()
//	if tb, ok := t.tables[nodeId]; ok {
//		delete(t.tables, nodeId)
//		t.lock.Unlock()
//		return tb
//	}
//	t.lock.Unlock()
//	return nil
//}

// MasterNode
//type MasterNode struct {
//	*Node
//	resAllocators []IsResAllocator
//	communicators []Communicator
//}
//
//func (m *MasterNode) Communicate(c *Call) bool {
//	for _, communicator := range m.communicators {
//		re, ok := communicator.Call(c)
//		if ok {
//			c.Result = re
//			return ok
//		}
//	}
//	return false
//}
//
//func (m *MasterNode) freeRes(tb *ResTable) bool {
//	for _, allocator := range m.resAllocators {
//		allocator.FreeRes(tb)
//	}
//
//	// check if all resources freed
//	for _, v := range tb.Table {
//		if v != nil {
//			return false
//		}
//	}
//	return true
//}
//
//func (m *MasterNode) fillRes(tb *ResTable) bool {
//	for _, allocator := range m.resAllocators {
//		allocator.FillRes(tb)
//	}
//
//	// check if all required key filled
//	for _, v := range tb.Table {
//		if v == nil {
//			return false
//		}
//	}
//	return true
//}
//
//// todo:比较上次表中减少的key应该被释放；表中value为nil的key应该fill(重新分配)
//func (m *MasterNode) watchingRes() {
//	resKey := m.ClusterName.KeyRes()
//	wc, err := m.Etcd3.WatchKey(m.ctx, resKey)
//	if err != nil {
//		m.logger.Error(fmt.Sprintf("watchingRes err:%v", err))
//		m.ChangeToState(StateYellow)
//		m.Fatal(err, "")
//		return
//	}
//
//	// keep res tables
//	for {
//		select {
//		case <-m.ctx.Done():
//			break
//		case wr := <-wc:
//			for _, evt := range wr.Events {
//				// res key deleted(expired), i should collect the resources
//				if evt.Type.EnumDescriptor()[1] == clientv3.EventTypeDelete {
//					var table ResTable
//					err := json.Unmarshal(evt.PrevKv.Value, &table)
//					if err != nil {
//						msg := fmt.Sprintf("unmarshal json err:%v", err)
//						m.logger.Error(msg)
//						m.Fatal(err, msg)
//					}
//					// free resources
//					allDone := m.freeRes(&table)
//					if !allDone {
//						m.logger.Warn(fmt.Sprintf("not all res freed, table:%+v", table))
//					}
//				}
//				// res key modified, i should fill the table with some resources it requires
//				if evt.Type.EnumDescriptor()[1] == clientv3.EventTypePut && len(evt.Kv.Value) > 0 {
//					var table ResTable
//					err := json.Unmarshal(evt.Kv.Value, &table)
//					if err != nil {
//						msg := fmt.Sprintf("unmarshal json err:%v", err)
//						m.logger.Error(msg)
//						m.Fatal(err, msg)
//					}
//					// if not need fill, i will do nothing
//					if !table.NeedFill {
//						continue
//					}
//					// fill table
//					allDone := m.fillRes(&table)
//					if !allDone {
//						m.logger.Warn(fmt.Sprintf("not all res filled, table:%+v", table))
//					}
//					// i have filled it this time, so no need to fill it again
//					table.NeedFill = false
//					s, err := json.Marshal(table)
//					if err != nil {
//						msg := fmt.Sprintf("marshal json err:%v", err)
//						m.logger.Error(msg)
//						m.Fatal(err, msg)
//					}
//					// update to the etcd server
//					_, err = m.Etcd3.Put(
//						m.ctx, string(evt.Kv.Key),
//						string(s),
//						clientv3.WithLease(clientv3.LeaseID(evt.Kv.Lease)))
//					if err != nil {
//						msg := fmt.Sprintf("put restable err:%v", err)
//						m.logger.Error(msg)
//						m.Fatal(err, msg)
//					}
//				}
//			}
//		}
//	}
//	// normal exit
//	m.logger.Warn("watchingRes bye")
//}
//
//

