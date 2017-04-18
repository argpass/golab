package cluster

// IsResAllocator
type IsResAllocator interface {
	FreeRes(tb *ResTable)
	FillRes(tb *ResTable)
}

// 资源分配表
// 结点创建分配表，maser负责往表中注入分配的资源
// 结点down后，分配表会过期删除，master负责回收表中资源
// {cluster}._res.{node_id}
// {cluster}._chan.{node_id}.{cmd}

// db info 全部信息存入该key，由master维护，node创建空key
// 每个结点保持同步
// {cluster}.ari.db.{db}

type Caller interface {
	Call(c *Call) (result map[string]interface{}, accepted bool)
}

type Call struct {
	NodeId      string                  `json:"node_id"`
	NeedResult  bool                    `json:"need_result"`
	Cmd         string                  `json:"cmd"`
	PlayLoad    map[string]interface{}  `json:"play_load"`
	Result      map[string]interface{}  `json:"result"`
}

type ResTable struct {
	NodeId      string                  `json:"node_id"`
	NeedFill    bool                    `json:"need_fill"`
	Table       map[string]interface{}  `json:"table"`
}


