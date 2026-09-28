package cluster

import "github.com/lisama569/gang-scheduler-go/pkg/api"

// Node 是一个模拟节点：维护容量、已占用资源与心跳时间戳。
// Node 自身不加锁，所有访问经由 Pool 串行化。
type Node struct {
	rt api.NodeRuntime
}

func NewNode(id string, capacity api.Resource) *Node {
	return &Node{rt: api.NodeRuntime{
		ID:       id,
		Capacity: capacity,
		State:    api.NodeHealthy,
	}}
}

func (n *Node) Snapshot() api.NodeRuntime { return n.rt }

func (n *Node) Available() api.Resource {
	return n.rt.Capacity.Sub(n.rt.Used)
}

// Beat 更新健康/排空中节点的心跳时间戳；已判死或已退役的节点不会因迟到心跳复活。
// 计划内排空是“节点仍在线、只是不再接新任务”，因此排空中也继续续心跳，
// 避免被超时检测误判为故障。
func (n *Node) Beat(now int64) {
	if (n.rt.State == api.NodeHealthy || n.rt.State == api.NodeDraining) && now > n.rt.LastBeat {
		n.rt.LastBeat = now
	}
}

// MarkDead 将节点置为死亡并清空其占用账本（上面的任务由 Executor 统一回收）。
func (n *Node) MarkDead() {
	n.rt.State = api.NodeDead
	n.rt.Used = api.Resource{}
}

// Cordon 将一个健康节点置为排空（计划内维护）。只有健康节点可排空。
func (n *Node) Cordon() bool {
	if n.rt.State != api.NodeHealthy {
		return false
	}
	n.rt.State = api.NodeDraining
	return true
}

// Retire 将排空节点正式退役。调用方须保证账本已清空。
func (n *Node) Retire() bool {
	if n.rt.State != api.NodeDraining {
		return false
	}
	n.rt.State = api.NodeRetired
	return true
}

// restoreState 在重启恢复时按快照重放节点生命周期状态。
func (n *Node) restoreState(state string) {
	n.rt.State = state
}

func (n *Node) isHealthy() bool  { return n.rt.State == api.NodeHealthy }
func (n *Node) isDraining() bool { return n.rt.State == api.NodeDraining }
func (n *Node) isRetired() bool  { return n.rt.State == api.NodeRetired }

// allocate 尝试在健康节点上占用 r，成功返回 true。排空中/死亡/退役节点一律拒绝。
func (n *Node) allocate(r api.Resource) bool {
	if !n.isHealthy() || !r.Fits(n.Available()) {
		return false
	}
	n.rt.Used = n.rt.Used.Add(r)
	return true
}

// allocateRaw 只做容量校验的占用，供“跨节点迁移回滚”等内部记账修正使用：
// 调用方负责语义（例如目标节点本就是健康节点、资源先前确实被扣走）。
func (n *Node) allocateRaw(r api.Resource) bool {
	if !r.Fits(n.Available()) {
		return false
	}
	n.rt.Used = n.rt.Used.Add(r)
	return true
}

// release 归还资源；归还量超过占用量时返回 false（账本不变）。
func (n *Node) release(r api.Resource) bool {
	if r.CPU > n.rt.Used.CPU || r.Memory > n.rt.Used.Memory {
		return false
	}
	n.rt.Used = n.rt.Used.Sub(r)
	return true
}
