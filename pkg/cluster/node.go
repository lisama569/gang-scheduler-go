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

// Beat 更新健康节点的心跳时间戳；已判定死亡的节点不会因迟到心跳复活。
func (n *Node) Beat(now int64) {
	if n.rt.State == api.NodeHealthy && now > n.rt.LastBeat {
		n.rt.LastBeat = now
	}
}

// MarkDead 将节点置为死亡并清空其占用账本（上面的任务由 Executor 统一回收）。
func (n *Node) MarkDead() {
	n.rt.State = api.NodeDead
	n.rt.Used = api.Resource{}
}

func (n *Node) isHealthy() bool { return n.rt.State == api.NodeHealthy }

// allocate 尝试在节点上占用 r，成功返回 true。
func (n *Node) allocate(r api.Resource) bool {
	if !n.isHealthy() || !r.Fits(n.Available()) {
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
