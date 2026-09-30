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

// Beat 更新存活节点（健康或排空中）的心跳时间戳；
// 已判定死亡的节点不会因迟到心跳复活。
func (n *Node) Beat(now int64) {
	if n.isAlive() && now > n.rt.LastBeat {
		n.rt.LastBeat = now
	}
}

// MarkDead 将节点置为死亡并清空其占用账本（上面的任务由 Executor 统一回收）。
func (n *Node) MarkDead() {
	n.rt.State = api.NodeDead
	n.rt.Used = api.Resource{}
}

// BeginDrain 将健康节点置为排空中：从这一刻起不再接收新任务，
// 但心跳照常刷新（它是计划内维护，不是故障），存量任务由调度器迁移。
func (n *Node) BeginDrain() bool {
	if n.rt.State != api.NodeHealthy {
		return false
	}
	n.rt.State = api.NodeDraining
	return true
}

// Retire 在排空完成（账本无占用）后将节点退役为 Dead，
// 容量从此从集群可用容量中永久扣除。仍有占用或不在排空态时返回 false。
func (n *Node) Retire() bool {
	if n.rt.State != api.NodeDraining || n.rt.Used != (api.Resource{}) {
		return false
	}
	n.rt.State = api.NodeDead
	return true
}

// MarkRetired 直接把节点标记为退役（Dead）并清空占用账本。
// 用于重启恢复：快照记录为“已退役”的节点重新注册时必须仍按退役处理，
// 绝不能作为健康机器复活、让容量涨回去。
func (n *Node) MarkRetired() {
	n.rt.State = api.NodeDead
	n.rt.Used = api.Resource{}
}

// Readmit 把一个已退役（Dead）节点作为运维显式重新上线的机器恢复为健康状态，
// 账本清零、心跳时间戳由后续 Beat 刷新。
// 节点不是退役态（仍健康/排空中）时返回 false，避免“重新上线”被误用于
// 摘除一台正在服务的机器。
func (n *Node) Readmit() bool {
	if n.rt.State != api.NodeDead {
		return false
	}
	n.rt.State = api.NodeHealthy
	n.rt.Used = api.Resource{}
	return true
}

// replaceCapacity 以新容量重建一个已存在节点的容量视图（重新上线时可携带新规格）。
func (n *Node) replaceCapacity(capacity api.Resource) {
	n.rt.Capacity = capacity
	n.rt.Used = api.Resource{}
}

func (n *Node) isHealthy() bool { return n.rt.State == api.NodeHealthy }

// isAlive 表示节点仍“活着”（健康或计划内排空中），需要心跳、会被超时检测。
func (n *Node) isAlive() bool {
	return n.rt.State == api.NodeHealthy || n.rt.State == api.NodeDraining
}

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
