package cluster

import (
	"fmt"
	"sort"
	"sync"

	"github.com/lisama569/gang-scheduler-go/pkg/api"
)

// Placement 描述一个任务到节点的放置：NodeID 上分配 Request。
type Placement struct {
	TaskName string
	NodeID   string
	Request  api.Resource
}

// DeadNode 是一次心跳超时检测的结果：记录被回收的资源量，便于日志解释。
type DeadNode struct {
	ID       string
	Released api.Resource
}

// Pool 管理全部模拟节点，串行化所有资源变更。
type Pool struct {
	mu    sync.Mutex
	nodes map[string]*Node
	order []string // 注册顺序，保证节点遍历顺序确定
}

func NewPool() *Pool {
	return &Pool{nodes: map[string]*Node{}}
}

func (p *Pool) AddNode(id string, capacity api.Resource) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.nodes[id]; ok {
		return fmt.Errorf("node %s already registered", id)
	}
	if capacity.CPU < 0 || capacity.Memory < 0 {
		return fmt.Errorf("node %s has negative capacity", id)
	}
	p.nodes[id] = NewNode(id, capacity)
	p.order = append(p.order, id)
	return nil
}

func (p *Pool) Snapshot() []api.NodeRuntime {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]api.NodeRuntime, 0, len(p.order))
	for _, id := range p.order {
		out = append(out, p.nodes[id].Snapshot())
	}
	return out
}

// Node 返回单个节点（主要供测试与同包代码细粒度观测）。
// 注意返回值在锁外使用时仅反映调用瞬间的快照。
func (p *Pool) Node(id string) (*Node, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	n, ok := p.nodes[id]
	return n, ok
}

// BeatAll 为本轮仍存活（sim 层未故障注入）的健康节点刷新心跳。
func (p *Pool) BeatAll(now int64, alive map[string]bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, id := range p.order {
		if alive[id] {
			p.nodes[id].Beat(now)
		}
	}
}

// DetectDead 找出心跳超时的健康/排空中节点，标记死亡并回收其账本上的全部占用。
// 已退役节点无心跳也不会被判死（它是计划内下线，不是故障）。
func (p *Pool) DetectDead(now, timeoutTicks int64) []DeadNode {
	p.mu.Lock()
	defer p.mu.Unlock()
	var dead []DeadNode
	for _, id := range p.order {
		n := p.nodes[id]
		snap := n.Snapshot()
		if (n.isHealthy() || n.isDraining()) && now-snap.LastBeat > timeoutTicks {
			released := snap.Used
			n.MarkDead()
			dead = append(dead, DeadNode{ID: id, Released: released})
		}
	}
	return dead
}

// CordonNode 请求对一个健康节点做计划内维护/排空：
// 状态 Healthy -> Draining，从此不再接收新任务。
// 节点不存在或非健康（已排空/死亡/退役）时返回错误。
func (p *Pool) CordonNode(nodeID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	n, ok := p.nodes[nodeID]
	if !ok {
		return fmt.Errorf("node %s not registered", nodeID)
	}
	if !n.Cordon() {
		return fmt.Errorf("node %s is %s, only healthy nodes can be drained", nodeID, n.Snapshot().State)
	}
	return nil
}

// MigrateOne 把一份正在 from 节点执行的占用 r 原子迁移到 to 节点：
// 先在 to 上占用成功才从 from 归还；目标占用失败则双方账本不变并返回 false。
// 目标必须仍是健康节点（可能在计算放置与提交之间被排空）；
// 源节点必须仍是排空中的同一节点（防止并发状态变化下对错账动手）。
// 这保证迁移“要么完成、要么没发生”，不会重复占用也不会让资源凭空消失。
func (p *Pool) MigrateOne(from, to string, r api.Resource) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	src, ok := p.nodes[from]
	if !ok || !src.isDraining() {
		return false
	}
	dst, ok := p.nodes[to]
	if !ok || !dst.isHealthy() || !r.Fits(dst.Available()) {
		return false
	}
	if !dst.allocate(r) {
		return false
	}
	if !src.release(r) {
		// 理论不可达：调用方按在执行行记账；回滚目标占用保护账本。
		dst.release(r)
		return false
	}
	return true
}

// UndoMigrate 是迁移失败后的内部回滚：把先前成功的目标占用还原回源节点。
func (p *Pool) UndoMigrate(from, to string, r api.Resource) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	dst := p.nodes[to]
	src := p.nodes[from]
	if dst == nil || src == nil {
		return false
	}
	if !dst.release(r) {
		return false
	}
	if !src.allocateRaw(r) {
		// 理论不可达：源账本此前确有该占用。
		dst.allocateRaw(r)
		return false
	}
	return true
}

// RetireIfEmpty 把账本已清空的排空节点正式退役（Draining -> Retired）。
// 节点不存在、不是排空中或仍有占用时返回 false，账本不变。
func (p *Pool) RetireIfEmpty(nodeID string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	n, ok := p.nodes[nodeID]
	if !ok || !n.isDraining() {
		return false
	}
	snap := n.Snapshot()
	if snap.Used != (api.Resource{}) {
		return false
	}
	return n.Retire()
}

// DrainingIDs 返回当前处于排空中的节点 ID（按注册顺序）。
func (p *Pool) DrainingIDs() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	var ids []string
	for _, id := range p.order {
		if p.nodes[id].isDraining() {
			ids = append(ids, id)
		}
	}
	return ids
}

// RestoreNodeState 在重启恢复时按快照重放节点生命周期状态。
// 节点必须已重新注册；返回错误表示快照中的节点拓扑在本进程缺失。
func (p *Pool) RestoreNodeState(id, state string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	n, ok := p.nodes[id]
	if !ok {
		return fmt.Errorf("snapshot references unregistered node %s", id)
	}
	switch state {
	case api.NodeHealthy, api.NodeDraining, api.NodeDead, api.NodeRetired:
		n.restoreState(state)
		return nil
	default:
		return fmt.Errorf("unknown node state %q for node %s", state, id)
	}
}

// sortedHealthyIDs 返回健康节点 ID 的确定性排序（注册顺序即确定，这里再防御性排序）。
func (p *Pool) sortedHealthyIDsLocked() []string {
	ids := make([]string, 0, len(p.order))
	for _, id := range p.order {
		if p.nodes[id].isHealthy() {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

// Plan 为一个作业的全部未放置任务计算“全有或全无”的放置方案：
// 要么所有任务都能放入，返回 placements；否则返回 nil 且不做任何变更。
// 节点按 ID 排序、任务按提交顺序处理，保证不同调度顺序下决策一致。
func (p *Pool) Plan(taskReqs map[string]api.Resource, taskOrder []string) []Placement {
	p.mu.Lock()
	defer p.mu.Unlock()

	// tentative 累积本轮各节点的拟占用量。
	tentative := map[string]api.Resource{}
	var placements []Placement

	for _, name := range taskOrder {
		req := taskReqs[name]
		placed := false
		for _, id := range p.sortedHealthyIDsLocked() {
			n := p.nodes[id]
			snap := n.Snapshot()
			reserved := tentative[id]
			available := snap.Capacity.Sub(snap.Used.Add(reserved))
			if req.Fits(available) {
				tentative[id] = reserved.Add(req)
				placements = append(placements, Placement{TaskName: name, NodeID: id, Request: req})
				placed = true
				break
			}
		}
		if !placed {
			return nil // gang 语义：任一任务放不下，整体放弃
		}
	}
	return placements
}

// PlanTarget 为单个任务在健康节点中找一个能放下的目标节点（按 ID 确定性选取）。
// 不做任何账本变更；放不下返回空串。排空中/退役/死亡节点不会成为目标。
func (p *Pool) PlanTarget(req api.Resource, exclude string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, id := range p.sortedHealthyIDsLocked() {
		if id == exclude {
			continue
		}
		n := p.nodes[id]
		if req.Fits(n.Available()) {
			return id
		}
	}
	return ""
}

// TryCommit 原子提交一批放置：任一节点占用失败则整体不生效。
func (p *Pool) TryCommit(placements []Placement) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 先在暂记账本上验证全部可分配。
	want := map[string]api.Resource{}
	for _, pl := range placements {
		n, ok := p.nodes[pl.NodeID]
		if !ok || !n.isHealthy() {
			return false
		}
		want[pl.NodeID] = want[pl.NodeID].Add(pl.Request)
	}
	for id, total := range want {
		if !total.Fits(p.nodes[id].Available()) {
			return false
		}
	}
	// 全部可行后才真正占用。
	for id, total := range want {
		if !p.nodes[id].allocate(total) {
			// 理论不可达：上面已校验；返回失败保护账本一致。
			return false
		}
	}
	return true
}

// Release 归还某节点上的资源（任务正常结束时调用）。
func (p *Pool) Release(nodeID string, r api.Resource) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	n, ok := p.nodes[nodeID]
	if !ok {
		return false
	}
	return n.release(r)
}
