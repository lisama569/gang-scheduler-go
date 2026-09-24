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

// DetectDead 找出心跳超时的健康节点，标记死亡并回收其账本上的全部占用。
func (p *Pool) DetectDead(now, timeoutTicks int64) []DeadNode {
	p.mu.Lock()
	defer p.mu.Unlock()
	var dead []DeadNode
	for _, id := range p.order {
		n := p.nodes[id]
		snap := n.Snapshot()
		if n.isHealthy() && now-snap.LastBeat > timeoutTicks {
			released := snap.Used
			n.MarkDead()
			dead = append(dead, DeadNode{ID: id, Released: released})
		}
	}
	return dead
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
