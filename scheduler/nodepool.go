package scheduler

import (
	"sort"
	"time"
)

// nodePool 管理模拟节点及其资源账本。
type nodePool struct {
	nodes map[string]*Node
}

func newNodePool() *nodePool {
	return &nodePool{nodes: map[string]*Node{}}
}

func (p *nodePool) add(n *Node) {
	if n.runningTaskKeys == nil {
		n.runningTaskKeys = map[string]struct{}{}
	}
	p.nodes[n.ID] = n
}

func (p *nodePool) get(id string) (*Node, bool) {
	n, ok := p.nodes[id]
	return n, ok
}

// aliveNodes 返回按 ID 排序的存活节点，保证放置顺序确定。
func (p *nodePool) aliveNodes() []*Node {
	out := make([]*Node, 0, len(p.nodes))
	for _, n := range p.nodes {
		if n.Alive {
			out = append(out, n)
		}
	}
	sort.Slice(out, func(i, k int) bool { return out[i].ID < out[k].ID })
	return out
}

// totalAliveCapacity 汇总所有存活节点的容量（用于提交时的可行性判断）。
func (p *nodePool) totalAliveCapacity() Resource {
	var total Resource
	for _, n := range p.nodes {
		if n.Alive {
			total = total.Add(n.Capacity)
		}
	}
	return total
}

// available 返回节点当前剩余资源。
func (n *Node) available() Resource {
	return n.Capacity.Sub(n.Used)
}

// allocate 在节点账本上占用资源并登记运行任务。
func (n *Node) allocate(key string, req Resource) {
	n.Used = n.Used.Add(req)
	n.runningTaskKeys[key] = struct{}{}
}

// release 归还资源并移除运行任务登记。
func (n *Node) release(key string, req Resource) {
	if _, ok := n.runningTaskKeys[key]; !ok {
		return
	}
	delete(n.runningTaskKeys, key)
	n.Used = n.Used.Sub(req)
	if n.Used.CPU < -1e-9 || n.Used.Memory < -1e-9 {
		panic("node resource accounting went negative")
	}
}

var _ = time.Time{}
