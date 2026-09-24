package cluster

import (
	"testing"

	"github.com/lisama569/gang-scheduler-go/pkg/api"
)

func TestPoolPlanAllOrNothing(t *testing.T) {
	p := NewPool()
	if err := p.AddNode("n1", api.Resource{CPU: 2, Memory: 2}); err != nil {
		t.Fatal(err)
	}
	if err := p.AddNode("n2", api.Resource{CPU: 2, Memory: 2}); err != nil {
		t.Fatal(err)
	}

	reqs := map[string]api.Resource{
		"a": {CPU: 2, Memory: 2},
		"b": {CPU: 2, Memory: 2},
	}
	plan := p.Plan(reqs, []string{"a", "b"})
	if len(plan) != 2 {
		t.Fatalf("both tasks should fit, got %d placements", len(plan))
	}
	// Plan 是纯计算：提交后账本不变。
	for _, n := range p.Snapshot() {
		if n.Used.CPU != 0 || n.Used.Memory != 0 {
			t.Fatalf("Plan must not mutate ledger: %+v", n)
		}
	}
	if !p.TryCommit(plan) {
		t.Fatal("commit should succeed")
	}

	// 两个节点已被各占满；再来两个任务必须整体放弃（gang），且不产生任何部分占用。
	plan2 := p.Plan(reqs, []string{"a", "b"})
	if plan2 != nil {
		t.Fatalf("gang plan must be nil when not all fit, got %+v", plan2)
	}
	nodes := p.Snapshot()
	for _, n := range nodes {
		avail := n.Capacity.Sub(n.Used)
		if avail.CPU != 0 || avail.Memory != 0 {
			t.Fatalf("nodes should remain fully used: %+v", n)
		}
	}
}

func TestPoolDeterministicOrder(t *testing.T) {
	// 无论注册顺序如何，放置决策都按节点 ID 排序：两次构造结果一致。
	build := func(first, second string) []Placement {
		p := NewPool()
		_ = p.AddNode(first, api.Resource{CPU: 4, Memory: 4})
		_ = p.AddNode(second, api.Resource{CPU: 4, Memory: 4})
		return p.Plan(
			map[string]api.Resource{"t": {CPU: 1, Memory: 1}},
			[]string{"t"},
		)
	}
	p1 := build("aaa", "bbb")
	p2 := build("bbb", "aaa")
	if p1[0].NodeID != "aaa" || p2[0].NodeID != "aaa" {
		t.Fatalf("placement must be deterministic by node id: %s %s", p1[0].NodeID, p2[0].NodeID)
	}
}

func TestPoolHeartbeatTimeout(t *testing.T) {
	p := NewPool()
	_ = p.AddNode("n1", api.Resource{CPU: 4, Memory: 4})

	// 占用资源后让心跳超时：MarkDead 必须回收账本。
	p.BeatAll(1, map[string]bool{"n1": true})
	plan := p.Plan(map[string]api.Resource{"t": {CPU: 3, Memory: 1}}, []string{"t"})
	if !p.TryCommit(plan) {
		t.Fatal("commit")
	}
	if got := p.DetectDead(3, 2); len(got) != 0 {
		t.Fatalf("heartbeat age 2 must not time out (>timeout required), got %d", len(got))
	}
	dead := p.DetectDead(4, 2)
	if len(dead) != 1 || dead[0].ID != "n1" {
		t.Fatalf("n1 must be detected dead at age 3, got %+v", dead)
	}
	if dead[0].Released.CPU != 3 {
		t.Fatalf("released resource must be reported, got %+v", dead[0].Released)
	}
	n, _ := p.Node("n1")
	if n.Snapshot().State != api.NodeDead || n.Snapshot().Capacity.Sub(n.Snapshot().Used).CPU != 4 {
		t.Fatalf("dead node state/ledger wrong: %+v", n.Snapshot())
	}
	// 迟到心跳不得复活节点。
	p.BeatAll(10, map[string]bool{"n1": true})
	if n.Snapshot().State != api.NodeDead {
		t.Fatal("late heartbeat must not resurrect a dead node")
	}
}
