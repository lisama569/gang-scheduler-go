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

// TestPoolDrainExcludesPlacement 排空中节点不参与 Plan/TryCommit，
// 但心跳与超时检测仍把它当存活节点；退役后容量彻底消失。
func TestPoolDrainExcludesPlacement(t *testing.T) {
	p := NewPool()
	_ = p.AddNode("n1", api.Resource{CPU: 2, Memory: 2})
	_ = p.AddNode("n2", api.Resource{CPU: 2, Memory: 2})

	if err := p.BeginDrain("n2"); err != nil {
		t.Fatalf("begin drain: %v", err)
	}
	// 排空中节点不再接收新任务：两个任务只能挤 n1，整体放不下 -> nil。
	plan := p.Plan(
		map[string]api.Resource{"a": {CPU: 2, Memory: 2}, "b": {CPU: 2, Memory: 2}},
		[]string{"a", "b"},
	)
	if plan != nil {
		t.Fatalf("draining node must be excluded from plan: %+v", plan)
	}
	// 单任务可放 n1；提交后 n2 仍空。
	plan = p.Plan(map[string]api.Resource{"a": {CPU: 2, Memory: 2}}, []string{"a"})
	if len(plan) != 1 || plan[0].NodeID != "n1" {
		t.Fatalf("task must go to healthy n1: %+v", plan)
	}
	if !p.TryCommit(plan) {
		t.Fatal("commit to n1 should succeed")
	}

	// 排空中节点仍算存活：心跳刷新、不会被超时判死。
	p.BeatAll(10, map[string]bool{"n1": true, "n2": true})
	if dead := p.DetectDead(11, 2); len(dead) != 0 {
		t.Fatalf("draining node with fresh heartbeat must not be reclaimed: %+v", dead)
	}
	// 但排空中的节点若停止心跳（维护中发生故障），同样被超时回收。
	if dead := p.DetectDead(20, 2); len(dead) != 2 {
		t.Fatalf("stale draining node must be reclaimed like healthy ones: %+v", dead)
	}
}

// TestPoolCompleteDrain 退役语义：未排空/有占用/状态非法时返回可区分错误。
func TestPoolCompleteDrain(t *testing.T) {
	p := NewPool()
	_ = p.AddNode("n1", api.Resource{CPU: 2, Memory: 2})

	if err := p.BeginDrain("ghost"); err != api.ErrNodeNotFound {
		t.Fatalf("drain unknown node: %v", err)
	}
	if err := p.CompleteDrain("n1"); err != api.ErrNodeNotDraining {
		t.Fatalf("complete on healthy node: %v", err)
	}
	if err := p.BeginDrain("n1"); err != nil {
		t.Fatal(err)
	}
	if err := p.BeginDrain("n1"); err != api.ErrNodeNotHealthy {
		t.Fatalf("double drain: %v", err)
	}
	// 排空中的节点被故障回收后不能再“完成退役”。
	p.BeatAll(1, map[string]bool{"n1": true})
	p.DetectDead(10, 2) // n1 超时判死
	if err := p.CompleteDrain("n1"); err != api.ErrNodeNotDraining {
		t.Fatalf("complete on dead node: %v", err)
	}
}

// TestPoolCompleteDrainEmpty 空节点排空后可正常退役，状态转为 Dead。
func TestPoolCompleteDrainEmpty(t *testing.T) {
	p := NewPool()
	_ = p.AddNode("n1", api.Resource{CPU: 2, Memory: 2})
	if err := p.BeginDrain("n1"); err != nil {
		t.Fatal(err)
	}
	used, err := p.DrainUsed("n1")
	if err != nil || used != (api.Resource{}) {
		t.Fatalf("empty node used: %+v err=%v", used, err)
	}
	if err := p.CompleteDrain("n1"); err != nil {
		t.Fatalf("complete empty drain: %v", err)
	}
	if got := p.Snapshot()[0].State; got != api.NodeDead {
		t.Fatalf("retired node state=%s want Dead", got)
	}
	if _, err := p.DrainUsed("ghost"); err != api.ErrNodeNotFound {
		t.Fatalf("drain used on unknown node: %v", err)
	}
}
