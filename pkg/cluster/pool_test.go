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

// TestCordonDrainMigrateRetire 覆盖计划内维护的节点账本生命周期：
// Cordon 后不接新任务 -> MigrateOne 原子迁移 -> RetireIfEmpty 退役。
func TestCordonDrainMigrateRetire(t *testing.T) {
	p := NewPool()
	if err := p.AddNode("n1", api.Resource{CPU: 4, Memory: 4}); err != nil {
		t.Fatal(err)
	}
	if err := p.AddNode("n2", api.Resource{CPU: 4, Memory: 4}); err != nil {
		t.Fatal(err)
	}

	// n1 放一个 3cpu 任务。
	if !p.TryCommit([]Placement{{TaskName: "a", NodeID: "n1", Request: api.Resource{CPU: 3, Memory: 1}}}) {
		t.Fatal("commit a")
	}
	if err := p.CordonNode("n1"); err != nil {
		t.Fatalf("cordon: %v", err)
	}
	// 排空中节点不接收新任务：Plan 不会选它。
	if target := p.PlanTarget(api.Resource{CPU: 2, Memory: 1}, ""); target != "n2" {
		t.Fatalf("new task must avoid draining n1, got target=%q", target)
	}
	// 重复 Cordon 报错。
	if err := p.CordonNode("n1"); err == nil {
		t.Fatal("repeated cordon must fail")
	}

	// 账本未空不能退役。
	if p.RetireIfEmpty("n1") {
		t.Fatal("draining node with usage must not retire")
	}

	// 迁移到 n2：原子成功，账本守恒（3 从 n1 移到 n2）。
	if !p.MigrateOne("n1", "n2", api.Resource{CPU: 3, Memory: 1}) {
		t.Fatal("migration should succeed")
	}
	if n, _ := p.Node("n1"); n.Snapshot().Used.CPU != 0 {
		t.Fatalf("source must be released, used=%d", n.Snapshot().Used.CPU)
	}
	if n, _ := p.Node("n2"); n.Snapshot().Used.CPU != 3 {
		t.Fatalf("destination must hold migrated amount, used=%d", n.Snapshot().Used.CPU)
	}
	// 迁空后退役。
	if !p.RetireIfEmpty("n1") {
		t.Fatal("emptied draining node must retire")
	}
	if n, _ := p.Node("n1"); n.Snapshot().State != api.NodeRetired {
		t.Fatalf("n1 must be retired, got %s", n.Snapshot().State)
	}
	// 退役节点既不能再排空，也不会被 PlanTarget 选中。
	if err := p.CordonNode("n1"); err == nil {
		t.Fatal("retired node cannot be cordoned")
	}
	if target := p.PlanTarget(api.Resource{CPU: 1, Memory: 1}, ""); target != "n2" {
		t.Fatalf("only n2 remains eligible, got %q", target)
	}
}

// TestMigrateFailsAtomically 目标放不下时迁移整体不发生，两边账本不变。
func TestMigrateFailsAtomically(t *testing.T) {
	p := NewPool()
	if err := p.AddNode("n1", api.Resource{CPU: 4, Memory: 4}); err != nil {
		t.Fatal(err)
	}
	if err := p.AddNode("n2", api.Resource{CPU: 4, Memory: 4}); err != nil {
		t.Fatal(err)
	}
	if !p.TryCommit([]Placement{{TaskName: "a", NodeID: "n1", Request: api.Resource{CPU: 4, Memory: 1}}}) {
		t.Fatal("commit a")
	}
	// n2 先占满 4cpu。
	if !p.TryCommit([]Placement{{TaskName: "b", NodeID: "n2", Request: api.Resource{CPU: 4, Memory: 1}}}) {
		t.Fatal("commit b")
	}
	if err := p.CordonNode("n1"); err != nil {
		t.Fatal(err)
	}
	// n1 上的 4cpu 想迁到已满的 n2：失败且账本零变化。
	if p.MigrateOne("n1", "n2", api.Resource{CPU: 4, Memory: 1}) {
		t.Fatal("migration to full node must fail")
	}
	if n, _ := p.Node("n1"); n.Snapshot().Used.CPU != 4 {
		t.Fatalf("failed migration must leave source untouched, used=%d", n.Snapshot().Used.CPU)
	}
	if n, _ := p.Node("n2"); n.Snapshot().Used.CPU != 4 {
		t.Fatalf("failed migration must leave destination untouched, used=%d", n.Snapshot().Used.CPU)
	}
	// 迁不出去时节点保持 Draining，不能退役。
	if p.RetireIfEmpty("n1") {
		t.Fatal("non-empty node must not retire")
	}
}

// TestCordonUnknownOrDeadNode 对不存在或已死亡节点排空应失败。
func TestCordonUnknownOrDeadNode(t *testing.T) {
	p := NewPool()
	if err := p.AddNode("n1", api.Resource{CPU: 4, Memory: 4}); err != nil {
		t.Fatal(err)
	}
	if err := p.CordonNode("ghost"); err == nil {
		t.Fatal("cordon unknown node must fail")
	}
	p.BeatAll(1, map[string]bool{"n1": true})
	if dead := p.DetectDead(4, 2); len(dead) != 1 {
		t.Fatal("n1 should be dead")
	}
	if err := p.CordonNode("n1"); err == nil {
		t.Fatal("cordon dead node must fail")
	}
}

// TestRestoreNodeState 重启恢复时可重放节点生命周期状态。
func TestRestoreNodeState(t *testing.T) {
	p := NewPool()
	if err := p.AddNode("n1", api.Resource{CPU: 4, Memory: 4}); err != nil {
		t.Fatal(err)
	}
	if err := p.RestoreNodeState("n1", api.NodeDraining); err != nil {
		t.Fatalf("restore draining: %v", err)
	}
	if n, _ := p.Node("n1"); n.Snapshot().State != api.NodeDraining {
		t.Fatalf("state not restored: %s", n.Snapshot().State)
	}
	if err := p.RestoreNodeState("ghost", api.NodeRetired); err == nil {
		t.Fatal("restore missing node must fail")
	}
	if err := p.RestoreNodeState("n1", "Bogus"); err == nil {
		t.Fatal("restore unknown state must fail")
	}
}
