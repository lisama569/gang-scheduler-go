package execution

import (
	"testing"

	"github.com/lisama569/gang-scheduler-go/pkg/api"
	"github.com/lisama569/gang-scheduler-go/pkg/cluster"
)

func newExec(t *testing.T) (*Executor, *cluster.Pool, chan Event) {
	t.Helper()
	pool := cluster.NewPool()
	if err := pool.AddNode("n1", api.Resource{CPU: 4, Memory: 4}); err != nil {
		t.Fatal(err)
	}
	events := make(chan Event, 16)
	return New(pool, events), pool, events
}

func rt(name string, dur, cpu int) api.TaskRuntime {
	return api.TaskRuntime{
		Spec:   api.TaskSpec{Name: name, Duration: dur, Resources: api.Resource{CPU: cpu, Memory: 1}},
		State:  api.TaskRunning,
		NodeID: "n1",
	}
}

func TestLaunchDuplicateRejected(t *testing.T) {
	e, _, _ := newExec(t)
	if !e.Launch("j", rt("a", 2, 1)) {
		t.Fatal("first launch must succeed")
	}
	if e.Launch("j", rt("a", 2, 1)) {
		t.Fatal("duplicate launch of same task must be rejected")
	}
	if e.Inflight() != 1 {
		t.Fatalf("inflight must be 1, got %d", e.Inflight())
	}
}

func TestTickFinishesAndReleases(t *testing.T) {
	e, pool, events := newExec(t)
	// 账本占用由调度层 TryCommit 完成，执行器只在结束时归还。
	if !pool.TryCommit([]cluster.Placement{{
		TaskName: "a", NodeID: "n1", Request: api.Resource{CPU: 2, Memory: 1},
	}}) {
		t.Fatal("commit")
	}
	_ = e.Launch("j", rt("a", 2, 2))

	// 启动后账本占用 2cpu。
	if n, _ := pool.Node("n1"); n.Snapshot().Used.CPU != 2 {
		t.Fatalf("used should be 2 after launch, got %d", n.Snapshot().Used.CPU)
	}
	e.Tick() // 剩 1
	if e.Inflight() != 1 {
		t.Fatal("task still running after first tick")
	}
	e.Tick() // 完成
	ev := <-events
	if ev.Kind != EvFinished || !ev.OK || ev.TaskName != "a" {
		t.Fatalf("unexpected event: %+v", ev)
	}
	if n, _ := pool.Node("n1"); n.Snapshot().Used.CPU != 0 {
		t.Fatalf("resources must be released after finish, got %d", n.Snapshot().Used.CPU)
	}
}

func TestKillOnNodeOnlyRunning(t *testing.T) {
	e, pool, events := newExec(t)
	if !pool.TryCommit([]cluster.Placement{
		{TaskName: "a", NodeID: "n1", Request: api.Resource{CPU: 1, Memory: 1}},
		{TaskName: "b", NodeID: "n1", Request: api.Resource{CPU: 1, Memory: 1}},
	}) {
		t.Fatal("commit")
	}
	_ = e.Launch("j", rt("a", 5, 1))
	_ = e.Launch("k", rt("b", 5, 1)) // 不同作业同名任务，键不冲突
	e.KillOnNode("n1")
	if e.Inflight() != 0 {
		t.Fatalf("all inflight on dead node must be killed, got %d", e.Inflight())
	}
	if n, _ := pool.Node("n1"); n.Snapshot().Used.CPU != 0 {
		t.Fatalf("killing node must release its ledger, used=%d", n.Snapshot().Used.CPU)
	}
	got := 0
	for ev := range events {
		if ev.Kind != EvKilled {
			t.Fatalf("expected kill event, got %+v", ev)
		}
		got++
		if got == 2 {
			break
		}
	}
	// 再次 kill 一个已无任务的节点是安全的空操作。
	e.KillOnNode("n1")
}

func TestInjectFailure(t *testing.T) {
	e, _, events := newExec(t)
	if e.InjectFailure("j", "ghost") {
		t.Fatal("injecting failure into unknown task must fail")
	}
	_ = e.Launch("j", rt("a", 5, 1))
	if !e.InjectFailure("j", "a") {
		t.Fatal("inject failure into running task must succeed")
	}
	ev := <-events
	if ev.Kind != EvFinished || ev.OK {
		t.Fatalf("expected failure finish event, got %+v", ev)
	}
	if e.InjectFailure("j", "a") {
		t.Fatal("second injection after finish must be rejected (idempotent)")
	}
}

// TestMigrateKeepsRemainingTicks 计划内维护的在线迁移只改在执行行归属，
// 保留剩余节拍；迁到错误源节点或不存在任务时失败。
func TestMigrateKeepsRemainingTicks(t *testing.T) {
	e, pool, events := newExec(t)
	if err := pool.AddNode("n2", api.Resource{CPU: 4, Memory: 4}); err != nil {
		t.Fatal(err)
	}
	// a 跑在 n1（占用 1cpu）。
	if !pool.TryCommit([]cluster.Placement{
		{TaskName: "a", NodeID: "n1", Request: api.Resource{CPU: 1, Memory: 1}},
	}) {
		t.Fatal("commit")
	}
	_ = e.Launch("j", rt("a", 3, 1))

	// 源节点不符时迁移失败。
	if e.Migrate("j", "a", "n2", "n1") {
		t.Fatal("migrate from a node that does not host the task must fail")
	}
	// n1 进入计划内排空；账本层先迁移（n1 -> n2），再让执行行改挂。
	if err := pool.CordonNode("n1"); err != nil {
		t.Fatal(err)
	}
	if !pool.MigrateOne("n1", "n2", api.Resource{CPU: 1, Memory: 1}) {
		t.Fatal("ledger migration should succeed")
	}
	if !e.Migrate("j", "a", "n1", "n2") {
		t.Fatal("live migrate should succeed")
	}

	// 再推进 3 拍：迁移保留剩余节拍，任务应在第 3 拍于 n2 上正常结束。
	for i := 0; i < 2; i++ {
		e.Tick()
		select {
		case ev := <-events:
			t.Fatalf("task must not finish before its remaining ticks, got %+v", ev)
		default:
		}
	}
	e.Tick()
	ev := <-events
	if ev.Kind != EvFinished || ev.NodeID != "n2" || !ev.OK {
		t.Fatalf("task should finish on n2 after preserved ticks: %+v", ev)
	}
	// 完成后资源在 n2 上释放，n1 始终为空。
	if n, _ := pool.Node("n1"); n.Snapshot().Used.CPU != 0 {
		t.Fatalf("n1 should be empty, used=%d", n.Snapshot().Used.CPU)
	}
	if n, _ := pool.Node("n2"); n.Snapshot().Used.CPU != 0 {
		t.Fatalf("n2 should release after finish, used=%d", n.Snapshot().Used.CPU)
	}
}
