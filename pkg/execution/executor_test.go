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

// TestEvictOnNodeCarriesProgress 计划内排空驱逐必须携带剩余节拍，
// 任务用该剩余节拍重新 Launch 后接着原进度跑完，且不丢资源账。
func TestEvictOnNodeCarriesProgress(t *testing.T) {
	e, pool, events := newExec(t)
	if !pool.TryCommit([]cluster.Placement{{
		TaskName: "a", NodeID: "n1", Request: api.Resource{CPU: 2, Memory: 1},
	}}) {
		t.Fatal("commit")
	}
	tr := rt("a", 5, 2) // duration=5
	if !e.Launch("j", tr) {
		t.Fatal("launch")
	}
	e.Tick() // 跑 1 拍，剩 4
	e.Tick() // 跑 1 拍，剩 3

	e.EvictOnNode("n1") // 计划内驱逐（不是 Kill）
	var ev Event
	for ev = range events {
		break
	}
	if ev.Kind != EvDrained {
		t.Fatalf("want EvDrained, got %+v", ev)
	}
	if ev.Remaining != 3 {
		t.Fatalf("eviction must carry remaining=3 ticks, got %d", ev.Remaining)
	}
	if n, _ := pool.Node("n1"); n.Snapshot().Used != (api.Resource{}) {
		t.Fatalf("eviction must release ledger: %+v", n.Snapshot().Used)
	}
	if e.Inflight() != 0 {
		t.Fatal("evicted task must leave inflight set")
	}

	// 模拟迁移：节点资源在健康节点重新提交，任务按剩余 3 拍继续。
	if !pool.TryCommit([]cluster.Placement{{
		TaskName: "a", NodeID: "n1", Request: api.Resource{CPU: 2, Memory: 1},
	}}) {
		t.Fatal("re-commit")
	}
	migrated := tr
	migrated.Progress = 5 - ev.Remaining // Progress=2
	if !e.Launch("j", migrated) {
		t.Fatal("re-launch after migration")
	}
	e.Tick() // 剩 2
	e.Tick() // 剩 1
	if e.Inflight() != 1 {
		t.Fatal("migrated task should still be running")
	}
	e.Tick() // 剩 0 -> 完成：只需 3 拍而非完整 5 拍
	var fin Event
	for fin = range events {
		break
	}
	if fin.Kind != EvFinished || !fin.OK {
		t.Fatalf("migrated task must finish using carried progress: %+v", fin)
	}
}
