package scheduler

import (
	"context"
	"testing"
	"time"
)

// TestNodeFailureReschedulesUnfinishedTasks 心跳超时 -> 回收 -> 重调度；
// 已完成任务保持成功、不重复执行。
func TestNodeFailureReschedulesUnfinishedTasks(t *testing.T) {
	vc := NewVirtualClock()
	s := newTestScheduler(t, NewMemoryStore(), vc, 5*time.Second)
	ctx := context.Background()
	for _, id := range []string{"n1", "n2", "n3"} {
		if err := s.AddNode(id, Resource{CPU: 4, Memory: 400}); err != nil {
			t.Fatal(err)
		}
	}
	// job: 3 个任务，每个 4 CPU（各占一个节点），g0 很快完成，g1/g2 长时间运行。
	if err := s.Submit(ctx, SubmitRequest{
		JobID: "job", Priority: 1,
		Tasks: []TaskSpec{
			spec("g0", 4, 400, time.Second),
			spec("g1", 4, 400, 100*time.Second),
			spec("g2", 4, 400, 100*time.Second),
		},
	}); err != nil {
		t.Fatal(err)
	}
	taskNode := map[string]string{}
	for _, id := range []string{"g0", "g1", "g2"} {
		st, node, ok := s.TaskState("job", id)
		if !ok || st != TaskRunning {
			t.Fatalf("task %s want RUNNING, got %s", id, st)
		}
		taskNode[id] = node
	}
	if taskNode["g0"] == taskNode["g1"] || taskNode["g1"] == taskNode["g2"] || taskNode["g0"] == taskNode["g2"] {
		t.Fatalf("tasks must be on distinct nodes, got %v", taskNode)
	}

	// g0 在其节点上先完成。
	vc.Advance(2 * time.Second)
	s.Heartbeat(taskNode["g0"], vc.Now())
	s.Heartbeat(taskNode["g1"], vc.Now())
	s.Heartbeat(taskNode["g2"], vc.Now())
	s.Tick(ctx)
	if st, _, _ := s.TaskState("job", "g0"); st != TaskSucceeded {
		t.Fatalf("g0 must be SUCCEEDED, got %s", st)
	}
	if got := s.Executions("job", "g0"); got != 1 {
		t.Fatalf("g0 executed %d times, want 1", got)
	}

	// g1 所在节点失联：其它节点正常心跳。
	dead := taskNode["g1"]
	vc.Advance(2 * time.Second)
	s.Heartbeat(taskNode["g0"], vc.Now())
	s.Heartbeat(taskNode["g2"], vc.Now())
	// 不向 dead 发心跳；推进超过超时阈值。
	vc.Advance(4 * time.Second)
	s.Tick(ctx)

	st, node, _ := s.TaskState("job", "g1")
	if st != TaskRunning {
		t.Fatalf("g1 must be RUNNING again after reschedule, got %s", st)
	}
	if node == dead {
		t.Fatalf("g1 must not be rescheduled onto dead node %s", dead)
	}
	// 死节点账本必须为空。
	s.mu.Lock()
	dn, _ := s.nodes.get(dead)
	if dn.Alive || dn.Used != (Resource{}) || len(dn.runningTaskKeys) != 0 {
		t.Fatalf("dead node not reclaimed: alive=%v used=%v tasks=%d",
			dn.Alive, dn.Used, len(dn.runningTaskKeys))
	}
	s.mu.Unlock()

	// g0 仍是 SUCCEEDED 且只执行过一次——成功结果不丢、不重复。
	if st, _, _ := s.TaskState("job", "g0"); st != TaskSucceeded {
		t.Fatalf("completed g0 changed state after failure: %s", st)
	}
	if got := s.Executions("job", "g0"); got != 1 {
		t.Fatalf("completed task g0 executed %d times, must remain 1", got)
	}

	// g1 重调度后运行至完成，作业整体 SUCCEEDED 且与任务结果一致。
	vc.Advance(101 * time.Second)
	for _, id := range []string{"n1", "n2", "n3"} {
		_ = s.Heartbeat(id, vc.Now()) // 重新上线的节点心跳也无妨
	}
	s.Tick(ctx)
	if jst, _ := s.JobState("job"); jst != JobSucceeded {
		t.Fatalf("job final state = %s, want SUCCEEDED", jst)
	}
	for _, id := range []string{"g0", "g1", "g2"} {
		if st, _, _ := s.TaskState("job", id); st != TaskSucceeded {
			t.Fatalf("task %s final = %s, want SUCCEEDED", id, st)
		}
	}
	assertResourceInvariant(t, s)
	assertJobTasksConsistent(t, s)
}

// TestPartialGangFailureKeepsSucceededResults 一个 gang 中部分任务已完成、
// 部分随节点丢失：已完成的不重跑，剩余任务再次整体放置。
func TestPartialGangFailureKeepsSucceededResults(t *testing.T) {
	vc := NewVirtualClock()
	s := newTestScheduler(t, NewMemoryStore(), vc, 5*time.Second)
	ctx := context.Background()
	if err := s.AddNode("n1", Resource{CPU: 8, Memory: 800}); err != nil {
		t.Fatal(err)
	}
	if err := s.AddNode("n2", Resource{CPU: 8, Memory: 800}); err != nil {
		t.Fatal(err)
	}
	// 两个 6 CPU 任务：n1/n2 各一个。a 先完成，b 所在节点随后故障；
	// 单节点放不下“a 重跑 + b”，但 a 已成功，因此只剩 b 时可整体重放置。
	if err := s.Submit(ctx, SubmitRequest{
		JobID: "gj", Priority: 1,
		Tasks: []TaskSpec{
			spec("a", 6, 600, time.Second),
			spec("b", 6, 600, 100*time.Second),
		},
	}); err != nil {
		t.Fatal(err)
	}
	_, na, _ := s.TaskState("gj", "a")
	_, nb, _ := s.TaskState("gj", "b")
	if na == nb {
		t.Fatalf("a and b should land on different nodes, both on %s", na)
	}
	vc.Advance(2 * time.Second)
	s.Heartbeat(na, vc.Now())
	s.Heartbeat(nb, vc.Now())
	s.Tick(ctx) // a 完成
	if st, _, _ := s.TaskState("gj", "a"); st != TaskSucceeded {
		t.Fatalf("a should succeed early, got %s", st)
	}
	// nb 失联。
	vc.Advance(6 * time.Second)
	s.Heartbeat(na, vc.Now())
	s.Tick(ctx)
	st, node, _ := s.TaskState("gj", "b")
	if st != TaskRunning || node == nb {
		t.Fatalf("b must be rescheduled to a live node, got state=%s node=%s (dead=%s)", st, node, nb)
	}
	if st, _, _ := s.TaskState("gj", "a"); st != TaskSucceeded {
		t.Fatalf("a must remain SUCCEEDED, got %s", st)
	}
	if got := s.Executions("gj", "a"); got != 1 {
		t.Fatalf("a executions=%d, must stay 1 (no double run)", got)
	}
	vc.Advance(101 * time.Second)
	s.Heartbeat(na, vc.Now())
	s.Heartbeat(node, vc.Now())
	s.Tick(ctx)
	if jst, _ := s.JobState("gj"); jst != JobSucceeded {
		t.Fatalf("gj = %s, want SUCCEEDED", jst)
	}
	assertResourceInvariant(t, s)
	assertJobTasksConsistent(t, s)
}
