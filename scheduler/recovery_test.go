package scheduler

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func newFileTestScheduler(t *testing.T, vc *VirtualClock, hb time.Duration) (*Scheduler, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.json")
	s := New(NewFileStore(path),
		WithLogger(newTestLogger(t)), WithClock(vc), WithHeartbeatTimeout(hb))
	if err := s.Recover(context.Background()); err != nil {
		t.Fatalf("recover: %v", err)
	}
	return s, path
}

// TestRestartRecoveryContinuesUnfinishedAndDoesNotReviveFinished 验证：
// 已结束作业保持终态不复活；未完成任务在新实例里继续调度至完成；
// 恢复后资源账本无重复占用。
func TestRestartRecoveryContinuesUnfinishedAndDoesNotReviveFinished(t *testing.T) {
	ctx := context.Background()
	vc := NewVirtualClock()
	s1, path := newFileTestScheduler(t, vc, time.Hour)
	for _, id := range []string{"n1", "n2"} {
		if err := s1.AddNode(id, Resource{CPU: 4, Memory: 400}); err != nil {
			t.Fatal(err)
		}
	}
	// done：短任务，崩溃前完成；long：两任务长耗时，崩溃时仍在运行。
	if err := s1.Submit(ctx, SubmitRequest{
		JobID: "done", Priority: 1,
		Tasks: []TaskSpec{spec("d0", 1, 100, time.Second)},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s1.Submit(ctx, SubmitRequest{
		JobID: "long", Priority: 1,
		Tasks: []TaskSpec{
			spec("l0", 3, 300, 100*time.Second),
			spec("l1", 3, 300, 100*time.Second),
		},
	}); err != nil {
		t.Fatal(err)
	}
	vc.Advance(2 * time.Second)
	s1.Tick(ctx)
	if st, _ := s1.JobState("done"); st != JobSucceeded {
		t.Fatalf("done should finish before crash, got %s", st)
	}
	if st, _ := s1.JobState("long"); st != JobRunning {
		t.Fatalf("long should be RUNNING at crash, got %s", st)
	}

	// 模拟新进程：全新实例 + 同一文件存储，恢复。
	vc2 := NewVirtualClock()
	vc2.Advance(20 * time.Second) // 时钟可以“跳变”，恢复不依赖旧时钟
	s2 := New(NewFileStore(path),
		WithLogger(newTestLogger(t)), WithClock(vc2), WithHeartbeatTimeout(time.Hour))
	if err := s2.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if st, ok := s2.JobState("done"); !ok || st != JobSucceeded {
		t.Fatalf("finished job after recover: state=%s ok=%v, want SUCCEEDED", st, ok)
	}
	// long 的两个任务必须是 PENDING（崩溃前 RUNNING 全部回退），节点等待重注册。
	for _, id := range []string{"l0", "l1"} {
		st, node, _ := s2.TaskState("long", id)
		if st != TaskPending || node != "" {
			t.Fatalf("task %s after recover: state=%s node=%q, want PENDING on no node", id, st, node)
		}
	}
	// 节点重注册（模拟集群重新上报）后，未完成作业继续调度。
	for _, id := range []string{"n1", "n2"} {
		if err := s2.AddNode(id, Resource{CPU: 4, Memory: 400}); err != nil {
			t.Fatal(err)
		}
	}
	if st, _ := s2.JobState("long"); st != JobRunning {
		t.Fatalf("long should be RUNNING after node re-registration, got %s", st)
	}
	assertResourceInvariant(t, s2)
	// 已结束作业绝不能被重新调度：done/d0 不应再次出现在任何节点账本上。
	if got := s2.Executions("done", "d0"); got != 1 {
		t.Fatalf("done/d0 executions after restart = %d, want 1", got)
	}

	vc2.Advance(101 * time.Second)
	s2.Tick(ctx)
	if st, _ := s2.JobState("long"); st != JobSucceeded {
		t.Fatalf("long final = %s, want SUCCEEDED", st)
	}
	if st, _ := s2.JobState("done"); st != JobSucceeded {
		t.Fatalf("done revived: state=%s", st)
	}
	assertResourceInvariant(t, s2)
	assertJobTasksConsistent(t, s2)

	// 再次重启：全部作业已结束，恢复后队列必须为空、任何 Tick 都不产生调度。
	s3 := New(NewFileStore(path),
		WithLogger(newTestLogger(t)), WithClock(vc2), WithHeartbeatTimeout(time.Hour))
	if err := s3.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	s3.Tick(ctx)
	for _, j := range []string{"done", "long"} {
		if st, _ := s3.JobState(j); st != JobSucceeded {
			t.Fatalf("job %s after second restart = %s", j, st)
		}
	}
}

// TestRecoveredStateIsPersistentAcrossStores 快照落盘内容可被再次读取，
// 且 MemoryStore 的保存具备隔离语义。
func TestSnapshotSaveLoadRoundTrip(t *testing.T) {
	store := NewMemoryStore()
	vc := NewVirtualClock()
	s := newTestScheduler(t, store, vc, time.Hour)
	ctx := context.Background()
	if err := s.AddNode("n1", Resource{CPU: 2, Memory: 200}); err != nil {
		t.Fatal(err)
	}
	if err := s.Submit(ctx, SubmitRequest{
		JobID: "j", Tasks: []TaskSpec{spec("a", 1, 100, time.Second)},
	}); err != nil {
		t.Fatal(err)
	}
	snap, ok, err := store.Load(ctx)
	if err != nil || !ok {
		t.Fatalf("snapshot load ok=%v err=%v", ok, err)
	}
	if len(snap.Jobs) != 1 || snap.Jobs[0].ID != "j" {
		t.Fatalf("unexpected snapshot jobs: %+v", snap.Jobs)
	}
	if len(snap.Nodes) != 1 || snap.Nodes[0].ID != "n1" {
		t.Fatalf("unexpected snapshot nodes: %+v", snap.Nodes)
	}
	// 改动内存中的作业不影响已保存快照（隔离性）。
	s.mu.Lock()
	s.jobs["j"].State = JobFailed
	s.mu.Unlock()
	snap2, _, _ := store.Load(ctx)
	if snap2.Jobs[0].State != JobRunning {
		t.Fatalf("snapshot must be an isolated copy, got %s", snap2.Jobs[0].State)
	}
}
