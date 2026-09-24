package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"
)

func spec(id string, cpu, mem float64, d time.Duration) TaskSpec {
	return TaskSpec{ID: id, CPU: cpu, Memory: mem, Duration: d}
}

func TestSubmitRejectionsAreDistinguishable(t *testing.T) {
	vc := NewVirtualClock()
	s := newTestScheduler(t, NewMemoryStore(), vc, time.Second)
	if err := s.AddNode("n1", Resource{CPU: 4, Memory: 400}); err != nil {
		t.Fatal(err)
	}
	if err := s.AddNode("n2", Resource{CPU: 4, Memory: 400}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	cases := []struct {
		name string
		req  SubmitRequest
		want error
	}{
		{"empty job id", SubmitRequest{JobID: "", Tasks: []TaskSpec{spec("t", 1, 1, 0)}}, ErrInvalidJob},
		{"no tasks", SubmitRequest{JobID: "j", Priority: 1}, ErrInvalidJob},
		{"zero cpu", SubmitRequest{JobID: "j0", Tasks: []TaskSpec{spec("t", 0, 1, 0)}}, ErrInvalidJob},
		{"dup task id", SubmitRequest{JobID: "jd", Tasks: []TaskSpec{spec("t", 1, 1, 0), spec("t", 1, 1, 0)}}, ErrInvalidJob},
		{"task too large", SubmitRequest{JobID: "jbig", Tasks: []TaskSpec{spec("t", 9, 900, 0)}}, ErrTaskTooLarge},
		{"over cluster capacity", SubmitRequest{
			JobID: "jmany",
			Tasks: []TaskSpec{spec("a", 3, 300, 0), spec("b", 3, 300, 0), spec("c", 3, 300, 0)}},
			ErrInsufficientCapacity},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := s.Submit(ctx, tc.req)
			if !errors.Is(err, tc.want) {
				t.Fatalf("want %v, got %v", tc.want, err)
			}
			if tc.req.JobID != "" {
				if _, ok := s.JobState(tc.req.JobID); ok {
					t.Fatalf("rejected job %s must not exist", tc.req.JobID)
				}
			}
		})
	}

	// 一个合法作业先成功，再重复提交必须被拒绝为 DUPLICATE。
	if err := s.Submit(ctx, SubmitRequest{JobID: "dup", Tasks: []TaskSpec{spec("t", 1, 100, 0)}}); err != nil {
		t.Fatalf("first submit: %v", err)
	}
	err := s.Submit(ctx, SubmitRequest{JobID: "dup", Tasks: []TaskSpec{spec("t2", 1, 100, 0)}})
	if !errors.Is(err, ErrDuplicateJob) {
		t.Fatalf("duplicate submit want ErrDuplicateJob, got %v", err)
	}
}

func TestGangSchedulingAllOrNothing(t *testing.T) {
	vc := NewVirtualClock()
	s := newTestScheduler(t, NewMemoryStore(), vc, time.Hour)
	ctx := context.Background()
	// n1 与 n2 各 4 CPU；3 个任务各 2 CPU 必须同时启动。
	for _, id := range []string{"n1", "n2"} {
		if err := s.AddNode(id, Resource{CPU: 4, Memory: 400}); err != nil {
			t.Fatal(err)
		}
	}
	// holder 占满 n1（4 CPU）：此时 gang 放不下（n1 剩 0、n2 剩 4，仅够 2 个任务）。
	if err := s.Submit(ctx, SubmitRequest{
		JobID: "holder", Priority: 1,
		Tasks: []TaskSpec{spec("h0", 4, 400, 100*time.Second)},
	}); err != nil {
		t.Fatal(err)
	}
	gang := SubmitRequest{
		JobID: "gang", Priority: 5,
		Tasks: []TaskSpec{spec("g0", 2, 200, time.Second), spec("g1", 2, 200, time.Second), spec("g2", 2, 200, time.Second)},
	}
	if err := s.Submit(ctx, gang); err != nil {
		t.Fatalf("gang submit should be queued, got %v", err)
	}
	s.Tick(ctx)
	for _, id := range []string{"g0", "g1", "g2"} {
		st, _, _ := s.TaskState("gang", id)
		if st != TaskPending {
			t.Fatalf("task %s must stay PENDING until whole gang fits, got %s", id, st)
		}
	}
	if st, _ := s.JobState("gang"); st != JobPending {
		t.Fatalf("gang job must stay PENDING, got %s", st)
	}

	// holder 完成释放 n1 的 4 CPU，下一个 tick gang 必须整体启动。
	vc.Advance(102 * time.Second)
	s.Tick(ctx)
	nodes := map[string]int{}
	for _, id := range []string{"g0", "g1", "g2"} {
		st, node, _ := s.TaskState("gang", id)
		if st != TaskRunning {
			t.Fatalf("task %s want RUNNING, got %s", id, st)
		}
		nodes[node]++
	}
	if len(nodes) != 2 {
		t.Fatalf("gang should span 2 nodes, got %v", nodes)
	}
	assertResourceInvariant(t, s)

	// 全部任务完成后作业 SUCCEEDED，且状态与任务结果一致。
	vc.Advance(2 * time.Second)
	s.Tick(ctx)
	if st, _ := s.JobState("gang"); st != JobSucceeded {
		t.Fatalf("gang want SUCCEEDED, got %s", st)
	}
	assertResourceInvariant(t, s)
	assertJobTasksConsistent(t, s)
}

func TestSchedulingPriorityAndFIFO(t *testing.T) {
	vc := NewVirtualClock()
	s := newTestScheduler(t, NewMemoryStore(), vc, time.Hour)
	ctx := context.Background()
	// 单节点 3 CPU：一次只能跑一个 2 CPU 的作业。
	if err := s.AddNode("n1", Resource{CPU: 3, Memory: 300}); err != nil {
		t.Fatal(err)
	}
	// 同一批次提交：低优先级 low 序号在前，但高优先级 high 必须先被选中。
	if err := s.SubmitBatch(ctx,
		SubmitRequest{JobID: "low", Priority: 1, Tasks: []TaskSpec{spec("l", 2, 200, time.Second)}},
		SubmitRequest{JobID: "high", Priority: 10, Tasks: []TaskSpec{spec("h", 2, 200, time.Second)}},
	); err != nil {
		t.Fatal(err)
	}
	if st, _, _ := s.TaskState("high", "h"); st != TaskRunning {
		t.Fatalf("high priority must run first, got %s", st)
	}
	if st, _, _ := s.TaskState("low", "l"); st != TaskPending {
		t.Fatalf("low priority must wait, got %s", st)
	}

	assertResourceInvariant(t, s)
}

func TestSchedulingFIFOAmongEqualPriority(t *testing.T) {
	vc := NewVirtualClock()
	s := newTestScheduler(t, NewMemoryStore(), vc, time.Hour)
	ctx := context.Background()
	if err := s.AddNode("n1", Resource{CPU: 3, Memory: 300}); err != nil {
		t.Fatal(err)
	}
	// holder 先占 2 CPU，first/second 同优先级批量排队；holder 释放后按提交序号先 first。
	if err := s.Submit(ctx, SubmitRequest{
		JobID: "holder", Priority: 1,
		Tasks: []TaskSpec{spec("h", 2, 200, 10*time.Second)},
	}); err != nil {
		t.Fatal(err)
	}
	// 注意：earlier 先出现在批次中（seq 更小），later 后出现。
	if err := s.SubmitBatch(ctx,
		SubmitRequest{JobID: "earlier", Priority: 1, Tasks: []TaskSpec{spec("e", 2, 200, time.Second)}},
		SubmitRequest{JobID: "later", Priority: 1, Tasks: []TaskSpec{spec("l", 2, 200, time.Second)}},
	); err != nil {
		t.Fatal(err)
	}
	if st, _, _ := s.TaskState("earlier", "e"); st != TaskPending {
		t.Fatalf("earlier must wait behind holder, got %s", st)
	}
	vc.Advance(12 * time.Second)
	s.Tick(ctx) // holder 完成并在同一 tick 触发调度
	if st, _, _ := s.TaskState("earlier", "e"); st != TaskRunning {
		t.Fatalf("same-priority FIFO: earlier must run, got %s", st)
	}
	if st, _, _ := s.TaskState("later", "l"); st != TaskPending {
		t.Fatalf("same-priority FIFO: later must wait, got %s", st)
	}
	assertResourceInvariant(t, s)
}
