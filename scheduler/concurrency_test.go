package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestConcurrentDuplicateSubmit 多个 goroutine 同时提交同一作业 ID：
// 恰好一个成功，其余全部得到 ErrDuplicateJob。
func TestConcurrentDuplicateSubmit(t *testing.T) {
	vc := NewVirtualClock()
	s := newTestScheduler(t, NewMemoryStore(), vc, time.Hour)
	if err := s.AddNode("n1", Resource{CPU: 64, Memory: 6400}); err != nil {
		t.Fatal(err)
	}
	const n = 32
	var wg sync.WaitGroup
	var okCount, dupCount, otherCount int64
	var mu sync.Mutex
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			err := s.Submit(context.Background(), SubmitRequest{
				JobID: "race",
				Tasks: []TaskSpec{spec("t", 1, 100, time.Second)},
			})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				okCount++
			case errors.Is(err, ErrDuplicateJob):
				dupCount++
			default:
				otherCount++
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()
	if okCount != 1 || dupCount != n-1 || otherCount != 0 {
		t.Fatalf("ok=%d dup=%d other=%d", okCount, dupCount, otherCount)
	}
	if st, ok := s.JobState("race"); !ok || st == "" {
		t.Fatalf("exactly one job must exist")
	}
	assertResourceInvariant(t, s)
}

// TestConcurrentDistinctSubmitNoDoubleOrLost 大量不同作业并发提交：
// 每个作业要么以唯一身份运行/排队，要么因容量被明确拒绝；
// 不允许重复分配资源或作业丢失。
func TestConcurrentDistinctSubmitNoDoubleOrLost(t *testing.T) {
	vc := NewVirtualClock()
	s := newTestScheduler(t, NewMemoryStore(), vc, 100000*time.Hour)
	if err := s.AddNode("n1", Resource{CPU: 64, Memory: 6400}); err != nil {
		t.Fatal(err)
	}
	const n = 100
	var wg sync.WaitGroup
	var accepted, rejected int64
	var mu sync.Mutex
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			err := s.Submit(context.Background(), SubmitRequest{
				JobID:    fmt.Sprintf("job-%03d", i),
				Priority: i % 5,
				Tasks:    []TaskSpec{spec("t", 1, 100, time.Hour)},
			})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				accepted++
			case errors.Is(err, ErrInsufficientCapacity):
				rejected++ // 单节点只有 64 CPU，其余排队（不会被拒，因为总量判断基于总量）
			default:
				t.Errorf("job %d unexpected err: %v", i, err)
			}
		}()
	}
	wg.Wait()
	if accepted != n {
		t.Fatalf("accepted=%d rejected=%d, total cluster capacity is static so all should be accepted and queued",
			accepted, rejected)
	}
	// 64 个运行中（占满 n1），其余 36 个排队等待。
	s.mu.Lock()
	running := 0
	pending := 0
	for _, j := range s.jobs {
		for _, task := range j.Tasks {
			switch task.State {
			case TaskRunning:
				running++
			case TaskPending:
				pending++
			}
		}
	}
	used := s.nodes.nodes["n1"].Used
	queued := s.queue.len()
	s.mu.Unlock()
	if running != 64 || pending != 36 {
		t.Fatalf("running=%d pending=%d, want 64/36", running, pending)
	}
	if used != (Resource{CPU: 64, Memory: 6400}) {
		t.Fatalf("node used=%v, want full capacity", used)
	}
	if queued != 36 {
		t.Fatalf("queued=%d, want 36", queued)
	}
	assertResourceInvariant(t, s)

	// 推进时间让全部任务完成：最终没有作业丢失，全部 SUCCEEDED。
	// 滚动推进直到全部完成（每批任务 StartedAt 不同，需要多次 Tick 收敛）。
	for k := 0; k < n+2; k++ {
		vc.Advance(2 * time.Hour)
		s.Tick(context.Background())
	}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("job-%03d", i)
		if st, ok := s.JobState(id); !ok || st != JobSucceeded {
			t.Fatalf("job %s state=%s ok=%v, want SUCCEEDED", id, st, ok)
		}
	}
	assertResourceInvariant(t, s)
	assertJobTasksConsistent(t, s)
}

// TestRecoveryConcurrentWithTick 恢复与后台 Tick/提交并发进行：
// 在 Recover 返回之前，Tick 与 Submit 必须等待，恢复完成后状态一致，
// 不允许资源重复占用或作业丢失。
func TestRecoveryConcurrentWithTick(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	vc := NewVirtualClock()

	// 先制造一个“上一进程”留下的快照：1 个未完成作业正在运行。
	old := New(store, WithLogger(newTestLogger(t)), WithClock(vc), WithHeartbeatTimeout(time.Hour))
	if err := old.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if err := old.AddNode("n1", Resource{CPU: 8, Memory: 800}); err != nil {
		t.Fatal(err)
	}
	if err := old.Submit(ctx, SubmitRequest{
		JobID: "oldjob",
		Tasks: []TaskSpec{spec("a", 2, 200, 100*time.Second)},
	}); err != nil {
		t.Fatal(err)
	}

	// 新进程：Recover 尚未完成时就并发发起 Tick / Submit / AddNode，
	// 它们都应阻塞到 Recover 完成，之后统一在一致状态上执行。
	fresh := NewVirtualClock()
	s := New(store, WithLogger(newTestLogger(t)), WithClock(fresh), WithHeartbeatTimeout(time.Hour))

	var wg sync.WaitGroup
	start := make(chan struct{})
	for k := 0; k < 8; k++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			s.Tick(ctx)
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		_ = s.Submit(ctx, SubmitRequest{
			JobID: "newjob",
			Tasks: []TaskSpec{spec("b", 1, 100, time.Second)},
		}) // 恢复后节点尚待重注册，可能因集群容量为 0 被拒，两种结果都可接受；
		// 关键是它必须在 Recover 之后执行而不是读到半初始化状态。
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		_ = s.AddNode("n1", Resource{CPU: 8, Memory: 800})
	}()
	close(start)
	time.Sleep(20 * time.Millisecond) // 让各 goroutine 阻塞在 waitReady
	if err := s.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	// oldjob 必须存在且来自快照（恢复没有丢作业）；无重复占用。
	if st, ok := s.JobState("oldjob"); !ok {
		t.Fatalf("oldjob lost during concurrent recovery")
	} else if st != JobPending && st != JobRunning {
		t.Fatalf("oldjob unexpected state %s", st)
	}
	assertResourceInvariant(t, s)
}
