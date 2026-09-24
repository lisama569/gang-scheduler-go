package scheduler

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/lisama569/gang-scheduler-go/pkg/api"
)

// TestConcurrentSubmitSameID 多协程并发提交同一作业 ID：
// 恰好一个成功，其余全部以 ErrDuplicateJob 被拒绝，存储中只有一份作业。
func TestConcurrentSubmitSameID(t *testing.T) {
	s := newTestScheduler(t)
	const n = 32
	var wg sync.WaitGroup
	var accepted, dup, other int64
	spec := api.JobSpec{ID: "race-job", Tasks: []api.TaskSpec{
		{Name: "t", Duration: 2, Resources: api.Resource{CPU: 1, Memory: 1}},
	}}
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			err := s.Submit(spec)
			switch {
			case err == nil:
				atomic.AddInt64(&accepted, 1)
			case errors.Is(err, api.ErrDuplicateJob):
				atomic.AddInt64(&dup, 1)
			default:
				atomic.AddInt64(&other, 1)
				t.Errorf("unexpected submit error: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()

	if accepted != 1 || dup != n-1 || other != 0 {
		t.Fatalf("accepted=%d dup=%d other=%d (want accepted=1 dup=%d)",
			accepted, dup, other, n-1)
	}
	if len(s.store.List()) != 1 {
		t.Fatalf("exactly one job must exist, got %d", len(s.store.List()))
	}
	// 作业可正常跑到终态，账本清零。
	s.Tick()
	s.Tick()
	j, _ := s.GetJob("race-job")
	if j.State != api.JobSucceeded {
		t.Fatalf("race job should succeed, got %s", j.State)
	}
	t.Logf("concurrent duplicate submit: exactly 1 accepted out of %d, job succeeded", n)
}

// TestConcurrentTicksAndSubmit 调度循环与大量提交并发执行，
// 最终所有被接受作业状态与节点账本保持一致（-race 下验证）。
func TestConcurrentTicksAndSubmit(t *testing.T) {
	s := newTestScheduler(t)
	if err := s.AddNode("n3", api.Resource{CPU: 4, Memory: 8}); err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	ticksDone := make(chan struct{})
	go func() {
		defer close(ticksDone)
		for {
			select {
			case <-stop:
				return
			default:
				s.Tick()
			}
		}
	}()

	var wg sync.WaitGroup
	var accepted int64
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := s.Submit(api.JobSpec{
				ID:       "cj-" + itoa(i),
				Priority: i % 5,
				Tasks: []api.TaskSpec{
					{Name: "a", Duration: 1 + i%3, Resources: api.Resource{CPU: 1, Memory: 1}},
					{Name: "b", Duration: 1 + i%3, Resources: api.Resource{CPU: 1, Memory: 1}},
				},
			})
			if err == nil {
				atomic.AddInt64(&accepted, 1)
			} else if !errors.Is(err, api.ErrInsufficientResources) {
				t.Errorf("job %d unexpected err: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	// 再跑一段时间排空队列，然后停止 tick 循环。
	for i := 0; i < 100; i++ {
		s.Tick()
	}
	close(stop)
	<-ticksDone

	for _, jb := range s.store.List() {
		if jb.State != api.JobSucceeded {
			t.Fatalf("job %s not succeeded: %s", jb.Spec.ID, jb.State)
		}
	}
	for _, n := range s.Nodes() {
		if n.State == api.NodeHealthy && (n.Used.CPU != 0 || n.Used.Memory != 0) {
			t.Fatalf("node %s leaked resources: %+v", n.ID, n.Used)
		}
	}
	t.Logf("accepted %d/40 jobs; all reached Succeeded with zero leaked resources", accepted)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [12]byte
	pos := len(buf)
	neg := i < 0
	if neg {
		i = -i
	}
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
