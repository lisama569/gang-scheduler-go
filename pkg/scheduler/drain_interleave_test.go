package scheduler

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/gang-scheduler-go/pkg/api"
)

// TestDrainInterleavedDeterministic 用确定性的交错序列验证“账一直对得上”：
// 多台节点先后排空（有的还在排空中）、作业陆续提交、调度持续推进、
// 中途进程重启恢复——任意交错顺序下：
//   - 不丢作业、作业不复活；
//   - 任务进度跨迁移/重启连续，Attempt 只被非计划中断（故障/崩溃）增加；
//   - 排空剩余量持续可观测，归零后才能退役；
//   - 资源账本不超发、不负数，终态全部归零；
//   - 退役节点重启后仍是退役，容量保持收缩。
func TestDrainInterleavedDeterministic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.json")

	s := New(Config{HeartbeatTimeoutTicks: 2, StateFile: path, Logger: testLogger(t)})
	for _, n := range []struct {
		id string
		c  api.Resource
	}{
		{"n1", api.Resource{CPU: 4, Memory: 4}},
		{"n2", api.Resource{CPU: 4, Memory: 4}},
		{"n3", api.Resource{CPU: 4, Memory: 4}},
		{"n4", api.Resource{CPU: 4, Memory: 4}},
	} {
		if err := s.AddNode(n.id, n.c); err != nil {
			t.Fatal(err)
		}
	}

	// jobA：两个长任务分别占 n1、n2。
	mustSubmit(t, s, api.JobSpec{ID: "jobA", Priority: 5, Tasks: []api.TaskSpec{
		{Name: "a1", Resources: api.Resource{CPU: 3, Memory: 1}, Duration: 14},
		{Name: "a2", Resources: api.Resource{CPU: 3, Memory: 1}, Duration: 14},
	}})
	s.Tick() // 1: a1->n1, a2->n2
	assertJobState(t, s, "jobA", api.JobRunning)
	assertLedger(t, s)

	// n2 先排空：a2 带着 progress=1 迁到 n3；n2 暂不退役（交错：立刻排空 n4）。
	if err := s.DrainNode("n2"); err != nil {
		t.Fatal(err)
	}
	// n4 本来没任务：空节点可以立刻退役，容量先收缩一刀。
	if err := s.DrainNode("n4"); err != nil {
		t.Fatal(err)
	}
	if info, _ := s.DrainStatus("n4"); info.RemainingTasks != 0 {
		t.Fatalf("n4 should be empty: %+v", info)
	}
	if err := s.CompleteNodeDrain("n4"); err != nil {
		t.Fatalf("retire empty n4: %v", err)
	}

	s.Tick() // 2: a2: n2 -> n3（progress=2）；n2 已空
	info, _ := s.DrainStatus("n2")
	if info.RemainingTasks != 0 || info.State != api.NodeDraining {
		t.Fatalf("n2 should be empty but still Draining: %+v", info)
	}
	j, _ := s.GetJob("jobA")
	if j.Tasks[1].NodeID != "n3" || j.Tasks[1].Attempt != 0 || j.Tasks[1].Progress != 2 {
		t.Fatalf("a2 migrated n2->n3 with progress: %+v", j.Tasks[1])
	}
	assertLedger(t, s)

	// 交错：作业在排空进行中提交。jobB 两个 2cpu 任务：n1/n3 各余 1cpu，
	// n2 排空不接收、n4 已退役，整体放不下 -> 收下排队（Pending），不判失败。
	mustSubmit(t, s, api.JobSpec{ID: "jobB", Priority: 1, Tasks: []api.TaskSpec{
		{Name: "b1", Resources: api.Resource{CPU: 2, Memory: 1}, Duration: 5},
		{Name: "b2", Resources: api.Resource{CPU: 2, Memory: 1}, Duration: 5},
	}})
	s.Tick() // 3
	assertJobState(t, s, "jobB", api.JobPending)

	// n2 此刻退役（任务早已迁空），容量再收缩一刀。
	if err := s.CompleteNodeDrain("n2"); err != nil {
		t.Fatalf("retire n2 after migration: %v", err)
	}
	if got := nodeState(t, s, "n2").State; got != api.NodeDead {
		t.Fatalf("n2 retired, got %s", got)
	}
	assertLedger(t, s)

	// 交错：n3 也开始排空，a2 必须再次迁移（n3 -> n1 放不下 -> Pending 排队，
	// 等 jobA 的 a1 结束；同时 jobB 也在排队）。排空进展持续可见。
	if err := s.DrainNode("n3"); err != nil {
		t.Fatal(err)
	}
	s.Tick() // 4: a2 被驱逐（progress=3）-> Pending；n1 余 1cpu 放不下
	info, _ = s.DrainStatus("n3")
	if info.RemainingTasks != 0 {
		t.Fatalf("n3 should be empty after eviction even if task is queued: %+v", info)
	}
	j, _ = s.GetJob("jobA")
	if j.Tasks[1].State != api.TaskPending || j.Tasks[1].Attempt != 0 || j.Tasks[1].Progress != 3 {
		t.Fatalf("a2 queued with preserved progress: %+v", j.Tasks[1])
	}
	if err := s.CompleteNodeDrain("n3"); err != nil {
		t.Fatalf("n3 empty -> retire ok: %v", err)
	}
	assertLedger(t, s)

	// 推进到 a1 完成（a1: tick1..tick14）。n1 释放后 gang 队列按优先级：
	// jobA(5) 的 a2 先得资源续跑；jobB 随后整体放置。
	// 时间线：a1 占着 n1 跑到 tick14 末；a2 在 tick4 带 progress=3 排队，
	// tick15 才重新在 n1 上续跑，再跑 11 拍到 tick25 末满 14。
	for i := 0; i < 21; i++ {
		s.Tick() // tick5..tick25
		assertLedger(t, s)
	}
	j, _ = s.GetJob("jobA")
	if j.State != api.JobSucceeded || j.Tasks[1].Progress != 14 {
		t.Fatalf("jobA must finish with continuous progress 14: %s %+v", j.State, j.Tasks[1])
	}
	if j.Tasks[1].Attempt != 0 {
		t.Fatalf("planned migrations must not bump attempt: %d", j.Tasks[1].Attempt)
	}

	// 交错：在 jobB 即将完成前“进程重启”。先让 jobB 跑起来并跑完一拍。
	// 此时 n1 已空闲，jobB 整体放置（两任务都在 n1，各 2cpu）。
	for i := 0; i < 3; i++ {
		s.Tick()
	}
	jb, _ := s.GetJob("jobB")
	if jb.State != api.JobRunning {
		t.Fatalf("jobB should be running: %s", jb.State)
	}

	// 全新进程：节点重新注册（n2/n4 已退役必须被拒），Recover 后继续。
	s2 := New(Config{HeartbeatTimeoutTicks: 2, StateFile: path, Logger: testLogger(t)})
	if err := s2.AddNode("n1", api.Resource{CPU: 4, Memory: 4}); err != nil {
		t.Fatal(err)
	}
	for _, dead := range []string{"n2", "n4"} {
		if err := s2.AddNode(dead, api.Resource{CPU: 4, Memory: 4}); !errors.Is(err, api.ErrNodeRetired) {
			t.Fatalf("%s must be rejected as retired on restart, got %v", dead, err)
		}
	}
	// n3 已退役，同样不能复活（即使恢复前列表里先列它）。
	if err := s2.AddNode("n3", api.Resource{CPU: 4, Memory: 4}); !errors.Is(err, api.ErrNodeRetired) {
		t.Fatalf("n3 retired -> reject, got %v", err)
	}
	if _, err := s2.Recover(); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"n2", "n3", "n4"} {
		for _, n := range s2.Nodes() {
			if n.ID == id && n.State != api.NodeDead {
				t.Fatalf("%s must not exist as healthy after restart, got %+v", id, n)
			}
		}
	}
	if got := healthyCapacitySum(s2); got != (api.Resource{CPU: 4, Memory: 4}) {
		t.Fatalf("healthy capacity after restart must be just n1, got %+v", got)
	}

	// 已结束作业不复活：jobA 保持 Succeeded，重复提交被拒。
	j, _ = s2.GetJob("jobA")
	if j.State != api.JobSucceeded {
		t.Fatalf("finished jobA must not resurrect: %s", j.State)
	}
	if err := s2.Submit(api.JobSpec{ID: "jobA", Tasks: []api.TaskSpec{
		{Name: "x", Resources: api.Resource{CPU: 1, Memory: 1}, Duration: 1},
	}}); !errors.Is(err, api.ErrDuplicateJob) {
		t.Fatalf("resubmit finished jobA must be duplicate, got %v", err)
	}

	// jobB 恢复后续跑完成，进度跨重启连续，只算一次执行。
	for i := 0; i < 6; i++ {
		s2.Tick()
		assertLedger(t, s2)
	}
	jb, _ = s2.GetJob("jobB")
	if jb.State != api.JobSucceeded {
		t.Fatalf("jobB must finish after restart: %s (%+v)", jb.State, jb.Tasks)
	}
	for _, task := range jb.Tasks {
		if task.State != api.TaskSucceeded || task.Progress != 5 {
			t.Fatalf("jobB task must execute once with progress 5: %+v", task)
		}
	}
	if got := s2.exec.Inflight(); got != 0 {
		t.Fatalf("no inflight may remain, got %d", got)
	}
	for _, n := range s2.Nodes() {
		if n.Used != (api.Resource{}) {
			t.Fatalf("node %s leaked: %+v", n.ID, n.Used)
		}
	}
	t.Log("deterministic interleave: all jobs accounted, progress continuous, capacity shrunken")
}

// TestDrainConcurrentChaos 随机交错压力：多个 goroutine 同时推进 tick、
// 提交作业、排空/退役不同节点。最终：被接受作业全部恰好成功一次、
// 账本清零、退役节点保持 Dead、无重复执行。（-race -count 多轮验证）
func TestDrainConcurrentChaos(t *testing.T) {
	s := newFourNodeScheduler(t) // n1={6,6} n2/n3/n4={2,2}

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
	var acceptedMu sync.Mutex
	accepted := map[string]bool{}

	// 24 个小作业，提交与调度交错。
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("chaos-%02d", i)
			err := s.Submit(api.JobSpec{ID: id, Priority: i % 3, Tasks: []api.TaskSpec{
				{Name: "a", Resources: api.Resource{CPU: 1, Memory: 1}, Duration: 1 + i%2},
				{Name: "b", Resources: api.Resource{CPU: 1, Memory: 1}, Duration: 1 + i%2},
			}})
			switch {
			case err == nil:
				acceptedMu.Lock()
				accepted[id] = true
				acceptedMu.Unlock()
			case errors.Is(err, api.ErrInsufficientResources):
				// 容量收缩后永远放不下被明确拒绝是合法结果。
			default:
				t.Errorf("unexpected submit error for %s: %v", id, err)
			}
		}(i)
	}

	// 三台小节点先后排空并退役（退役在节点空之前会被拒，循环重试）。
	for _, nodeID := range []string{"n2", "n3", "n4"} {
		wg.Add(1)
		go func(nodeID string) {
			defer wg.Done()
			if err := s.DrainNode(nodeID); err != nil {
				t.Errorf("drain %s: %v", nodeID, err)
				return
			}
			deadline := time.Now().Add(20 * time.Second)
			for {
				err := s.CompleteNodeDrain(nodeID)
				if err == nil {
					return
				}
				if !errors.Is(err, api.ErrNodeNotEmpty) {
					t.Errorf("retire %s: %v", nodeID, err)
					return
				}
				if time.Now().After(deadline) {
					t.Errorf("node %s never emptied", nodeID)
					return
				}
				time.Sleep(time.Millisecond)
			}
		}(nodeID)
	}
	wg.Wait()

	// 等被接受作业全部终态。
	deadline := time.Now().Add(40 * time.Second)
	for {
		done := true
		acceptedMu.Lock()
		for id := range accepted {
			j, err := s.GetJob(id)
			if err != nil {
				t.Fatalf("accepted job %s lost: %v", id, err)
			}
			if j.State != api.JobSucceeded && j.State != api.JobFailed {
				done = false
			}
		}
		acceptedMu.Unlock()
		if done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("jobs did not settle in time")
		}
		time.Sleep(2 * time.Millisecond)
	}
	close(stop)
	<-ticksDone

	acceptedMu.Lock()
	defer acceptedMu.Unlock()
	for id := range accepted {
		j, _ := s.GetJob(id)
		if j.State != api.JobSucceeded {
			t.Fatalf("job %s must succeed (no failure injected): %s", id, j.State)
		}
		for _, task := range j.Tasks {
			if task.State != api.TaskSucceeded {
				t.Fatalf("job %s task not succeeded: %+v", id, task)
			}
			// 只经历了计划内迁移：Attempt 必须为 0，进度恰好等于 Duration。
			if task.Attempt != 0 {
				t.Fatalf("job %s task %s attempt=%d: planned drain must not count as retry",
					id, task.Spec.Name, task.Attempt)
			}
			if task.Progress != task.Spec.Duration {
				t.Fatalf("job %s task %s executed extra ticks: progress=%d duration=%d",
					id, task.Spec.Name, task.Progress, task.Spec.Duration)
			}
		}
	}
	if got := s.exec.Inflight(); got != 0 {
		t.Fatalf("inflight must be empty, got %d", got)
	}
	for _, n := range s.Nodes() {
		if n.Used != (api.Resource{}) {
			t.Fatalf("node %s leaked resources: %+v", n.ID, n.Used)
		}
	}
	for _, id := range []string{"n2", "n3", "n4"} {
		if got := nodeState(t, s, id).State; got != api.NodeDead {
			t.Fatalf("node %s must be retired, got %s", id, got)
		}
	}
	t.Logf("chaos interleave: %d accepted jobs all succeeded once, 3 nodes retired", len(accepted))
}
