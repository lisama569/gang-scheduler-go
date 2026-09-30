package scheduler

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/gang-scheduler-go/pkg/api"
)

// TestMultiDrainInterleaveDeterministic 多台节点先后进入维护、有的还在排空中，
// 排空与正常调度推进、作业提交交错时账本始终一致：
//   - 不丢作业：所有被接受作业最终 Succeeded；
//   - 不重复统计：每个任务恰好成功一次，排空迁移不增加 Attempt、保留进度；
//   - 排空剩余任务/资源持续可观测，迁完归零；
//   - 已退役节点 Dead，全部健康节点终态账本为零。
func TestMultiDrainInterleaveDeterministic(t *testing.T) {
	s := New(Config{HeartbeatTimeoutTicks: 2, Logger: testLogger(t)})
	// 五台节点：n1..n4 主力，n5 作为排空期间持续接收迁移任务的健康兜底。
	for _, id := range []string{"n1", "n2", "n3", "n4", "n5"} {
		if err := s.AddNode(id, api.Resource{CPU: 4, Memory: 8}); err != nil {
			t.Fatal(err)
		}
	}

	// 放一批作业占住 n1..n3（资源量取整节点容量，强制一任务一节点），
	// n5 保持空闲用于承接后续迁移。
	mustSubmit(t, s, api.JobSpec{ID: "base", Priority: 5, Tasks: []api.TaskSpec{
		{Name: "a", Resources: api.Resource{CPU: 4, Memory: 2}, Duration: 8},
		{Name: "b", Resources: api.Resource{CPU: 4, Memory: 2}, Duration: 7},
		{Name: "c", Resources: api.Resource{CPU: 4, Memory: 2}, Duration: 6},
	}})
	s.Tick() // 1: a->n1, b->n2, c->n3

	// n4 空闲，先排空退役（容量立即收缩），n5 仍是健康兜底。
	if err := s.DrainNode("n4"); err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteNodeDrain("n4"); err != nil {
		t.Fatalf("empty n4 retires: %v", err)
	}

	// n2 先进入排空（b 还在上面跑），但不立即推进迁移。
	if err := s.DrainNode("n2"); err != nil {
		t.Fatal(err)
	}
	info, _ := s.DrainStatus("n2")
	if info.State != api.NodeDraining || info.RemainingTasks != 1 {
		t.Fatalf("n2 should be draining with 1 task: %+v", info)
	}
	if info.RemainingResources.CPU != 4 {
		t.Fatalf("remaining resources should be visible, got %+v", info.RemainingResources)
	}

	s.Tick() // 2: b 安全迁到健康兜底节点 n5
	j, _ := s.GetJob("base")
	for _, task := range j.Tasks {
		if task.NodeID == "n2" || task.NodeID == "n4" {
			t.Fatalf("no task may remain on draining/retired node after eviction: %+v", task)
		}
	}
	// b 是被计划内迁移的：Attempt 不增加，进度 >=1（tick1 已跑一拍）。
	var b *api.TaskRuntime
	for i := range j.Tasks {
		if j.Tasks[i].Spec.Name == "b" {
			b = &j.Tasks[i]
		}
	}
	if b.Attempt != 0 || b.Progress < 1 {
		t.Fatalf("migrated b must keep attempt=0 and progress>=1: %+v", b)
	}
	info, _ = s.DrainStatus("n2")
	if info.RemainingTasks != 0 || info.RemainingResources != (api.Resource{}) {
		t.Fatalf("n2 must be drained empty: %+v", info)
	}

	// n2 迁空后退役；紧接着 n3 进入排空（c 在上面跑），与新作业提交交错。
	if err := s.CompleteNodeDrain("n2"); err != nil {
		t.Fatalf("retire n2: %v", err)
	}
	if err := s.DrainNode("n3"); err != nil {
		t.Fatal(err)
	}
	// 收缩后只剩 n1 健康（n3 排空中不收新任务，n2/n4 已退役）：
	// {4,...} 单任务仍可排队等待（n1={4,8} 永远放得下，属于暂时不足）。
	mustSubmit(t, s, api.JobSpec{ID: "late", Tasks: []api.TaskSpec{
		{Name: "x", Resources: api.Resource{CPU: 4, Memory: 4}, Duration: 3},
	}})

	// n3 排空推进 + 若干 tick 调度交错。
	for i := 0; i < 12; i++ {
		s.Tick()
		assertDrainLedger(t, s)
	}
	// c 应已迁走（不在 n3），可退役 n3。
	if err := s.CompleteNodeDrain("n3"); err != nil {
		t.Fatalf("retire n3 after migration: %v", err)
	}
	for i := 0; i < 12; i++ {
		s.Tick()
		assertDrainLedger(t, s)
	}

	for _, id := range []string{"base", "late"} {
		jb, _ := s.GetJob(id)
		if jb.State != api.JobSucceeded {
			t.Fatalf("job %s lost or not succeeded: %s (%+v)", id, jb.State, jb.Tasks)
		}
		for _, task := range jb.Tasks {
			if task.State != api.TaskSucceeded {
				t.Fatalf("job %s task %s not succeeded", id, task.Spec.Name)
			}
		}
	}
	for _, id := range []string{"n2", "n3", "n4"} {
		if got := nodeState(t, s, id).State; got != api.NodeDead {
			t.Fatalf("%s should be retired, got %s", id, got)
		}
	}
	if s.exec.Inflight() != 0 {
		t.Fatalf("no inflight may remain, got %d", s.exec.Inflight())
	}
	for _, n := range s.Nodes() {
		if n.Used != (api.Resource{}) {
			t.Fatalf("all ledgers must be zero at the end, %s used=%+v", n.ID, n.Used)
		}
	}
	t.Log("multi-drain interleave: all jobs succeeded exactly once, n2/n3/n4 retired")
}

// TestDrainInterleavedWithFailure 排空链路与故障链路交错时的边界：
// 排空中节点（任务已安全迁走）在等待退役期间真的发生故障（停止心跳），
// 它走故障终态转 Dead —— 排空标记必须被清除（重启后不会被错误套成 Draining），
// 且不能再走 CompleteNodeDrain 退役；已迁走的任务不受二次影响、作业最终成功。
func TestDrainInterleavedWithFailure(t *testing.T) {
	s := New(Config{HeartbeatTimeoutTicks: 2, Logger: testLogger(t)})
	if err := s.AddNode("n1", api.Resource{CPU: 8, Memory: 16}); err != nil {
		t.Fatal(err)
	}
	if err := s.AddNode("n2", api.Resource{CPU: 8, Memory: 16}); err != nil {
		t.Fatal(err)
	}
	mustSubmit(t, s, api.JobSpec{ID: "job", Tasks: []api.TaskSpec{
		{Name: "t", Resources: api.Resource{CPU: 2, Memory: 2}, Duration: 5},
	}})
	s.Tick() // 1: t->n1
	if err := s.DrainNode("n1"); err != nil {
		t.Fatal(err)
	}
	s.Tick() // 2: t 被安全驱逐（进度保留、attempt 不增）并迁到 n2
	j, _ := s.GetJob("job")
	if j.Tasks[0].NodeID != "n2" || j.Tasks[0].Attempt != 0 {
		t.Fatalf("setup: task should have migrated safely to n2: %+v", j.Tasks[0])
	}
	info, _ := s.DrainStatus("n1")
	if info.RemainingTasks != 0 {
		t.Fatalf("setup: n1 should be empty after migration: %+v", info)
	}
	// 还没退役，n1 就在维护中发生真实故障（停止心跳）。
	s.TriggerNodeFailure("n1")
	for i := 0; i < 4; i++ {
		s.Tick()
	}
	if got := nodeState(t, s, "n1").State; got != api.NodeDead {
		t.Fatalf("drained-but-not-retired node must be reclaimed on heartbeat timeout, got %s", got)
	}
	if s.store.IsDraining("n1") {
		t.Fatal("drain marker must be cleared once the draining node dies of failure")
	}
	// 已按故障终态死亡：不能再按排空退役。
	if err := s.CompleteNodeDrain("n1"); !errors.Is(err, api.ErrNodeNotDraining) {
		t.Fatalf("dead node must not be completable as drain, got %v", err)
	}
	// 已迁到 n2 的任务不被二次扰动，作业正常成功。
	for i := 0; i < 6; i++ {
		s.Tick()
	}
	j, _ = s.GetJob("job")
	if j.State != api.JobSucceeded || j.Tasks[0].NodeID != "n2" {
		t.Fatalf("migrated job must succeed undisturbed on n2: %s %+v", j.State, j.Tasks[0])
	}
	t.Log("failure after safe migration: node reclaimed via failure path, marker cleared, job intact")
}

// TestDrainInterleavedWithRestart 排空进行到一半时重启，恢复后继续排空、
// 继续退役，期间提交的作业不丢、容量不反弹，最终账全平。
func TestDrainInterleavedWithRestart(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/snapshot.json"

	s1 := New(Config{HeartbeatTimeoutTicks: 2, StateFile: path, Logger: testLogger(t)})
	for _, id := range []string{"n1", "n2", "n3"} {
		if err := s1.AddNode(id, api.Resource{CPU: 6, Memory: 12}); err != nil {
			t.Fatal(err)
		}
	}
	mustSubmit(t, s1, api.JobSpec{ID: "job1", Tasks: []api.TaskSpec{
		{Name: "a", Resources: api.Resource{CPU: 3, Memory: 2}, Duration: 10},
		{Name: "b", Resources: api.Resource{CPU: 3, Memory: 2}, Duration: 10},
	}})
	s1.Tick() // 1: a->n1, b->n2
	// n2 进入排空，n3 是空节点先退役：重启后一个应保持 Draining，一个应保持 Dead。
	if err := s1.DrainNode("n2"); err != nil {
		t.Fatal(err)
	}
	if err := s1.DrainNode("n3"); err != nil {
		t.Fatal(err)
	}
	if err := s1.CompleteNodeDrain("n3"); err != nil {
		t.Fatal(err)
	}
	// b 还没来得及迁走就“崩溃”。

	s2 := New(Config{HeartbeatTimeoutTicks: 2, StateFile: path, Logger: testLogger(t)})
	for _, id := range []string{"n1", "n2", "n3"} {
		if err := s2.AddNode(id, api.Resource{CPU: 6, Memory: 12}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s2.Recover(); err != nil {
		t.Fatal(err)
	}
	if got := nodeState(t, s2, "n2").State; got != api.NodeDraining {
		t.Fatalf("n2 must resume Draining, got %s", got)
	}
	if got := nodeState(t, s2, "n3").State; got != api.NodeDead {
		t.Fatalf("n3 must remain retired Dead after restart, got %s", got)
	}

	// 恢复后继续提交作业（与继续排空交错）。容量仅剩 n1 健康（n2 排空、n3 退役）。
	// {6,...} 任务放得进 n1，但 a 占着 3cpu：先排队，a 完成后再上。
	mustSubmit(t, s2, api.JobSpec{ID: "job2", Tasks: []api.TaskSpec{
		{Name: "z", Resources: api.Resource{CPU: 6, Memory: 2}, Duration: 2},
	}})
	for i := 0; i < 20; i++ {
		s2.Tick()
		assertDrainLedger(t, s2)
	}
	if err := s2.CompleteNodeDrain("n2"); err != nil {
		t.Fatalf("n2 must be drainable to completion after restart: %v", err)
	}
	for i := 0; i < 20; i++ {
		s2.Tick()
		assertDrainLedger(t, s2)
	}
	for _, id := range []string{"job1", "job2"} {
		jb, _ := s2.GetJob(id)
		if jb.State != api.JobSucceeded {
			t.Fatalf("job %s must succeed across restart+continued drain: %s", id, jb.State)
		}
	}
	for _, id := range []string{"n2", "n3"} {
		if got := nodeState(t, s2, id).State; got != api.NodeDead {
			t.Fatalf("%s must be Dead finally, got %s", id, got)
		}
	}
	if s2.exec.Inflight() != 0 {
		t.Fatalf("inflight must be zero, got %d", s2.exec.Inflight())
	}
	t.Log("restart mid-drain: draining resumed, retired stayed dead, jobs preserved")
}

// TestDrainSubmitTickConcurrentInterleave 排空/退役、提交、Tick 三者持续并发，
// 在 -race 下校验：不丢作业、不超发、终态不复活、迁移任务不重复统计。
func TestDrainSubmitTickConcurrentInterleave(t *testing.T) {
	s := New(Config{HeartbeatTimeoutTicks: 2, Logger: testLogger(t)})
	for _, id := range []string{"n1", "n2", "n3", "n4"} {
		if err := s.AddNode(id, api.Resource{CPU: 4, Memory: 8}); err != nil {
			t.Fatal(err)
		}
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
	var accMu sync.Mutex
	accepted := map[string]bool{}
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("ij-%02d", i)
			err := s.Submit(api.JobSpec{ID: id, Priority: i % 4, Tasks: []api.TaskSpec{
				{Name: "a", Resources: api.Resource{CPU: 1, Memory: 1}, Duration: 1 + i%3},
				{Name: "b", Resources: api.Resource{CPU: 1, Memory: 1}, Duration: 1 + i%3},
			}})
			if err == nil {
				accMu.Lock()
				accepted[id] = true
				accMu.Unlock()
			} else if !errors.Is(err, api.ErrInsufficientResources) {
				t.Errorf("submit %s unexpected err: %v", id, err)
			}
		}(i)
	}
	// n2 先排空；n4 在排空进行中也排空，两台退役彼此独立推进。
	for _, id := range []string{"n2", "n4"} {
		wg.Add(1)
		go func(nodeID string) {
			defer wg.Done()
			if err := s.DrainNode(nodeID); err != nil {
				t.Errorf("drain %s: %v", nodeID, err)
				return
			}
			deadline := time.Now().Add(30 * time.Second)
			for {
				err := s.CompleteNodeDrain(nodeID)
				if err == nil {
					return
				}
				if !errors.Is(err, api.ErrNodeNotEmpty) {
					t.Errorf("complete %s: %v", nodeID, err)
					return
				}
				if time.Now().After(deadline) {
					t.Errorf("node %s never emptied", nodeID)
					return
				}
				time.Sleep(time.Millisecond)
			}
		}(id)
	}
	wg.Wait()

	deadline := time.Now().Add(60 * time.Second)
	for {
		done := true
		accMu.Lock()
		for id := range accepted {
			j, err := s.GetJob(id)
			if err != nil {
				t.Fatalf("accepted job %s vanished: %v", id, err)
			}
			if j.State != api.JobSucceeded && j.State != api.JobFailed {
				done = false
			}
		}
		accMu.Unlock()
		if done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("accepted jobs did not settle")
		}
		time.Sleep(2 * time.Millisecond)
	}
	close(stop)
	<-ticksDone

	accMu.Lock()
	for id := range accepted {
		j, _ := s.GetJob(id)
		if j.State != api.JobSucceeded {
			t.Fatalf("job %s must succeed under drain (no failures injected): %s", id, j.State)
		}
		for _, task := range j.Tasks {
			if task.State != api.TaskSucceeded {
				t.Fatalf("job %s task %s not succeeded (double execution/stat?)", id, task.Spec.Name)
			}
			// 计划内迁移绝不能被计成故障重试。
			if task.Attempt != 0 {
				t.Fatalf("job %s task %s attempt=%d: drain must not count attempts",
					id, task.Spec.Name, task.Attempt)
			}
		}
	}
	accMu.Unlock()
	if s.exec.Inflight() != 0 {
		t.Fatalf("inflight leaked: %d", s.exec.Inflight())
	}
	for _, n := range s.Nodes() {
		if n.Used.CPU < 0 || n.Used.Memory < 0 || n.Used.CPU > n.Capacity.CPU || n.Used.Memory > n.Capacity.Memory {
			t.Fatalf("ledger corrupted: %+v", n)
		}
		if n.Used != (api.Resource{}) {
			t.Fatalf("node %s leaked resources: %+v", n.ID, n.Used)
		}
	}
	for _, id := range []string{"n2", "n4"} {
		if got := nodeState(t, s, id).State; got != api.NodeDead {
			t.Fatalf("%s must be retired, got %s", id, got)
		}
	}
	t.Logf("concurrent drain/submit/tick: %d accepted jobs all succeeded once", len(accepted))
}

// assertDrainLedger 在交错推进的任意时点校验资源账：
// 健康节点 Used 之和 == executor 在执行行请求之和；无超发、无负数。
func assertDrainLedger(t *testing.T, s *Scheduler) {
	t.Helper()
	var usedCPU, usedMem, runCPU, runMem int
	for _, n := range s.Nodes() {
		if n.Used.CPU < 0 || n.Used.Memory < 0 ||
			n.Used.CPU > n.Capacity.CPU || n.Used.Memory > n.Capacity.Memory {
			t.Fatalf("ledger broken on %s: %+v", n.ID, n)
		}
		if n.State == api.NodeHealthy {
			usedCPU += n.Used.CPU
			usedMem += n.Used.Memory
		}
	}
	for _, jb := range s.store.List() {
		for _, task := range jb.Tasks {
			if task.State == api.TaskRunning && s.exec.Running(jb.Spec.ID, task.Spec.Name) {
				runCPU += task.Spec.Resources.CPU
				runMem += task.Spec.Resources.Memory
			}
		}
	}
	if usedCPU != runCPU || usedMem != runMem {
		t.Fatalf("drain interleave ledger mismatch: healthyUsed={%d,%d} inflight={%d,%d}",
			usedCPU, usedMem, runCPU, runMem)
	}
}
