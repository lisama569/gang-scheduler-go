package scheduler

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/gang-scheduler-go/pkg/api"
)

// newDrainTestScheduler 构造三节点调度器：n1={6,12} n2={2,4} n3={4,8}。
func newDrainTestScheduler(t *testing.T) *Scheduler {
	t.Helper()
	s := New(Config{HeartbeatTimeoutTicks: 2, Logger: testLogger(t)})
	if err := s.AddNode("n1", api.Resource{CPU: 6, Memory: 12}); err != nil {
		t.Fatal(err)
	}
	if err := s.AddNode("n2", api.Resource{CPU: 2, Memory: 4}); err != nil {
		t.Fatal(err)
	}
	if err := s.AddNode("n3", api.Resource{CPU: 4, Memory: 8}); err != nil {
		t.Fatal(err)
	}
	return s
}

func nodeState(t *testing.T, s *Scheduler, id string) api.NodeRuntime {
	t.Helper()
	for _, n := range s.Nodes() {
		if n.ID == id {
			return n
		}
	}
	t.Fatalf("node %s not found", id)
	return api.NodeRuntime{}
}

// TestDrainMigratesTasks 验证计划内排空的主流程：
// 标记排空 -> 不再收新任务 -> 存量任务安全迁到健康节点 -> 退役后容量收缩。
func TestDrainMigratesTasks(t *testing.T) {
	s := newDrainTestScheduler(t)

	// t1{5,4}->n1（余{1,8}），t2{2,2} 放不进 n1 -> n2。
	mustSubmit(t, s, api.JobSpec{ID: "job", Priority: 1, Tasks: []api.TaskSpec{
		{Name: "t1", Resources: api.Resource{CPU: 5, Memory: 4}, Duration: 6},
		{Name: "t2", Resources: api.Resource{CPU: 2, Memory: 2}, Duration: 6},
	}})
	s.Tick() // 1: 放置 t1->n1, t2->n2
	j, _ := s.GetJob("job")
	if j.Tasks[0].NodeID != "n1" || j.Tasks[1].NodeID != "n2" {
		t.Fatalf("unexpected initial placement: %+v", j.Tasks)
	}

	// 主动把健康节点 n2 置为维护/排空中。
	if err := s.DrainNode("n2"); err != nil {
		t.Fatalf("drain n2: %v", err)
	}
	if got := nodeState(t, s, "n2").State; got != api.NodeDraining {
		t.Fatalf("n2 state=%s want Draining", got)
	}
	// 排空进展可见：t2 还没迁走（驱逐随下一个 Tick 推进）。
	info, err := s.DrainStatus("n2")
	if err != nil {
		t.Fatal(err)
	}
	if info.RemainingTasks != 1 || info.RemainingResources.CPU != 2 {
		t.Fatalf("drain progress before tick: %+v", info)
	}
	// 节点未空，不能退役。
	if err := s.CompleteNodeDrain("n2"); !errors.Is(err, api.ErrNodeNotEmpty) {
		t.Fatalf("complete while task remains: %v", err)
	}

	s.Tick() // 2: t2 被安全驱逐 -> Pending -> 同 tick 迁到 n3
	j, _ = s.GetJob("job")
	if j.Tasks[1].State != api.TaskRunning || j.Tasks[1].NodeID != "n3" {
		t.Fatalf("t2 must migrate to healthy n3, got %+v", j.Tasks[1])
	}
	if j.Tasks[1].Attempt != 1 {
		t.Fatalf("t2 attempt should be 1 after migration, got %d", j.Tasks[1].Attempt)
	}
	if j.Tasks[0].State != api.TaskRunning || j.Tasks[0].NodeID != "n1" {
		t.Fatalf("t1 must be undisturbed on n1: %+v", j.Tasks[0])
	}
	info, _ = s.DrainStatus("n2")
	if info.RemainingTasks != 0 {
		t.Fatalf("n2 should be empty after eviction: %+v", info)
	}

	// 任务全部迁走后，n2 正式退役，容量从集群中扣除。
	if err := s.CompleteNodeDrain("n2"); err != nil {
		t.Fatalf("complete drain: %v", err)
	}
	if got := nodeState(t, s, "n2").State; got != api.NodeDead {
		t.Fatalf("n2 state=%s want Dead after retire", got)
	}

	// 收缩后的集群容量 = n1+n3 = {10,20}：永远放不下的作业被明确拒绝。
	err = s.Submit(api.JobSpec{ID: "too-big", Tasks: []api.TaskSpec{
		{Name: "x", Resources: api.Resource{CPU: 11, Memory: 1}, Duration: 1},
	}})
	if !errors.Is(err, api.ErrInsufficientResources) {
		t.Fatalf("job exceeding shrunken capacity must be rejected, got %v", err)
	}

	for i := 0; i < 8; i++ {
		s.Tick()
	}
	j, _ = s.GetJob("job")
	if j.State != api.JobSucceeded {
		t.Fatalf("job must succeed after migration: %s (%+v)", j.State, j.Tasks)
	}
	// t2 只在 n3 上完整执行了一次（attempt=1），没有被重复统计。
	if j.Tasks[1].NodeID != "n3" || j.Tasks[1].Attempt != 1 {
		t.Fatalf("t2 final: %+v", j.Tasks[1])
	}
	if n := nodeState(t, s, "n2"); n.Used != (api.Resource{}) {
		t.Fatalf("retired n2 must hold no resources: %+v", n)
	}
	t.Logf("drain+migration verified: t1=%+v t2=%+v", j.Tasks[0], j.Tasks[1])
}

// TestDrainEmptyNodeAndCapacityShrink 空节点可立即排空退役；
// 之后提交按收缩后容量判定，暂时放不下的作业继续排队且状态可解释。
func TestDrainEmptyNodeAndCapacityShrink(t *testing.T) {
	s := newTestScheduler(t) // n1={4,8} n2={2,4}

	if err := s.DrainNode("n2"); err != nil {
		t.Fatal(err)
	}
	info, _ := s.DrainStatus("n2")
	if info.RemainingTasks != 0 {
		t.Fatalf("empty node should have nothing to migrate: %+v", info)
	}
	if err := s.CompleteNodeDrain("n2"); err != nil {
		t.Fatalf("empty node retires immediately: %v", err)
	}
	if got := nodeState(t, s, "n2").State; got != api.NodeDead {
		t.Fatalf("n2 state=%s want Dead", got)
	}

	// 收缩后集群只剩 n1={4,8}：总量 {5,4} 的作业永远放不下 -> 拒绝。
	err := s.Submit(api.JobSpec{ID: "oversize", Tasks: []api.TaskSpec{
		{Name: "a", Resources: api.Resource{CPU: 3, Memory: 2}, Duration: 1},
		{Name: "b", Resources: api.Resource{CPU: 2, Memory: 2}, Duration: 1},
	}})
	if !errors.Is(err, api.ErrInsufficientResources) {
		t.Fatalf("job exceeding shrunken capacity must be rejected, got %v", err)
	}

	// 能放下但当前被占用的作业：收下并排队，不错误判成功/失败。
	mustSubmit(t, s, api.JobSpec{ID: "first", Tasks: []api.TaskSpec{
		{Name: "a", Resources: api.Resource{CPU: 4, Memory: 4}, Duration: 3},
	}})
	s.Tick() // 1: first 占满 n1
	mustSubmit(t, s, api.JobSpec{ID: "second", Tasks: []api.TaskSpec{
		{Name: "b", Resources: api.Resource{CPU: 4, Memory: 4}, Duration: 2},
	}})
	s.Tick() // 2: n1 被占满，second 继续排队
	j, _ := s.GetJob("second")
	if j.State != api.JobPending {
		t.Fatalf("second must stay queued (Pending), got %s", j.State)
	}
	for i := 0; i < 6; i++ {
		s.Tick()
	}
	for _, id := range []string{"first", "second"} {
		j, _ := s.GetJob(id)
		if j.State != api.JobSucceeded {
			t.Fatalf("job %s must succeed after capacity frees: %s", id, j.State)
		}
	}
	t.Log("capacity shrink: oversize rejected, queued job waited and succeeded")
}

// TestDrainErrors 排空相关操作的可区分错误。
func TestDrainErrors(t *testing.T) {
	s := newTestScheduler(t)

	if err := s.DrainNode("ghost"); !errors.Is(err, api.ErrNodeNotFound) {
		t.Fatalf("drain unknown node: %v", err)
	}
	if err := s.CompleteNodeDrain("ghost"); !errors.Is(err, api.ErrNodeNotFound) {
		t.Fatalf("complete unknown node: %v", err)
	}
	if _, err := s.DrainStatus("ghost"); !errors.Is(err, api.ErrNodeNotFound) {
		t.Fatalf("status unknown node: %v", err)
	}
	if err := s.CompleteNodeDrain("n1"); !errors.Is(err, api.ErrNodeNotDraining) {
		t.Fatalf("complete healthy node: %v", err)
	}
	if err := s.DrainNode("n2"); err != nil {
		t.Fatal(err)
	}
	if err := s.DrainNode("n2"); !errors.Is(err, api.ErrNodeNotHealthy) {
		t.Fatalf("double drain: %v", err)
	}

	// 死亡节点不能再排空。
	s.TriggerNodeFailure("n1")
	for i := 0; i < 4; i++ {
		s.Tick()
	}
	if err := s.DrainNode("n1"); !errors.Is(err, api.ErrNodeNotHealthy) {
		t.Fatalf("drain dead node: %v", err)
	}
	t.Log("drain error kinds are distinguishable")
}

// TestDrainCompletedTaskNotDisturbed 节点上已完成的任务不受排空影响：
// 保持 Succeeded、attempt 不变、不会被重复执行。
func TestDrainCompletedTaskNotDisturbed(t *testing.T) {
	s := newTestScheduler(t)
	mustSubmit(t, s, api.JobSpec{ID: "job", Tasks: []api.TaskSpec{
		{Name: "t1", Resources: api.Resource{CPU: 2, Memory: 1}, Duration: 2},
	}})
	s.Tick() // 1: 启动（在 n1）
	s.Tick() // 2: 成功
	j, _ := s.GetJob("job")
	if j.State != api.JobSucceeded {
		t.Fatalf("setup: job should succeed: %+v", j)
	}

	if err := s.DrainNode("n1"); err != nil {
		t.Fatal(err)
	}
	s.Tick() // 3: 排空驱逐只作用于在执行行，已完成任务不受影响
	s.Tick() // 4
	j, _ = s.GetJob("job")
	if j.State != api.JobSucceeded || j.Tasks[0].State != api.TaskSucceeded {
		t.Fatalf("finished job must stay succeeded across drain: %+v", j)
	}
	if j.Tasks[0].Attempt != 0 {
		t.Fatalf("completed task must not be re-run: %+v", j.Tasks[0])
	}
	if err := s.CompleteNodeDrain("n1"); err != nil {
		t.Fatalf("node with only finished tasks retires cleanly: %v", err)
	}
	t.Log("completed tasks preserved across drain")
}

// TestDrainingNodeKeepsHeartbeat 排空中的节点心跳照常（不会被误判死）；
// 若维护期间真的发生故障，仍走统一的超时回收路径。
func TestDrainingNodeKeepsHeartbeat(t *testing.T) {
	s := newTestScheduler(t)
	if err := s.DrainNode("n2"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		s.Tick()
	}
	if got := nodeState(t, s, "n2").State; got != api.NodeDraining {
		t.Fatalf("draining node with heartbeat must not be judged dead, got %s", got)
	}

	// 维护期间节点故障：停止心跳后被超时回收，状态转 Dead。
	s.TriggerNodeFailure("n2")
	for i := 0; i < 4; i++ {
		s.Tick()
	}
	if got := nodeState(t, s, "n2").State; got != api.NodeDead {
		t.Fatalf("failed draining node must be reclaimed, got %s", got)
	}
	t.Log("draining node: heartbeat honored, real failure still reclaimed")
}

// TestDrainRecoverMidDrain 排空做到一半进程重启：
// 恢复后节点仍是排空中（不会被当成健康机器），迁到一半的任务继续推进。
func TestDrainRecoverMidDrain(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.json")

	s1 := New(Config{HeartbeatTimeoutTicks: 2, StateFile: path, Logger: testLogger(t)})
	if err := s1.AddNode("n1", api.Resource{CPU: 6, Memory: 12}); err != nil {
		t.Fatal(err)
	}
	if err := s1.AddNode("n2", api.Resource{CPU: 2, Memory: 4}); err != nil {
		t.Fatal(err)
	}
	if err := s1.AddNode("n3", api.Resource{CPU: 4, Memory: 8}); err != nil {
		t.Fatal(err)
	}
	mustSubmit(t, s1, api.JobSpec{ID: "job", Tasks: []api.TaskSpec{
		{Name: "t1", Resources: api.Resource{CPU: 5, Memory: 4}, Duration: 8},
		{Name: "t2", Resources: api.Resource{CPU: 2, Memory: 2}, Duration: 8},
	}})
	s1.Tick() // t1->n1, t2->n2
	if err := s1.DrainNode("n2"); err != nil {
		t.Fatal(err)
	}
	// 在 t2 还跑在 n2 上、排空刚开始时“进程崩溃”。

	// 重启：新调度器 + 同一状态文件。
	s2 := New(Config{HeartbeatTimeoutTicks: 2, StateFile: path, Logger: testLogger(t)})
	if err := s2.AddNode("n1", api.Resource{CPU: 6, Memory: 12}); err != nil {
		t.Fatal(err)
	}
	if err := s2.AddNode("n2", api.Resource{CPU: 2, Memory: 4}); err != nil {
		t.Fatal(err)
	}
	if err := s2.AddNode("n3", api.Resource{CPU: 4, Memory: 8}); err != nil {
		t.Fatal(err)
	}
	if _, err := s2.Recover(); err != nil {
		t.Fatal(err)
	}

	// 维护中的节点不能被当成正常机器用。
	if got := nodeState(t, s2, "n2").State; got != api.NodeDraining {
		t.Fatalf("n2 must stay Draining after restart, got %s", got)
	}
	j, _ := s2.GetJob("job")
	for _, task := range j.Tasks {
		if task.State == api.TaskRunning {
			t.Fatalf("running tasks must be reset on recovery: %+v", task)
		}
	}

	s2.Tick() // t1->n1, t2 必须避开 n2 -> n3
	j, _ = s2.GetJob("job")
	if j.Tasks[1].State != api.TaskRunning || j.Tasks[1].NodeID != "n3" {
		t.Fatalf("t2 must migrate to n3 after recovery, got %+v", j.Tasks[1])
	}
	if j.Tasks[0].NodeID != "n1" {
		t.Fatalf("t1 must be back on n1: %+v", j.Tasks[0])
	}

	// 排空可继续推进直至退役。
	if err := s2.CompleteNodeDrain("n2"); err != nil {
		t.Fatalf("complete drain after recovery: %v", err)
	}
	for i := 0; i < 12; i++ {
		s2.Tick()
	}
	j, _ = s2.GetJob("job")
	if j.State != api.JobSucceeded {
		t.Fatalf("job must succeed across restart+drain: %s (%+v)", j.State, j.Tasks)
	}
	for _, task := range j.Tasks {
		if task.NodeID == "n2" {
			t.Fatalf("no task may run on draining n2 after restart: %+v", task)
		}
	}
	t.Log("mid-drain restart recovered: node stayed draining, tasks migrated, job succeeded")
}

// TestDrainConcurrentWithTicksAndSubmit 排空与调度推进、作业提交并发交错：
// 不丢作业、不重复占资源、已结束作业不复活。
func TestDrainConcurrentWithTicksAndSubmit(t *testing.T) {
	s := newTestScheduler(t) // n1={4,8} n2={2,4}
	if err := s.AddNode("n3", api.Resource{CPU: 4, Memory: 8}); err != nil {
		t.Fatal(err)
	}
	if err := s.AddNode("n4", api.Resource{CPU: 2, Memory: 4}); err != nil {
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
	var acceptedMu sync.Mutex
	accepted := map[string]bool{}
	// 并发提交 30 个小作业（n1+n3 容量足够，全部应收下）。
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := "dj-" + itoa(i)
			err := s.Submit(api.JobSpec{ID: id, Priority: i % 4, Tasks: []api.TaskSpec{
				{Name: "a", Duration: 1 + i%3, Resources: api.Resource{CPU: 1, Memory: 1}},
				{Name: "b", Duration: 1 + i%3, Resources: api.Resource{CPU: 1, Memory: 1}},
			}})
			if err == nil {
				acceptedMu.Lock()
				accepted[id] = true
				acceptedMu.Unlock()
			} else if !errors.Is(err, api.ErrInsufficientResources) {
				t.Errorf("job %s unexpected err: %v", id, err)
			}
		}(i)
	}
	// 并发对 n2、n4 做排空 + 退役（退役在节点空之前会被拒绝，重试直到成功）。
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
					t.Errorf("complete drain %s: %v", nodeID, err)
					return
				}
				if time.Now().After(deadline) {
					t.Errorf("drain %s never emptied", nodeID)
					return
				}
				time.Sleep(2 * time.Millisecond)
			}
		}(id)
	}
	wg.Wait()

	// 等所有被接受作业到达终态。
	deadline := time.Now().Add(60 * time.Second)
	for {
		allTerminal := true
		acceptedMu.Lock()
		for id := range accepted {
			j, err := s.GetJob(id)
			if err != nil {
				t.Fatalf("accepted job %s lost: %v", id, err)
			}
			if j.State != api.JobSucceeded && j.State != api.JobFailed {
				allTerminal = false
			}
		}
		acceptedMu.Unlock()
		if allTerminal {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("jobs did not reach terminal state in time")
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(stop)
	<-ticksDone

	// 终态断言：全部成功（无故障注入，排空不允许丢任务）。
	// 注意：任务在节点退役“之前”于其上正常完成是合法的，NodeID 只是历史记录；
	// 这里断言的是没有任务被遗留在执行行里、没有作业丢失。
	acceptedMu.Lock()
	for id := range accepted {
		j, _ := s.GetJob(id)
		if j.State != api.JobSucceeded {
			t.Fatalf("job %s state=%s want Succeeded", id, j.State)
		}
		for _, task := range j.Tasks {
			if task.State != api.TaskSucceeded {
				t.Fatalf("job %s task %+v not succeeded", id, task)
			}
		}
	}
	acceptedMu.Unlock()
	if got := s.exec.Inflight(); got != 0 {
		t.Fatalf("executor inflight must be empty at the end, got %d", got)
	}
	// 退役节点为 Dead，其余节点账本无泄漏、无超发。
	for _, n := range s.Nodes() {
		if n.Used.CPU < 0 || n.Used.Memory < 0 || n.Used.CPU > n.Capacity.CPU || n.Used.Memory > n.Capacity.Memory {
			t.Fatalf("ledger broken on %s: %+v", n.ID, n)
		}
		if n.Used != (api.Resource{}) {
			t.Fatalf("node %s leaked resources after all jobs done: %+v", n.ID, n.Used)
		}
	}
	for _, id := range []string{"n2", "n4"} {
		if got := nodeState(t, s, id).State; got != api.NodeDead {
			t.Fatalf("node %s should be retired (Dead), got %s", id, got)
		}
	}
	t.Logf("concurrent drain+submit+ticks: %d jobs all succeeded, n2/n4 retired", len(accepted))
}
