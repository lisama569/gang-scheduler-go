package scheduler

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/lisama569/gang-scheduler-go/pkg/api"
)

// writeSnapshot 手工构造一张持久化快照（模拟进程在任意时刻崩溃后的磁盘状态）。
func writeSnapshot(t *testing.T, path string, snap api.Snapshot) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestRecoverTerminalNotResurrected 已结束作业恢复后保持终态，重复提交仍被拒绝。
func TestRecoverTerminalNotResurrected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.json")
	writeSnapshot(t, path, api.Snapshot{Counter: 3, Jobs: []api.JobRecord{{
		Spec: api.JobSpec{ID: "done", Tasks: []api.TaskSpec{
			{Name: "t", Duration: 2, Resources: api.Resource{CPU: 1}},
		}},
		State: api.JobSucceeded,
		Tasks: []api.TaskRecord{{
			Spec:  api.TaskSpec{Name: "t", Duration: 2, Resources: api.Resource{CPU: 1}},
			State: api.TaskSucceeded, NodeID: "n1",
		}},
	}}})

	s := New(Config{StateFile: path, Logger: testLogger(t)})
	if err := s.AddNode("n1", api.Resource{CPU: 4, Memory: 8}); err != nil {
		t.Fatal(err)
	}
	ver, err := s.Recover()
	if err != nil || ver != 3 {
		t.Fatalf("recover: ver=%d err=%v", ver, err)
	}
	j, _ := s.GetJob("done")
	if j.State != api.JobSucceeded || j.Tasks[0].State != api.TaskSucceeded {
		t.Fatalf("terminal job must be preserved: %+v", j)
	}
	// 结束作业不参与调度；跑若干 tick 状态不变。
	for i := 0; i < 3; i++ {
		s.Tick()
	}
	j, _ = s.GetJob("done")
	if j.State != api.JobSucceeded {
		t.Fatalf("finished job resurrected: %s", j.State)
	}
	err = s.Submit(api.JobSpec{ID: "done", Tasks: []api.TaskSpec{
		{Name: "t", Duration: 1, Resources: api.Resource{CPU: 1}},
	}})
	if !errors.Is(err, api.ErrDuplicateJob) {
		t.Fatalf("resubmit recovered terminal job must be duplicate, got %v", err)
	}
	t.Logf("terminal job survived recovery and resubmit rejected: %v", err)
}

// TestRecoverRunningTasksResumed 崩溃前在运行的任务恢复后复位为 Pending，
// 随 Tick 重新 gang 放置并完成；不会出现节点资源重复占用（新池账本初始为零）。
func TestRecoverRunningTasksResumed(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.json")
	writeSnapshot(t, path, api.Snapshot{Counter: 9, Jobs: []api.JobRecord{{
		Spec: api.JobSpec{ID: "work", Priority: 2, Tasks: []api.TaskSpec{
			{Name: "t1", Duration: 3, Resources: api.Resource{CPU: 3, Memory: 2}},
			{Name: "t2", Duration: 3, Resources: api.Resource{CPU: 2, Memory: 2}},
		}},
		State: api.JobRunning,
		Tasks: []api.TaskRecord{
			{Spec: api.TaskSpec{Name: "t1", Duration: 3, Resources: api.Resource{CPU: 3, Memory: 2}},
				State: api.TaskRunning, NodeID: "n1"},
			{Spec: api.TaskSpec{Name: "t2", Duration: 3, Resources: api.Resource{CPU: 2, Memory: 2}},
				State: api.TaskRunning, NodeID: "n2"},
		},
	}}})

	s := New(Config{StateFile: path, HeartbeatTimeoutTicks: 2, Logger: testLogger(t)})
	if err := s.AddNode("n1", api.Resource{CPU: 4, Memory: 8}); err != nil {
		t.Fatal(err)
	}
	if err := s.AddNode("n2", api.Resource{CPU: 2, Memory: 4}); err != nil {
		t.Fatal(err)
	}
	ver, err := s.Recover()
	if err != nil || ver != 9 {
		t.Fatalf("recover ver=%d err=%v", ver, err)
	}
	j, _ := s.GetJob("work")
	if j.State != api.JobPending {
		t.Fatalf("recovered job should be pending re-placement, got %s", j.State)
	}
	for _, task := range j.Tasks {
		if task.State != api.TaskPending || task.NodeID != "" || task.Attempt != 1 {
			t.Fatalf("recovered running task must be Pending with attempt=1: %+v", task)
		}
	}
	// 新节点池账本必须为零：恢复不重复占用崩溃前的资源。
	for _, n := range s.Nodes() {
		if n.Used.CPU != 0 || n.Used.Memory != 0 {
			t.Fatalf("fresh node %s must have zero usage after recover: %+v", n.ID, n.Used)
		}
	}
	// 驱动至完成。
	for i := 0; i < 6; i++ {
		s.Tick()
	}
	j, _ = s.GetJob("work")
	if j.State != api.JobSucceeded {
		t.Fatalf("recovered unfinished job must run to success: %s (%+v)", j.State, j.Tasks)
	}
	t.Logf("recovered job completed: t1=%+v t2=%+v", j.Tasks[0], j.Tasks[1])
}

// TestRecoverPersistRoundTrip 正常运行产生的快照可被另一个 Scheduler 实例继续，
// 且恢复后新版本号单调递增（每次状态落盘 +1）。
func TestRecoverPersistRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.json")

	s1 := New(Config{StateFile: path, Logger: testLogger(t)})
	if err := s1.AddNode("n1", api.Resource{CPU: 4, Memory: 8}); err != nil {
		t.Fatal(err)
	}
	mustSubmit(t, s1, api.JobSpec{ID: "j", Tasks: []api.TaskSpec{
		{Name: "t", Duration: 3, Resources: api.Resource{CPU: 2, Memory: 1}},
	}})
	s1.Tick() // 已启动并跑完一拍（progress=1），尚未完成 -> 进程在此“崩溃”

	s2 := New(Config{StateFile: path, Logger: testLogger(t)})
	if err := s2.AddNode("n1", api.Resource{CPU: 4, Memory: 8}); err != nil {
		t.Fatal(err)
	}
	if _, err := s2.Recover(); err != nil {
		t.Fatal(err)
	}
	// 崩溃前的执行进度随快照保留：恢复后任务从 progress=1 续跑而不是重头再来。
	j, _ := s2.GetJob("j")
	if j.Tasks[0].Progress != 1 || j.Tasks[0].State != api.TaskPending {
		t.Fatalf("recovered task must keep progress=1 and be pending: %+v", j.Tasks[0])
	}
	before := s2.store.Counter()
	s2.Tick() // 恢复后重新放置并启动，tick 末再跑一拍（progress=2，仍 Running）
	j, _ = s2.GetJob("j")
	if j.State != api.JobRunning {
		t.Fatalf("recovered task should relaunch, got state=%s", j.State)
	}
	if j.Tasks[0].Progress != 2 {
		t.Fatalf("relaunched task must continue from saved progress: %+v", j.Tasks[0])
	}
	s2.Tick() // 执行完剩余一拍即完成
	after := s2.store.Counter()
	j, _ = s2.GetJob("j")
	if j.State != api.JobSucceeded {
		t.Fatalf("job should finish on recovered scheduler, got %s", j.State)
	}
	if j.Tasks[0].Progress != 3 {
		t.Fatalf("recovered task must reach full progress 3, got %d", j.Tasks[0].Progress)
	}
	if after <= before {
		t.Fatalf("snapshot version must increase after writes: before=%d after=%d", before, after)
	}
	t.Logf("snapshot version advanced %d -> %d across restart, progress resumed 1 -> 3", before, after)
}

// TestRecoverConcurrentWithTicks 恢复与外部并发 Tick/Submit 同时发生时，
// 不得丢失作业或重复占用：最终全部作业恰好成功一次。
func TestRecoverConcurrentWithTicks(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.json")
	writeSnapshot(t, path, api.Snapshot{Counter: 1, Jobs: []api.JobRecord{{
		Spec: api.JobSpec{ID: "old", Tasks: []api.TaskSpec{
			{Name: "t", Duration: 2, Resources: api.Resource{CPU: 1, Memory: 1}},
		}},
		State: api.JobRunning,
		Tasks: []api.TaskRecord{{
			Spec:    api.TaskSpec{Name: "t", Duration: 2, Resources: api.Resource{CPU: 1, Memory: 1}},
			State:   api.TaskRunning,
			NodeID:  "n1",
			Attempt: 0,
		}},
	}}})

	s := New(Config{StateFile: path, Logger: testLogger(t)})
	if err := s.AddNode("n1", api.Resource{CPU: 4, Memory: 8}); err != nil {
		t.Fatal(err)
	}

	// 重启契约：Recover 在调度循环与任何提交开始前先执行一次。
	if _, err := s.Recover(); err != nil {
		t.Fatal(err)
	}

	// 恢复完成后：多个提交者并发提交互不相同的作业，同时调度循环开始推进，
	// 验证恢复出的作业与新作业都不丢、不重复占用（s.mu 串行化一切状态变更）。
	var wg sync.WaitGroup
	wg.Add(16)
	for i := 0; i < 16; i++ {
		i := i
		go func() {
			defer wg.Done()
			err := s.Submit(api.JobSpec{ID: "new-" + itoa(i), Tasks: []api.TaskSpec{
				{Name: "n", Duration: 1, Resources: api.Resource{CPU: 1, Memory: 1}},
			}})
			if err != nil && !errors.Is(err, api.ErrInsufficientResources) {
				t.Errorf("concurrent submit %d unexpected err: %v", i, err)
			}
		}()
	}
	// 调度与提交并发（模拟恢复完成后立刻启动调度循环）。
	for i := 0; i < 30; i++ {
		s.Tick()
	}
	wg.Wait()
	for i := 0; i < 30; i++ {
		s.Tick()
	}
	old, err := s.GetJob("old")
	if err != nil {
		t.Fatalf("recovered job lost: %v", err)
	}
	if old.State != api.JobSucceeded {
		t.Fatalf("recovered job must eventually succeed, got %s", old.State)
	}
	if old.Tasks[0].Attempt < 1 {
		t.Fatalf("recovered running task should be re-placed at least once: %+v", old.Tasks[0])
	}
	// 所有被接受的新作业也必须恰好成功一次，不存在 Pending/Running 残留。
	for _, jb := range s.store.List() {
		if jb.Spec.ID == "old" {
			continue
		}
		if jb.State != api.JobSucceeded {
			t.Fatalf("accepted job %s must succeed after drain, got %s", jb.Spec.ID, jb.State)
		}
	}
	for _, n := range s.Nodes() {
		if n.Used.CPU != 0 || n.Used.Memory != 0 {
			t.Fatalf("resources leaked after concurrent recovery: %s used=%+v", n.ID, n.Used)
		}
	}
	t.Logf("concurrent recovery preserved job and released all resources: %+v", old.Tasks[0])
}
