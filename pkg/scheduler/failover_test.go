package scheduler

import (
	"testing"

	"github.com/lisama569/gang-scheduler-go/pkg/api"
)

// TestNodeFailureReschedule 验证：运行中的任务所在节点心跳超时后，
// 任务被复位并在其他健康节点重新执行；作业最终成功。
func TestNodeFailureReschedule(t *testing.T) {
	s := newTestScheduler(t) // n1=4cpu8mem, n2=2cpu4mem

	mustSubmit(t, s, api.JobSpec{ID: "job", Priority: 1, Tasks: []api.TaskSpec{
		{Name: "t1", Resources: api.Resource{CPU: 3, Memory: 4}, Duration: 6},
		{Name: "t2", Resources: api.Resource{CPU: 2, Memory: 3}, Duration: 6},
	}})

	s.Tick() // 1: 放置 t1->n1, t2->n2
	j, _ := s.GetJob("job")
	if j.Tasks[0].NodeID != "n1" || j.Tasks[1].NodeID != "n2" {
		t.Fatalf("unexpected initial placement: %+v", j.Tasks)
	}

	s.TriggerNodeFailure("n2") // n2 停止心跳（此时 t2 仍在运行，duration=6）
	s.Tick()                   // 2
	s.Tick()                   // 3
	s.Tick()                   // 4: now(4)-lastBeat(1)=3 > timeout(2) -> 判死
	j, _ = s.GetJob("job")
	if j.Tasks[1].State != api.TaskPending || j.Tasks[1].NodeID != "" {
		t.Fatalf("t2 should be reset to Pending after node death: %+v", j.Tasks[1])
	}
	if j.Tasks[1].Attempt != 1 {
		t.Fatalf("t2 attempt should be 1, got %d", j.Tasks[1].Attempt)
	}
	// t1 仍在 n1 运行，占用 3cpu；n1 仅剩 1cpu，t2(2cpu) 当 tick 放不下。
	if j.Tasks[0].State != api.TaskRunning {
		t.Fatalf("t1 must keep running on healthy n1: %+v", j.Tasks[0])
	}

	s.Tick() // 5: t2 仍排队（n1 余量不足）
	j, _ = s.GetJob("job")
	if j.Tasks[1].State != api.TaskPending {
		t.Fatalf("t2 should wait until capacity frees, got %s", j.Tasks[1].State)
	}

	for i := 0; i < 8; i++ {
		s.Tick() // t1 完成释放 n1 -> t2 重新放置到 n1 并执行完成
	}
	j, _ = s.GetJob("job")
	if j.State != api.JobSucceeded {
		t.Fatalf("job must succeed after reschedule: %s (%+v)", j.State, j.Tasks)
	}
	if j.Tasks[1].NodeID != "n1" || j.Tasks[1].Attempt != 1 {
		t.Fatalf("t2 must have run once more on n1: %+v", j.Tasks[1])
	}
	t.Logf("failover verified: t1=%+v t2=%+v", j.Tasks[0], j.Tasks[1])
}

// TestCompletedTaskNotRerun 验证：任务已完成后节点才被判定死亡，
// 已完成任务不得重复执行，attempt 不增加。
func TestCompletedTaskNotRerun(t *testing.T) {
	s := newTestScheduler(t)
	mustSubmit(t, s, api.JobSpec{ID: "job", Tasks: []api.TaskSpec{
		{Name: "t1", Resources: api.Resource{CPU: 2, Memory: 1}, Duration: 2},
	}})
	s.Tick() // 1: 启动在 n1（n1<n2 按 ID 排序）
	s.Tick() // 2: 成功
	j, _ := s.GetJob("job")
	if j.State != api.JobSucceeded || j.Tasks[0].State != api.TaskSucceeded {
		t.Fatalf("setup: job should succeed: %+v", j)
	}

	s.TriggerNodeFailure("n1")
	s.Tick() // 3
	s.Tick() // 4: n1 超时被判死
	s.Tick() // 5
	j, _ = s.GetJob("job")
	if j.State != api.JobSucceeded {
		t.Fatalf("finished job must stay succeeded: %s", j.State)
	}
	if j.Tasks[0].State != api.TaskSucceeded || j.Tasks[0].Attempt != 0 {
		t.Fatalf("completed task must not be re-run: %+v", j.Tasks[0])
	}
	if s.exec.Running("job", "t1") {
		t.Fatalf("completed task must not exist in executor inflight set")
	}
	t.Logf("completed task preserved across node death: %+v", j.Tasks[0])
}

// TestTaskFailureFailsJob 验证：任一任务执行失败 -> 作业 Failed 且原因可区分，
// 其余运行中任务结束后作业终态确定。
func TestTaskFailureFailsJob(t *testing.T) {
	s := newTestScheduler(t)
	mustSubmit(t, s, api.JobSpec{ID: "job", Tasks: []api.TaskSpec{
		{Name: "t1", Resources: api.Resource{CPU: 1, Memory: 1}, Duration: 5},
		{Name: "t2", Resources: api.Resource{CPU: 1, Memory: 1}, Duration: 5},
	}})
	s.Tick()
	if !s.InjectTaskFailure("job", "t1") {
		t.Fatal("inject failure should succeed for running task")
	}
	j, _ := s.GetJob("job")
	if j.State != api.JobFailed || j.Reason != "task failed: t1" {
		t.Fatalf("job should fail with task reason: state=%s reason=%q", j.State, j.Reason)
	}
	// 再注入一次必须幂等：任务已 Failed，不存在重复失败事件。
	if s.InjectTaskFailure("job", "t1") {
		t.Fatal("second failure injection on finished task must be rejected")
	}
	t.Logf("job failed with distinguishable reason: %q", j.Reason)
}

// TestResourceAccountingInvariant 在整段故障-恢复过程中反复校验节点账本：
// 所有在执行行的资源请求之和 == 节点 Used 之和；终态后全部归零。
func TestResourceAccountingInvariant(t *testing.T) {
	s := newTestScheduler(t)
	if err := s.AddNode("n3", api.Resource{CPU: 4, Memory: 8}); err != nil {
		t.Fatalf("add n3: %v", err)
	}
	mustSubmit(t, s, api.JobSpec{ID: "j1", Tasks: []api.TaskSpec{
		{Name: "a", Resources: api.Resource{CPU: 3, Memory: 2}, Duration: 7},
		{Name: "b", Resources: api.Resource{CPU: 2, Memory: 2}, Duration: 7},
	}})

	s.Tick()
	s.TriggerNodeFailure("n2")
	for tick := 0; tick < 14; tick++ {
		s.Tick()
		assertLedger(t, s)
	}
	j, _ := s.GetJob("j1")
	if j.State != api.JobSucceeded {
		t.Fatalf("j1 final state=%s want Succeeded", j.State)
	}
	for _, n := range s.Nodes() {
		if n.State == api.NodeHealthy && (n.Used.CPU != 0 || n.Used.Memory != 0) {
			t.Fatalf("healthy node %s used not zero: %+v", n.ID, n.Used)
		}
	}
	t.Log("ledger invariant held across failure and reschedule")
}

func assertLedger(t *testing.T, s *Scheduler) {
	t.Helper()
	var usedCPU, usedMem int
	for _, n := range s.Nodes() {
		if n.Used.CPU < 0 || n.Used.Memory < 0 {
			t.Fatalf("negative usage on %s: %+v", n.ID, n.Used)
		}
		if n.Used.CPU > n.Capacity.CPU || n.Used.Memory > n.Capacity.Memory {
			t.Fatalf("overcommit on %s: used=%+v cap=%+v", n.ID, n.Used, n.Capacity)
		}
		if n.State == api.NodeHealthy {
			usedCPU += n.Used.CPU
			usedMem += n.Used.Memory
		}
	}
	// executor 在执行行的请求总和必须与健康节点账本一致。
	var inflightCPU, inflightMem int
	for _, j := range s.store.List() {
		for _, task := range j.Tasks {
			if task.State == api.TaskRunning && s.exec.Running(j.Spec.ID, task.Spec.Name) {
				inflightCPU += task.Spec.Resources.CPU
				inflightMem += task.Spec.Resources.Memory
			}
		}
	}
	if inflightCPU != usedCPU || inflightMem != usedMem {
		t.Fatalf("ledger mismatch: inflight={%d,%d} healthyUsed={%d,%d}",
			inflightCPU, inflightMem, usedCPU, usedMem)
	}
}
