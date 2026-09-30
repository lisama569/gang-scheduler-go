package scheduler

import (
	"testing"

	"github.com/lisama569/gang-scheduler-go/pkg/api"
)

// newFourNodeScheduler 构造 n1={6,6} n2={2,2} n3={2,2} n4={2,2}。
func newFourNodeScheduler(t *testing.T) *Scheduler {
	t.Helper()
	s := New(Config{HeartbeatTimeoutTicks: 2, Logger: testLogger(t)})
	for _, spec := range []struct {
		id  string
		res api.Resource
	}{
		{"n1", api.Resource{CPU: 6, Memory: 6}},
		{"n2", api.Resource{CPU: 2, Memory: 2}},
		{"n3", api.Resource{CPU: 2, Memory: 2}},
		{"n4", api.Resource{CPU: 2, Memory: 2}},
	} {
		if err := s.AddNode(spec.id, spec.res); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

// TestDrainMigrationPreservesProgressAndAttempt 验证计划内排空与故障回收
// 是两条严格区分的链路：
//   - 健康节点被主动排空时，其上任务带着已执行进度迁移到健康节点续跑，
//     Attempt 不增长，任务只执行一次（总 tick 数不被拉长）；
//   - 对照：故障回收（心跳超时）的任务 Attempt+1、进度清零从头重跑。
func TestDrainMigrationPreservesProgressAndAttempt(t *testing.T) {
	s := newDrainTestScheduler(t) // n1={6,12} n2={2,4} n3={4,8}

	// 长任务：duration=10。t1->n1，t2->n2。
	mustSubmit(t, s, api.JobSpec{ID: "job", Priority: 1, Tasks: []api.TaskSpec{
		{Name: "t1", Resources: api.Resource{CPU: 5, Memory: 4}, Duration: 10},
		{Name: "t2", Resources: api.Resource{CPU: 2, Memory: 2}, Duration: 10},
	}})
	s.Tick() // 1: t1->n1, t2->n2；tick 末各跑完第 1 拍
	s.Tick() // 2: progress=2
	s.Tick() // 3: progress=3
	j, _ := s.GetJob("job")
	if j.Tasks[1].Progress != 3 || j.Tasks[1].Attempt != 0 {
		t.Fatalf("setup: t2 progress=%d attempt=%d, want 3/0", j.Tasks[1].Progress, j.Tasks[1].Attempt)
	}

	// 此时主动把健康节点 n2 置为维护/排空中。
	if err := s.DrainNode("n2"); err != nil {
		t.Fatal(err)
	}
	info, _ := s.DrainStatus("n2")
	if info.RemainingTasks != 1 || info.RemainingResources != (api.Resource{CPU: 2, Memory: 2}) {
		t.Fatalf("drain status before eviction: %+v", info)
	}
	s.Tick() // 4: t2 被安全驱逐（带着 progress=3）并同 tick 迁到 n3，tick 末再跑一拍

	j, _ = s.GetJob("job")
	t2 := j.Tasks[1]
	if t2.NodeID != "n3" || t2.State != api.TaskRunning {
		t.Fatalf("t2 must migrate to n3: %+v", t2)
	}
	if t2.Attempt != 0 {
		t.Fatalf("planned migration must not increment attempt, got %d", t2.Attempt)
	}
	// 迁移前 progress=3，迁移 tick 在 n3 上再跑一拍 -> 4，绝不是从 0 重来。
	if t2.Progress != 4 {
		t.Fatalf("t2 must resume from progress 3 (got %d), not restart from 0", t2.Progress)
	}
	if info, _ := s.DrainStatus("n2"); info.RemainingTasks != 0 {
		t.Fatalf("n2 must be drained empty: %+v", info)
	}
	if err := s.CompleteNodeDrain("n2"); err != nil {
		t.Fatalf("retire n2: %v", err)
	}

	// t1 从 tick1 起连续跑，到 tick10 完成；t2 带进度续跑，同样在 tick10 完成
	// （迁移没有让它多跑任何一拍）。当前 clock=4，再推进 6 拍到 tick10。
	for i := 0; i < 6; i++ {
		s.Tick()
	}
	j, _ = s.GetJob("job")
	if j.State != api.JobSucceeded {
		t.Fatalf("job must succeed exactly on schedule at tick10: state=%s tasks=%+v", j.State, j.Tasks)
	}
	for _, task := range j.Tasks {
		if task.State != api.TaskSucceeded || task.Progress != 10 {
			t.Fatalf("task %s must succeed once with full progress 10: %+v", task.Spec.Name, task)
		}
		if task.Attempt != 0 {
			t.Fatalf("no failure happened, task %s attempt must stay 0: %d", task.Spec.Name, task.Attempt)
		}
	}
	// 迁移期间 t2 没有被重复执行：执行行最终为空。
	if got := s.exec.Inflight(); got != 0 {
		t.Fatalf("executor must be empty, got %d inflight", got)
	}
	t.Logf("planned migration preserved progress/attempt: %+v %+v", j.Tasks[0], j.Tasks[1])
}

// TestFailureRescheduleSemanticsUnchanged 对照用例：故障链路语义不变——
// 心跳超时杀死的任务 Attempt+1、进度清零从头重跑，作业仍能最终成功。
func TestFailureRescheduleSemanticsUnchanged(t *testing.T) {
	s := newTestScheduler(t) // n1={4,8} n2={2,4}
	mustSubmit(t, s, api.JobSpec{ID: "job", Tasks: []api.TaskSpec{
		{Name: "t1", Resources: api.Resource{CPU: 3, Memory: 4}, Duration: 6},
		{Name: "t2", Resources: api.Resource{CPU: 2, Memory: 3}, Duration: 6},
	}})
	s.Tick() // 1: t1->n1, t2->n2；tick 末 progress=1
	j, _ := s.GetJob("job")
	if j.Tasks[1].Progress != 1 {
		t.Fatalf("setup: t2 progress=%d want 1", j.Tasks[1].Progress)
	}

	s.TriggerNodeFailure("n2") // n2 在 tick1 后停止心跳
	s.Tick()                   // 2
	s.Tick()                   // 3
	s.Tick()                   // 4: now(4)-lastBeat(1)=3>2 -> n2 判死，t2 被杀
	j, _ = s.GetJob("job")
	t2 := j.Tasks[1]
	// 故障语义：Attempt+1、进度清零、回到 Pending（n1 余量不足，本 tick 放不下）。
	if t2.State != api.TaskPending || t2.Attempt != 1 || t2.Progress != 0 || t2.NodeID != "" {
		t.Fatalf("failure must clear progress and bump attempt: %+v", t2)
	}

	// t1 在 tick6 完成释放 n1，t2 随后从 n1 上从头重跑完整的 6 拍。
	for i := 0; i < 9; i++ {
		s.Tick()
	}
	j, _ = s.GetJob("job")
	if j.State != api.JobSucceeded {
		t.Fatalf("job must succeed after failure reschedule: %s", j.State)
	}
	if j.Tasks[1].NodeID != "n1" || j.Tasks[1].Attempt != 1 || j.Tasks[1].Progress != 6 {
		t.Fatalf("failed task reruns full duration once on n1: %+v", j.Tasks[1])
	}
	t.Logf("failure semantics unchanged: t2=%+v", j.Tasks[1])
}

// TestDrainThenMigrateAgainProgressKept 任务可以连续经历多台节点排空：
// 进度始终累加、Attempt 始终为 0，最终只跑满一个 Duration。
// 拓扑：t1(6,2) 长期占满 n1 的 CPU；t2(2,2) 在 n2 ->(排空)-> n3 ->(排空)-> n4。
func TestDrainThenMigrateAgainProgressKept(t *testing.T) {
	s := newFourNodeScheduler(t)
	mustSubmit(t, s, api.JobSpec{ID: "job", Tasks: []api.TaskSpec{
		{Name: "t1", Resources: api.Resource{CPU: 6, Memory: 2}, Duration: 12},
		{Name: "t2", Resources: api.Resource{CPU: 2, Memory: 2}, Duration: 12},
	}})
	s.Tick() // 1: t1->n1（n1 CPU 占满）, t2->n2；tick 末 progress=1
	j, _ := s.GetJob("job")
	if j.Tasks[0].NodeID != "n1" || j.Tasks[1].NodeID != "n2" {
		t.Fatalf("unexpected initial placement: %+v", j.Tasks)
	}

	// 第一次排空：n2 -> n3（n1 CPU 已满，放不下 t2）。
	if err := s.DrainNode("n2"); err != nil {
		t.Fatal(err)
	}
	s.Tick() // 2: t2 带 progress=1 迁到 n3 并再跑一拍 -> 2
	if err := s.CompleteNodeDrain("n2"); err != nil {
		t.Fatalf("retire n2: %v", err)
	}
	j, _ = s.GetJob("job")
	if j.Tasks[1].NodeID != "n3" || j.Tasks[1].Progress != 2 || j.Tasks[1].Attempt != 0 {
		t.Fatalf("first migration n2->n3: %+v", j.Tasks[1])
	}

	// 第二次排空：n3 -> n4，进度继续累加。
	if err := s.DrainNode("n3"); err != nil {
		t.Fatal(err)
	}
	s.Tick() // 3: t2 带 progress=2 迁到 n4 并再跑一拍 -> 3
	if err := s.CompleteNodeDrain("n3"); err != nil {
		t.Fatalf("retire n3: %v", err)
	}
	j, _ = s.GetJob("job")
	if j.Tasks[1].NodeID != "n4" || j.Tasks[1].Progress != 3 || j.Tasks[1].Attempt != 0 {
		t.Fatalf("second migration n3->n4 must accumulate progress: %+v", j.Tasks[1])
	}

	// 跑完剩余 9 拍（tick4..tick12），t2 总执行量恰好 12，没有重跑。
	for i := 0; i < 9; i++ {
		s.Tick()
	}
	j, _ = s.GetJob("job")
	if j.State != api.JobSucceeded {
		t.Fatalf("job must succeed after repeated migrations: %s (%+v)", j.State, j.Tasks)
	}
	if j.Tasks[1].Progress != 12 || j.Tasks[1].Attempt != 0 {
		t.Fatalf("t2 must execute exactly one full duration: %+v", j.Tasks[1])
	}
	t.Logf("double migration: t2 final node=%s progress=%d attempt=%d",
		j.Tasks[1].NodeID, j.Tasks[1].Progress, j.Tasks[1].Attempt)
}
