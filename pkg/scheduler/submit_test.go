package scheduler

import (
	"errors"
	"testing"

	"github.com/lisama569/gang-scheduler-go/pkg/api"
)

// newTestScheduler 构造带测试日志的两节点调度器。
func newTestScheduler(t *testing.T) *Scheduler {
	t.Helper()
	s := New(Config{
		HeartbeatTimeoutTicks: 2,
		Logger:                testLogger(t),
	})
	if err := s.AddNode("n1", api.Resource{CPU: 4, Memory: 8}); err != nil {
		t.Fatalf("add n1: %v", err)
	}
	if err := s.AddNode("n2", api.Resource{CPU: 2, Memory: 4}); err != nil {
		t.Fatalf("add n2: %v", err)
	}
	return s
}

func TestSubmitInvalid(t *testing.T) {
	s := newTestScheduler(t)

	cases := []struct {
		name string
		spec api.JobSpec
		want string
	}{
		{"empty id", api.JobSpec{Tasks: []api.TaskSpec{{Name: "a", Duration: 1, Resources: api.Resource{CPU: 1}}}}, "empty job id"},
		{"no tasks", api.JobSpec{ID: "j"}, "no tasks"},
		{"empty task name", api.JobSpec{ID: "j", Tasks: []api.TaskSpec{{Duration: 1}}}, "empty name"},
		{"dup task name", api.JobSpec{ID: "j", Tasks: []api.TaskSpec{
			{Name: "a", Duration: 1}, {Name: "a", Duration: 1}}}, "duplicate task name"},
		{"negative resource", api.JobSpec{ID: "j", Tasks: []api.TaskSpec{
			{Name: "a", Duration: 1, Resources: api.Resource{CPU: -1}}}}, "negative resource"},
		{"non-positive duration", api.JobSpec{ID: "j", Tasks: []api.TaskSpec{
			{Name: "a", Duration: 0, Resources: api.Resource{CPU: 1}}}}, "duration must be positive"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := s.Submit(c.spec)
			if !errors.Is(err, api.ErrInvalidRequest) {
				t.Fatalf("want ErrInvalidRequest, got %v", err)
			}
			if !contains(err.Error(), c.want) {
				t.Fatalf("error %q does not mention %q", err.Error(), c.want)
			}
		})
	}

	// 非法作业不得入队。
	if jobs := s.store.List(); len(jobs) != 0 {
		t.Fatalf("invalid jobs must not be queued, got %d", len(jobs))
	}
}

func TestSubmitInsufficient(t *testing.T) {
	s := newTestScheduler(t)

	// 单任务超过最大单节点容量。
	err := s.Submit(api.JobSpec{ID: "too-big", Tasks: []api.TaskSpec{
		{Name: "x", Duration: 1, Resources: api.Resource{CPU: 5, Memory: 1}},
	}})
	if !errors.Is(err, api.ErrInsufficientResources) {
		t.Fatalf("single-node overflow: want ErrInsufficientResources, got %v", err)
	}
	t.Logf("oversized task rejected: %v", err)

	// 总量超过集群健康容量之和。
	err = s.Submit(api.JobSpec{ID: "sum-too-big", Tasks: []api.TaskSpec{
		{Name: "a", Duration: 1, Resources: api.Resource{CPU: 4, Memory: 8}},
		{Name: "b", Duration: 1, Resources: api.Resource{CPU: 3, Memory: 1}},
	}})
	if !errors.Is(err, api.ErrInsufficientResources) {
		t.Fatalf("capacity-sum overflow: want ErrInsufficientResources, got %v", err)
	}
	t.Logf("capacity-sum overflow rejected: %v", err)
}

func TestSubmitDuplicate(t *testing.T) {
	s := newTestScheduler(t)
	spec := api.JobSpec{ID: "dup", Tasks: []api.TaskSpec{
		{Name: "a", Duration: 1, Resources: api.Resource{CPU: 1}},
	}}
	if err := s.Submit(spec); err != nil {
		t.Fatalf("first submit: %v", err)
	}
	err := s.Submit(spec)
	if !errors.Is(err, api.ErrDuplicateJob) {
		t.Fatalf("want ErrDuplicateJob, got %v", err)
	}
	t.Logf("duplicate submit rejected: %v", err)

	// 让作业跑完后再次提交：已结束作业不得复活，仍按重复拒绝。
	s.Tick()
	s.Tick()
	j, _ := s.GetJob("dup")
	if j.State != api.JobSucceeded {
		t.Fatalf("setup: job should succeed, got %s", j.State)
	}
	if err := s.Submit(spec); !errors.Is(err, api.ErrDuplicateJob) {
		t.Fatalf("resubmit finished job must be ErrDuplicateJob, got %v", err)
	}
}

// TestQueueAndPriority 验证资源暂时不足时排队、释放后按优先级调度。
func TestQueueAndPriority(t *testing.T) {
	// 三节点：n1=4cpu, n2=2cpu, n3=2cpu。
	s := newTestScheduler(t)
	if err := s.AddNode("n3", api.Resource{CPU: 2, Memory: 4}); err != nil {
		t.Fatalf("add n3: %v", err)
	}

	// big 先独占 n1 并长时间运行。
	mustSubmit(t, s, api.JobSpec{ID: "big", Priority: 1, Tasks: []api.TaskSpec{
		{Name: "b", Duration: 4, Resources: api.Resource{CPU: 4, Memory: 1}},
	}})
	s.Tick()
	assertJobState(t, s, "big", api.JobRunning)

	// big 运行期间再提交同样需要整台 n1 的 low/high；二者必须排队等待。
	mustSubmit(t, s, api.JobSpec{ID: "low", Priority: 1, Tasks: []api.TaskSpec{
		{Name: "l", Duration: 2, Resources: api.Resource{CPU: 4, Memory: 1}},
	}})
	mustSubmit(t, s, api.JobSpec{ID: "high", Priority: 10, Tasks: []api.TaskSpec{
		{Name: "h", Duration: 2, Resources: api.Resource{CPU: 4, Memory: 1}},
	}})

	s.Tick() // 2
	s.Tick() // 3
	assertJobState(t, s, "high", api.JobPending)
	assertJobState(t, s, "low", api.JobPending)
	s.Tick() // 4：big 完成，资源在 tick 末释放
	s.Tick() // 5：high 必须先获得 n1
	high, _ := s.GetJob("high")
	low, _ := s.GetJob("low")
	if high.State != api.JobRunning || high.Tasks[0].NodeID != "n1" {
		t.Fatalf("high must be scheduled first on n1: %+v", high)
	}
	if low.State != api.JobPending {
		t.Fatalf("low must stay pending, got %s", low.State)
	}
	s.Tick() // 6：high 完成，low 启动
	s.Tick() // 7：low 运行中
	s.Tick() // 8：low 完成
	high, _ = s.GetJob("high")
	low, _ = s.GetJob("low")
	if high.State != api.JobSucceeded || low.State != api.JobSucceeded {
		t.Fatalf("both queued jobs should finish: high=%s low=%s", high.State, low.State)
	}
	t.Logf("priority ordering verified: high node=%s, low node=%s",
		high.Tasks[0].NodeID, low.Tasks[0].NodeID)
}

func mustSubmit(t *testing.T, s *Scheduler, spec api.JobSpec) {
	t.Helper()
	if err := s.Submit(spec); err != nil {
		t.Fatalf("submit %s: %v", spec.ID, err)
	}
}

func assertJobState(t *testing.T, s *Scheduler, id, want string) {
	t.Helper()
	j, err := s.GetJob(id)
	if err != nil {
		t.Fatalf("get %s: %v", id, err)
	}
	if j.State != want {
		t.Fatalf("job %s state=%s want %s", id, j.State, want)
	}
}

func contains(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
