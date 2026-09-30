package scheduler

import (
	"errors"
	"testing"

	"github.com/lisama569/gang-scheduler-go/pkg/api"
)

// newProgressScheduler 构造专用拓扑：n1={8,16} n2={8,16}（两台等价大节点）。
func newProgressScheduler(t *testing.T) *Scheduler {
	t.Helper()
	s := New(Config{HeartbeatTimeoutTicks: 2, Logger: testLogger(t)})
	if err := s.AddNode("n1", api.Resource{CPU: 8, Memory: 16}); err != nil {
		t.Fatal(err)
	}
	if err := s.AddNode("n2", api.Resource{CPU: 8, Memory: 16}); err != nil {
		t.Fatal(err)
	}
	return s
}

// TestDrainMigrationPreservesProgressAndAttempt 计划内维护迁移必须：
//   - 已执行节拍（进度）随任务带到新节点，任务接着原进度跑完，而不是从头重跑；
//   - Attempt 不增加（迁移不是重试）；
//   - 作业状态在迁移前后自洽（Running，不经过 Failed）。
func TestDrainMigrationPreservesProgressAndAttempt(t *testing.T) {
	s := newProgressScheduler(t)

	mustSubmit(t, s, api.JobSpec{ID: "job", Tasks: []api.TaskSpec{
		{Name: "t", Resources: api.Resource{CPU: 2, Memory: 2}, Duration: 6},
	}})
	s.Tick() // 1: t->n1，tick 末执行 1 拍（剩 5）
	j, _ := s.GetJob("job")
	if j.Tasks[0].NodeID != "n1" || j.State != api.JobRunning {
		t.Fatalf("setup unexpected: %+v state=%s", j.Tasks[0], j.State)
	}

	if err := s.DrainNode("n1"); err != nil {
		t.Fatal(err)
	}
	s.Tick() // 2: t 被安全驱逐（剩 5）-> 同 tick 迁到 n2，带着 1 拍进度继续
	j, _ = s.GetJob("job")
	if j.Tasks[0].State != api.TaskRunning || j.Tasks[0].NodeID != "n2" {
		t.Fatalf("task must migrate to n2 and keep running: %+v", j.Tasks[0])
	}
	if j.Tasks[0].Attempt != 0 {
		t.Fatalf("planned migration must NOT increment attempt, got %d", j.Tasks[0].Attempt)
	}
	if j.Tasks[0].Progress != 1 {
		t.Fatalf("done ticks must be carried over, got Progress=%d want 1", j.Tasks[0].Progress)
	}
	if j.State != api.JobRunning {
		t.Fatalf("job must stay Running across planned migration, got %s", j.State)
	}
	// 排空中节点不再承载任务：executor 里的在执行行只应挂在 n2。
	if !s.exec.Running("job", "t") {
		t.Fatal("task must be running in executor after migration")
	}

	// tick2 迁移时 tick 末再推进一拍（n2 上第 1 拍，累计 2）；
	// 到 tick6 恰好累计 6 拍完成 —— 若从头重跑要到 tick7 才完成。
	for i := 0; i < 4; i++ { // tick 3,4,5,6
		s.Tick()
		j, _ = s.GetJob("job")
		if i < 3 && j.State == api.JobSucceeded {
			t.Fatalf("task finished too early at sub-step %d (progress lost?), state=%s", i, j.State)
		}
	}
	j, _ = s.GetJob("job")
	if j.State != api.JobSucceeded {
		t.Fatalf("job must finish exactly at tick 6 using carried progress, got %s (%+v)",
			j.State, j.Tasks[0])
	}
	if j.Tasks[0].Attempt != 0 {
		t.Fatalf("no retry may be counted for a planned migration: attempt=%d", j.Tasks[0].Attempt)
	}
	if j.Tasks[0].State != api.TaskSucceeded {
		t.Fatalf("task must succeed once, got %+v", j.Tasks[0])
	}
	info, _ := s.DrainStatus("n1")
	if info.RemainingTasks != 0 {
		t.Fatalf("draining node must have no remaining task: %+v", info)
	}
	if err := s.CompleteNodeDrain("n1"); err != nil {
		t.Fatalf("retire emptied draining node: %v", err)
	}
	t.Logf("progress preserved across drain: final task=%+v", j.Tasks[0])
}

// TestFailureRescheduleRunsFromStartAndCountsAttempt 与上一个用例形成对照：
// 节点故障回收必须从头重跑且 Attempt+1，证明“计划内维护”和“故障”两条链路语义不同。
func TestFailureRescheduleRunsFromStartAndCountsAttempt(t *testing.T) {
	s := newProgressScheduler(t)

	mustSubmit(t, s, api.JobSpec{ID: "job", Tasks: []api.TaskSpec{
		{Name: "t", Resources: api.Resource{CPU: 2, Memory: 2}, Duration: 6},
	}})
	s.Tick() // 1: t->n1
	s.TriggerNodeFailure("n1")
	s.Tick() // 2
	s.Tick() // 3
	s.Tick() // 4: n1 超时判死，t 被杀复位（Attempt=1、Progress=0）后同 tick 重放到 n2
	j, _ := s.GetJob("job")
	if j.Tasks[0].State != api.TaskRunning || j.Tasks[0].NodeID != "n2" {
		t.Fatalf("task must be relaunched on n2 within the reclaim tick: %+v", j.Tasks[0])
	}
	if j.Tasks[0].Attempt != 1 {
		t.Fatalf("failure must count an attempt, got %d", j.Tasks[0].Attempt)
	}
	if j.Tasks[0].Progress != 0 {
		t.Fatalf("failure loses execution site: progress must be 0, got %d", j.Tasks[0].Progress)
	}
	// 从头重跑：tick4 末在 n2 跑完第 1 拍，要到 tick9 才累计满 6 拍完成。
	for i := 0; i < 4; i++ { // tick 5..8
		s.Tick()
		j, _ = s.GetJob("job")
		if j.State == api.JobSucceeded {
			t.Fatalf("task must NOT finish at tick %d after a from-scratch rerun", 4+i)
		}
	}
	s.Tick() // tick 9: 从头跑完 6 拍
	j, _ = s.GetJob("job")
	if j.State != api.JobSucceeded {
		t.Fatalf("job must succeed after a full re-run: %s (%+v)", j.State, j.Tasks[0])
	}
	if j.Tasks[0].Attempt != 1 {
		t.Fatalf("failure path must record one attempt: %+v", j.Tasks[0])
	}
	t.Logf("failure semantics verified (from scratch, attempt counted): %+v", j.Tasks[0])
}

// TestRetiredNodeSurvivesRestart 已正式退役（排空完成下线）的节点必须扛住重启：
//   - 重启后重新注册同名节点仍是 Dead，不会被当成健康机器复活；
//   - 收缩后的容量不会涨回：超容量提交仍被 ErrInsufficientResources 拒绝，
//     能放下的作业仍可在剩余健康节点上正常调度成功。
func TestRetiredNodeSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/snapshot.json"

	s1 := New(Config{HeartbeatTimeoutTicks: 2, StateFile: path, Logger: testLogger(t)})
	if err := s1.AddNode("n1", api.Resource{CPU: 4, Memory: 8}); err != nil {
		t.Fatal(err)
	}
	if err := s1.AddNode("n2", api.Resource{CPU: 2, Memory: 4}); err != nil {
		t.Fatal(err)
	}
	// n2 无任务：排空 -> 退役（容量从 {6,12} 收缩为 {4,8}）。
	if err := s1.DrainNode("n2"); err != nil {
		t.Fatal(err)
	}
	if err := s1.CompleteNodeDrain("n2"); err != nil {
		t.Fatal(err)
	}
	if got := nodeState(t, s1, "n2").State; got != api.NodeDead {
		t.Fatalf("n2 should be retired, got %s", got)
	}

	// “进程重启”：新调度器，并在 Recover 之前就把节点都注册回来。
	s2 := New(Config{HeartbeatTimeoutTicks: 2, StateFile: path, Logger: testLogger(t)})
	if err := s2.AddNode("n1", api.Resource{CPU: 4, Memory: 8}); err != nil {
		t.Fatal(err)
	}
	if err := s2.AddNode("n2", api.Resource{CPU: 2, Memory: 4}); err != nil {
		t.Fatalf("re-registering a retired node id should be accepted but kept dead: %v", err)
	}
	if _, err := s2.Recover(); err != nil {
		t.Fatal(err)
	}
	n2 := nodeState(t, s2, "n2")
	if n2.State != api.NodeDead {
		t.Fatalf("retired n2 must stay Dead after restart+re-register, got %s", n2.State)
	}
	if n2.Used != (api.Resource{}) {
		t.Fatalf("dead node must hold no resources: %+v", n2)
	}

	// 容量保持收缩：总量 {5,?} 的作业仍超过健康容量（仅 n1={4,8}）。
	err := s2.Submit(api.JobSpec{ID: "too-big", Tasks: []api.TaskSpec{
		{Name: "a", Resources: api.Resource{CPU: 3, Memory: 2}, Duration: 1},
		{Name: "b", Resources: api.Resource{CPU: 2, Memory: 2}, Duration: 1},
	}})
	if err == nil || !errors.Is(err, api.ErrInsufficientResources) {
		t.Fatalf("job over shrunken capacity must be rejected after restart, got %v", err)
	}
	// 单任务 {4,...} 仍可被唯一健康节点 n1 接收并跑成功。
	mustSubmit(t, s2, api.JobSpec{ID: "fits", Tasks: []api.TaskSpec{
		{Name: "a", Resources: api.Resource{CPU: 4, Memory: 4}, Duration: 2},
	}})
	s2.Tick()
	s2.Tick()
	j, _ := s2.GetJob("fits")
	if j.State != api.JobSucceeded || j.Tasks[0].NodeID != "n1" {
		t.Fatalf("fitting job must run on the remaining healthy n1: %s %+v", j.State, j.Tasks[0])
	}
	// 退役节点从头到尾不能收到任务。
	if j.Tasks[0].NodeID == "n2" {
		t.Fatal("task must never be placed on retired node")
	}
	t.Log("retired node stayed dead across restart and capacity stayed shrunken")
}

// TestRetiredNodeReRegisteredBeforeRecoverStaysDead 覆盖另一种注册顺序：
// Recover 之后再注册同名退役节点，仍必须立即置 Dead。
func TestRetiredNodeReRegisteredAfterRecoverStaysDead(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/snapshot.json"

	s1 := New(Config{HeartbeatTimeoutTicks: 2, StateFile: path, Logger: testLogger(t)})
	if err := s1.AddNode("n1", api.Resource{CPU: 4, Memory: 8}); err != nil {
		t.Fatal(err)
	}
	if err := s1.AddNode("n2", api.Resource{CPU: 4, Memory: 8}); err != nil {
		t.Fatal(err)
	}
	if err := s1.DrainNode("n2"); err != nil {
		t.Fatal(err)
	}
	if err := s1.CompleteNodeDrain("n2"); err != nil {
		t.Fatal(err)
	}

	s2 := New(Config{HeartbeatTimeoutTicks: 2, StateFile: path, Logger: testLogger(t)})
	if err := s2.AddNode("n1", api.Resource{CPU: 4, Memory: 8}); err != nil {
		t.Fatal(err)
	}
	if _, err := s2.Recover(); err != nil {
		t.Fatal(err)
	}
	// Recover 之后才注册 n2（同名退役节点）。
	if err := s2.AddNode("n2", api.Resource{CPU: 4, Memory: 8}); err != nil {
		t.Fatalf("add retired id should be accepted but kept dead: %v", err)
	}
	if got := nodeState(t, s2, "n2").State; got != api.NodeDead {
		t.Fatalf("late-registered retired node must be Dead immediately, got %s", got)
	}
	// 跑若干 tick：迟到心跳/调度都不能让它复活。
	for i := 0; i < 5; i++ {
		s2.Tick()
	}
	if got := nodeState(t, s2, "n2").State; got != api.NodeDead {
		t.Fatalf("retired node resurrected during ticks: %s", got)
	}
	// {5,...} 单任务超过仅剩 n1 的单节点容量 -> 明确拒绝。
	err := s2.Submit(api.JobSpec{ID: "big", Tasks: []api.TaskSpec{
		{Name: "x", Resources: api.Resource{CPU: 5, Memory: 1}, Duration: 1},
	}})
	if err == nil || !errors.Is(err, api.ErrInsufficientResources) {
		t.Fatalf("capacity must stay withdrawn: %v", err)
	}
	t.Log("retired node registered after Recover stays dead and capacity shrunken")
}

// TestDrainQueuedMigrationKeepsProgressAndAttempt 被安全驱逐的任务若一时
// 放不下健康节点，会以 Pending 排队等待；排队期间进度不丢、Attempt 不涨，
// 容量释放后仍按剩余节拍接着跑完（不会从头跑、不会被判失败）。
func TestDrainQueuedMigrationKeepsProgressAndAttempt(t *testing.T) {
	s := New(Config{HeartbeatTimeoutTicks: 2, Logger: testLogger(t)})
	// 确定性拓扑（按节点 ID first-fit）：
	//   na={4,..}：occ 的 o(4) 先单独占住，长期不释放；
	//   nb={3,..}：blocker 的 k(3) 落这里；
	//   nc={1,..}：放不下任何相关任务；
	//   nd={4,..}：t(4) 的初始节点，排空后健康节点里再无 4cpu 余量 -> t 只能排队。
	for _, spec := range []struct {
		id  string
		cpu int
	}{{"na", 4}, {"nb", 3}, {"nc", 1}, {"nd", 4}} {
		if err := s.AddNode(spec.id, api.Resource{CPU: spec.cpu, Memory: 8}); err != nil {
			t.Fatal(err)
		}
	}
	// 先让 occ 单独占住排序最早的 4cpu 节点 na（长时间运行）。
	mustSubmit(t, s, api.JobSpec{ID: "occ", Priority: 9, Tasks: []api.TaskSpec{
		{Name: "o", Resources: api.Resource{CPU: 4, Memory: 1}, Duration: 12},
	}})
	s.Tick() // 0->1: o->na
	if jb, _ := s.GetJob("occ"); jb.Tasks[0].NodeID != "na" {
		t.Fatalf("setup: occ must land on na, got %+v", jb.Tasks[0])
	}
	// blocker 的 k(3)：na 满、nb=3 可落；t(4)：只能落 nd。
	mustSubmit(t, s, api.JobSpec{ID: "blocker", Priority: 10, Tasks: []api.TaskSpec{
		{Name: "k", Resources: api.Resource{CPU: 3, Memory: 1}, Duration: 4},
	}})
	mustSubmit(t, s, api.JobSpec{ID: "job", Priority: 20, Tasks: []api.TaskSpec{
		{Name: "t", Resources: api.Resource{CPU: 4, Memory: 1}, Duration: 6},
	}})
	s.Tick() // 1->2: k->nb, t->nd
	j, _ := s.GetJob("job")
	if j.Tasks[0].NodeID != "nd" {
		t.Fatalf("setup: t should be on nd, got %+v", j.Tasks[0])
	}

	// 排空 nd：t 被安全驱逐（已跑 1 拍，剩 5），但 na 被 occ 占住、
	// nb/nc 都放不下 4cpu -> Pending 排队。
	if err := s.DrainNode("nd"); err != nil {
		t.Fatal(err)
	}
	s.Tick() // 绝对 tick3: 驱逐 -> 排队
	j, _ = s.GetJob("job")
	if j.Tasks[0].State != api.TaskPending || j.Tasks[0].NodeID != "" {
		t.Fatalf("t should be queued pending migration, got %+v", j.Tasks[0])
	}
	if j.Tasks[0].Attempt != 0 || j.Tasks[0].Progress != 1 {
		t.Fatalf("queued migration must keep attempt=0 and progress=1: %+v", j.Tasks[0])
	}
	if j.State != api.JobPending {
		t.Fatalf("job with only task waiting should be Pending, got %s", j.State)
	}

	// blocker 即使先在 nb 结束也救不了 t：健康节点里只有 na 放得下 4cpu，
	// 而 occ 从绝对 tick1 起要跑 12 拍（tick13 末才释放）。
	// t 在 tick13 的调度阶段前必须一直排队，期间进度(=1)/尝试次数(=0)保持不变。
	for tick := 4; tick <= 13; tick++ {
		s.Tick()
		j, _ = s.GetJob("job")
		if tick < 13 && j.Tasks[0].State != api.TaskPending {
			t.Fatalf("t must keep waiting while na is occupied, tick %d: %+v", tick, j.Tasks[0])
		}
		if j.Tasks[0].Attempt != 0 || j.Tasks[0].Progress != 1 {
			t.Fatalf("waiting must preserve progress=1/attempt=0 at tick %d: %+v", tick, j.Tasks[0])
		}
	}
	s.Tick() // 14: na 已空，t 带 1 拍进度重新启动到 na
	j, _ = s.GetJob("job")
	if j.Tasks[0].State != api.TaskRunning || j.Tasks[0].NodeID != "na" {
		t.Fatalf("t should relaunch on freed na at tick14: %+v", j.Tasks[0])
	}
	if j.Tasks[0].Attempt != 0 || j.Tasks[0].Progress != 1 {
		t.Fatalf("relaunched task keeps progress and zero attempt: %+v", j.Tasks[0])
	}
	// tick14 末跑 1 拍（剩 4），再 4 拍到 tick18 累计满 6 拍完成；
	// 若错误地从头跑，则要到 tick19 才完成。
	for i := 0; i < 4; i++ { // tick 15..18
		s.Tick()
	}
	j, _ = s.GetJob("job")
	if j.State != api.JobSucceeded {
		t.Fatalf("job must finish using preserved progress after waiting: %s (%+v)",
			j.State, j.Tasks[0])
	}
	if j.Tasks[0].Attempt != 0 {
		t.Fatalf("no attempt may be counted: %+v", j.Tasks[0])
	}
	t.Logf("queued migration preserved progress across waiting: %+v", j.Tasks[0])
}
