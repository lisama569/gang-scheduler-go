package scheduler

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/lisama569/gang-scheduler-go/pkg/api"
)

// nodeOf 返回某任务当前所在节点。
func nodeOf(t *testing.T, s *Scheduler, job, task string) api.TaskRuntime {
	t.Helper()
	j, err := s.GetJob(job)
	if err != nil {
		t.Fatalf("get job %s: %v", job, err)
	}
	for _, tt := range j.Tasks {
		if tt.Spec.Name == task {
			return tt
		}
	}
	t.Fatalf("task %s not found in %s", task, job)
	return api.TaskRuntime{}
}

func findNode(s *Scheduler, id string) api.NodeRuntime {
	for _, n := range s.Nodes() {
		if n.ID == id {
			return n
		}
	}
	return api.NodeRuntime{ID: id, State: "<missing>"}
}

// TestDrainLiveMigratesTasksAndRetires 核心场景：健康节点排空后，
// 在跑任务被在线迁移（保留 attempt/节拍语义）到其他健康节点，迁空后退役，
// 容量从可用容量中扣除；迁移期间作业始终 Running、任务不丢、账本不重复占用。
func TestDrainLiveMigratesTasksAndRetires(t *testing.T) {
	s := newTestScheduler(t) // n1=4/8, n2=2/4
	if err := s.AddNode("n3", api.Resource{CPU: 4, Memory: 8}); err != nil {
		t.Fatal(err)
	}

	mustSubmit(t, s, api.JobSpec{ID: "job", Priority: 1, Tasks: []api.TaskSpec{
		{Name: "t1", Resources: api.Resource{CPU: 3, Memory: 4}, Duration: 10},
		{Name: "t2", Resources: api.Resource{CPU: 2, Memory: 3}, Duration: 10},
	}})
	s.Tick() // t1->n1, t2->n2
	if got := nodeOf(t, s, "job", "t2"); got.NodeID != "n2" || got.State != api.TaskRunning {
		t.Fatalf("setup: t2 should run on n2, got %+v", got)
	}

	// 排空 n2：立即不再接收新任务，状态 Draining。
	if err := s.DrainNode("n2"); err != nil {
		t.Fatalf("drain n2: %v", err)
	}
	if st := findNode(s, "n2").State; st != api.NodeDraining {
		t.Fatalf("n2 should be draining, got %s", st)
	}

	// 进展可见：排空中且还剩 1 个任务未迁完。
	prog := s.DrainProgress()
	if len(prog) != 1 || prog[0].NodeID != "n2" || prog[0].State != api.NodeDraining || prog[0].Remaining != 1 {
		t.Fatalf("unexpected drain progress: %+v", prog)
	}

	// 重复排空/对不存在节点排空应报错。
	if err := s.DrainNode("n2"); err == nil {
		t.Fatal("draining an already-draining node must be rejected")
	}
	if err := s.DrainNode("ghost"); err == nil {
		t.Fatal("draining an unknown node must be rejected")
	}

	s.Tick() // 排空推进：t2 在线迁移。n3 与 n1 都可放，按 ID 排序首选 n1；
	// n1 已用 3cpu 剩 1cpu 放不下 2cpu 的 t2，因此应迁到 n3。
	tt2 := nodeOf(t, s, "job", "t2")
	if tt2.NodeID != "n3" {
		t.Fatalf("t2 should live-migrate to n3, got %+v", tt2)
	}
	if tt2.Attempt != 0 {
		t.Fatalf("live migration must not increase attempt, got %d", tt2.Attempt)
	}
	// 作业始终 Running，t1 未受影响。
	j, _ := s.GetJob("job")
	if j.State != api.JobRunning {
		t.Fatalf("job must stay running during migration, got %s", j.State)
	}
	if got := nodeOf(t, s, "job", "t1"); got.NodeID != "n1" || got.State != api.TaskRunning {
		t.Fatalf("t1 must keep running on n1: %+v", got)
	}

	// n2 已迁空 -> 同 tick 内退役，账本为零。
	n2 := findNode(s, "n2")
	if n2.State != api.NodeRetired {
		t.Fatalf("emptied node must be retired, got %s", n2.State)
	}
	if n2.Used != (api.Resource{}) {
		t.Fatalf("retired node must have zero usage, got %+v", n2.Used)
	}
	prog = s.DrainProgress()
	if len(prog) != 1 || prog[0].State != api.NodeRetired || prog[0].Remaining != 0 {
		t.Fatalf("retired node progress wrong: %+v", prog)
	}

	// 退役节点不能再排空。
	if err := s.DrainNode("n2"); err == nil {
		t.Fatal("retired node cannot be drained")
	}

	// 跑到作业结束，全程账本自洽。
	for i := 0; i < 12; i++ {
		s.Tick()
		assertLedger(t, s)
	}
	j, _ = s.GetJob("job")
	if j.State != api.JobSucceeded {
		t.Fatalf("job must succeed after live migration: %s (%+v)", j.State, j.Tasks)
	}
	for _, n := range s.Nodes() {
		if n.State != api.NodeDead && n.Used != (api.Resource{}) {
			t.Fatalf("node %s leaked resources: %+v", n.ID, n.Used)
		}
	}
	t.Logf("drain+retire verified: t1=%s t2=%s",
		nodeOf(t, s, "job", "t1").NodeID, nodeOf(t, s, "job", "t2").NodeID)
}

// TestDrainWaitsForCapacity 排空中任务暂时无处可去时保持原地继续运行，
// 待健康节点释放容量后迁出并退役；期间不能误判故障、不能丢任务。
func TestDrainWaitsForCapacity(t *testing.T) {
	s := newTestScheduler(t) // n1=4/8, n2=2/4

	// blocker 长时间占满 n1（4cpu）。
	mustSubmit(t, s, api.JobSpec{ID: "blocker", Priority: 5, Tasks: []api.TaskSpec{
		{Name: "b", Resources: api.Resource{CPU: 4, Memory: 1}, Duration: 6},
	}})
	// victim 占满 n2（2cpu）。
	mustSubmit(t, s, api.JobSpec{ID: "victim", Priority: 1, Tasks: []api.TaskSpec{
		{Name: "v", Resources: api.Resource{CPU: 2, Memory: 1}, Duration: 8},
	}})
	s.Tick()
	if got := nodeOf(t, s, "victim", "v"); got.NodeID != "n2" {
		t.Fatalf("setup: v on n2, got %s", got.NodeID)
	}

	if err := s.DrainNode("n2"); err != nil {
		t.Fatal(err)
	}
	// n1 已满，v 迁不出去：保持在 n2 继续运行，n2 仍是 Draining。
	for i := 0; i < 3; i++ {
		s.Tick()
		assertLedger(t, s)
	}
	if got := nodeOf(t, s, "victim", "v"); got.NodeID != "n2" || got.State != api.TaskRunning {
		t.Fatalf("v must keep running on draining n2 when nowhere fits: %+v", got)
	}
	if st := findNode(s, "n2").State; st != api.NodeDraining {
		t.Fatalf("n2 must stay draining with tasks, got %s", st)
	}
	prog := s.DrainProgress()
	if len(prog) != 1 || prog[0].Remaining != 1 {
		t.Fatalf("progress should show 1 remaining: %+v", prog)
	}

	// blocker 在 tick6 末完成释放 n1；随后 tick v 可迁到 n1，n2 退役。
	for i := 0; i < 6; i++ {
		s.Tick()
		assertLedger(t, s)
	}
	if got := nodeOf(t, s, "victim", "v"); got.NodeID != "n1" {
		t.Fatalf("v should migrate to freed n1, got %+v", got)
	}
	if st := findNode(s, "n2").State; st != api.NodeRetired {
		t.Fatalf("n2 should retire after migration, got %s", st)
	}
	for i := 0; i < 8; i++ {
		s.Tick()
	}
	j, _ := s.GetJob("victim")
	if j.State != api.JobSucceeded {
		t.Fatalf("victim must finish: %s", j.State)
	}
}

// TestDrainEmptyNodeRetiresImmediately 排空一个本就没有任务的节点：
// 下一次推进即退役，容量立即从可用容量中扣除。
func TestDrainEmptyNodeRetiresImmediately(t *testing.T) {
	s := newTestScheduler(t)
	if err := s.DrainNode("n2"); err != nil {
		t.Fatal(err)
	}
	s.Tick()
	if st := findNode(s, "n2").State; st != api.NodeRetired {
		t.Fatalf("empty draining node must retire on next tick, got %s", st)
	}
}

// TestCapacityShrunkSubmitAndQueue 容量收缩后的提交与排队判定：
// 排空发起后提交按“仅健康节点”容量判定；退役后按收缩容量判定。
// 之前已收下、暂时放不下的作业继续排队，状态保持 Pending，既不会被误判成功/失败也不会超发。
func TestCapacityShrunkSubmitAndQueue(t *testing.T) {
	s := newTestScheduler(t) // n1=4/8, n2=2/4

	// 先收下一个 5cpu 的作业（全集群 6cpu 可整体放下），但它尚未启动。
	mustSubmit(t, s, api.JobSpec{ID: "queued-before", Priority: 1, Tasks: []api.TaskSpec{
		{Name: "a", Resources: api.Resource{CPU: 3, Memory: 1}, Duration: 2},
		{Name: "b", Resources: api.Resource{CPU: 2, Memory: 1}, Duration: 2},
	}})

	// 先用 filler 占满健康容量，让上面的作业排队。
	// n1 放 4cpu、n2 放 2cpu。
	mustSubmit(t, s, api.JobSpec{ID: "filler-n1", Priority: 10, Tasks: []api.TaskSpec{
		{Name: "f", Resources: api.Resource{CPU: 4, Memory: 1}, Duration: 3},
	}})
	mustSubmit(t, s, api.JobSpec{ID: "filler-n2", Priority: 10, Tasks: []api.TaskSpec{
		{Name: "g", Resources: api.Resource{CPU: 2, Memory: 1}, Duration: 3},
	}})
	s.Tick() // fillers 启动；queued-before 因容量被占保持 Pending
	assertJobState(t, s, "queued-before", api.JobPending)

	// 发起 n2 排空（其任务迁出前先保持；为让容量立刻收缩，给一个可承接的节点）。
	if err := s.AddNode("n3", api.Resource{CPU: 4, Memory: 8}); err != nil {
		t.Fatal(err)
	}
	if err := s.DrainNode("n2"); err != nil {
		t.Fatal(err)
	}
	s.Tick() // filler-n2 的 g 迁到 n3，n2 退役
	if st := findNode(s, "n2").State; st != api.NodeRetired {
		t.Fatalf("n2 must retire, got %s", st)
	}

	// 此刻健康容量 = n1(4)+n3(4)=8cpu。一个需要 8cpu 的作业应被接受（可放下）；
	// 但一个需要 9cpu 的作业按收缩后容量必须被拒绝（不会被错误接受后超发）。
	mustSubmit(t, s, api.JobSpec{ID: "fits-8", Priority: 1, Tasks: []api.TaskSpec{
		{Name: "x", Resources: api.Resource{CPU: 4, Memory: 1}, Duration: 1},
		{Name: "y", Resources: api.Resource{CPU: 4, Memory: 1}, Duration: 1},
	}})
	err := s.Submit(api.JobSpec{ID: "never-9", Priority: 1, Tasks: []api.TaskSpec{
		{Name: "x", Resources: api.Resource{CPU: 5, Memory: 1}, Duration: 1},
		{Name: "y", Resources: api.Resource{CPU: 4, Memory: 1}, Duration: 1},
	}})
	if !errors.Is(err, api.ErrInsufficientResources) {
		t.Fatalf("job exceeding shrunk capacity must be rejected, got %v", err)
	}

	// 排空发起期间提交、需要用到原 n2 才能放下的作业：按收缩拓扑直接拒绝。
	// n2 退役后，一个 2cpu 但要求 5mem 的任务：n1(8mem)/n3(8mem) 都能放，
	// 这里改为验证“总量超过剩余健康容量”已由 never-9 覆盖；此用例不再重复。

	// queued-before 之前已收下：容量被 fillers 占用期间保持 Pending（可解释：等待资源），
	// 不能因为容量变化被判成功/失败。
	assertJobState(t, s, "queued-before", api.JobPending)

	// 跑足够久让 fillers 完成、所有可调度作业最终完成。
	for i := 0; i < 20; i++ {
		s.Tick()
		assertLedger(t, s)
	}
	for _, id := range []string{"queued-before", "fits-8", "filler-n1", "filler-n2"} {
		assertJobState(t, s, id, api.JobSucceeded)
	}
	// 从未被接受的作业不在存储中。
	if _, err := s.GetJob("never-9"); !errors.Is(err, api.ErrJobNotFound) {
		t.Fatalf("rejected job must not be stored, got %v", err)
	}
}

// TestDrainMultipleNodes 多台节点先后排空、有的仍在排空中时账目始终对得上。
func TestDrainMultipleNodes(t *testing.T) {
	s := newTestScheduler(t) // n1,n2
	if err := s.AddNode("n3", api.Resource{CPU: 4, Memory: 8}); err != nil {
		t.Fatal(err)
	}
	if err := s.AddNode("n4", api.Resource{CPU: 4, Memory: 8}); err != nil {
		t.Fatal(err)
	}
	mustSubmit(t, s, api.JobSpec{ID: "j1", Tasks: []api.TaskSpec{
		{Name: "a", Resources: api.Resource{CPU: 3, Memory: 1}, Duration: 4},
		{Name: "b", Resources: api.Resource{CPU: 2, Memory: 1}, Duration: 4},
	}})
	s.Tick() // a(3)->n1 后 n1 只剩 1cpu，b(2) -> n2
	if got := nodeOf(t, s, "j1", "a"); got.NodeID != "n1" {
		t.Fatalf("setup: a on n1, got %s", got.NodeID)
	}
	if got := nodeOf(t, s, "j1", "b"); got.NodeID != "n2" {
		t.Fatalf("setup: b on n2, got %s", got.NodeID)
	}

	// 先排空 n1（n2 被 b 占满，a(3) 应迁到 n3）。
	if err := s.DrainNode("n1"); err != nil {
		t.Fatal(err)
	}
	s.Tick()
	if got := nodeOf(t, s, "j1", "a"); got.NodeID != "n3" {
		t.Fatalf("a should migrate to n3, got %s", got.NodeID)
	}
	if st := findNode(s, "n1").State; st != api.NodeRetired {
		t.Fatalf("n1 should retire, got %s", st)
	}

	// 再排空 n2：n3 已被 a(3) 占去 1cpu 余量放不下 b(2)，b 应迁到 n4。
	if err := s.DrainNode("n2"); err != nil {
		t.Fatal(err)
	}
	s.Tick()
	if got := nodeOf(t, s, "j1", "b"); got.NodeID != "n4" {
		t.Fatalf("b should migrate to n4, got %s", got.NodeID)
	}
	if st := findNode(s, "n2").State; st != api.NodeRetired {
		t.Fatalf("n2 should retire, got %s", st)
	}

	for i := 0; i < 8; i++ {
		s.Tick()
		assertLedger(t, s)
	}
	j, _ := s.GetJob("j1")
	if j.State != api.JobSucceeded {
		t.Fatalf("j1 must succeed: %s", j.State)
	}
	// 退役节点容量被扣除：只剩 n3、n4 健康。
	for _, id := range []string{"n1", "n2"} {
		if st := findNode(s, id).State; st != api.NodeRetired {
			t.Fatalf("%s should stay retired, got %s", id, st)
		}
	}
}

// TestDrainDoesNotPlaceNewTasks 排空中节点绝不接收新任务，
// 即使它是按 ID 排序的首个“看起来合适”的节点。
func TestDrainDoesNotPlaceNewTasks(t *testing.T) {
	s := newTestScheduler(t)
	// n1 排空（空闲），n2 健康。新任务只能去 n2，不能去 n1。
	if err := s.DrainNode("n1"); err != nil {
		t.Fatal(err)
	}
	s.Tick() // n1 退役
	mustSubmit(t, s, api.JobSpec{ID: "j", Tasks: []api.TaskSpec{
		{Name: "t", Resources: api.Resource{CPU: 1, Memory: 1}, Duration: 2},
	}})
	s.Tick()
	if got := nodeOf(t, s, "j", "t"); got.NodeID != "n2" {
		t.Fatalf("new task must avoid retired n1 and land on n2, got %s", got.NodeID)
	}
}

// TestDrainPersistsAcrossRestart 排空中进程重启：恢复后节点仍是排空中，
// 未迁完的任务不会丢、不会重复占；排空可继续推进直至退役。
func TestDrainPersistsAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.json")

	s1 := New(Config{StateFile: path, HeartbeatTimeoutTicks: 2, Logger: testLogger(t)})
	for _, id := range []string{"n1", "n2", "n3"} {
		cap := api.Resource{CPU: 4, Memory: 8}
		if id == "n2" {
			cap = api.Resource{CPU: 2, Memory: 4}
		}
		if err := s1.AddNode(id, cap); err != nil {
			t.Fatal(err)
		}
	}
	mustSubmit(t, s1, api.JobSpec{ID: "job", Tasks: []api.TaskSpec{
		{Name: "t1", Resources: api.Resource{CPU: 3, Memory: 4}, Duration: 9},
		{Name: "t2", Resources: api.Resource{CPU: 2, Memory: 3}, Duration: 9},
	}})
	s1.Tick() // t1->n1, t2->n2
	if err := s1.DrainNode("n2"); err != nil {
		t.Fatal(err)
	}
	// 不再推进，直接模拟“排空刚发起就崩溃”。

	// 重启：相同拓扑重新注册并恢复。
	s2 := New(Config{StateFile: path, HeartbeatTimeoutTicks: 2, Logger: testLogger(t)})
	caps := map[string]api.Resource{
		"n1": {CPU: 4, Memory: 8}, "n2": {CPU: 2, Memory: 4}, "n3": {CPU: 4, Memory: 8},
	}
	for _, id := range []string{"n1", "n2", "n3"} {
		if err := s2.AddNode(id, caps[id]); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s2.Recover(); err != nil {
		t.Fatalf("recover: %v", err)
	}

	// n2 必须仍是 Draining，不会被当健康机器重新接任务。
	if st := findNode(s2, "n2").State; st != api.NodeDraining {
		t.Fatalf("draining node must stay draining after restart, got %s", st)
	}
	// 重启后执行行清空：t2 按恢复语义复位为 Pending（与崩溃恢复一致），不重复占用。
	j, _ := s2.GetJob("job")
	for _, tsk := range j.Tasks {
		if tsk.State == api.TaskRunning {
			t.Fatalf("running tasks must be reset on restart: %+v", tsk)
		}
	}
	// 新节点池账本初始为零。
	for _, n := range s2.Nodes() {
		if n.Used != (api.Resource{}) {
			t.Fatalf("fresh pool must start zero-used: %s %+v", n.ID, n.Used)
		}
	}

	// 推进：gang 重放只放到健康节点（t2 不会回 n2）；排空把 n2 退役。
	for i := 0; i < 12; i++ {
		s2.Tick()
		assertLedger(t, s2)
	}
	if st := findNode(s2, "n2").State; st != api.NodeRetired {
		t.Fatalf("n2 must retire after resumed drain, got %s", st)
	}
	j, _ = s2.GetJob("job")
	if j.State != api.JobSucceeded {
		t.Fatalf("job must complete after resumed drain: %s", j.State)
	}
	for _, tsk := range j.Tasks {
		if tsk.NodeID == "n2" {
			t.Fatalf("task must never be placed back on drained node: %+v", tsk)
		}
	}
}

// TestDrainRetiredPersistsAcrossRestart 已退役节点重启后保持退役，容量持续扣除。
func TestDrainRetiredPersistsAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.json")

	s1 := New(Config{StateFile: path, Logger: testLogger(t)})
	if err := s1.AddNode("n1", api.Resource{CPU: 4, Memory: 8}); err != nil {
		t.Fatal(err)
	}
	if err := s1.AddNode("n2", api.Resource{CPU: 2, Memory: 4}); err != nil {
		t.Fatal(err)
	}
	if err := s1.DrainNode("n2"); err != nil {
		t.Fatal(err)
	}
	s1.Tick() // n2 空节点退役
	if st := findNode(s1, "n2").State; st != api.NodeRetired {
		t.Fatalf("setup: n2 retired, got %s", st)
	}

	s2 := New(Config{StateFile: path, Logger: testLogger(t)})
	if err := s2.AddNode("n1", api.Resource{CPU: 4, Memory: 8}); err != nil {
		t.Fatal(err)
	}
	if err := s2.AddNode("n2", api.Resource{CPU: 2, Memory: 4}); err != nil {
		t.Fatal(err)
	}
	if _, err := s2.Recover(); err != nil {
		t.Fatal(err)
	}
	if st := findNode(s2, "n2").State; st != api.NodeRetired {
		t.Fatalf("retired node must stay retired after restart, got %s", st)
	}
	// 只剩 n1 的 4cpu：5cpu 作业必须被拒绝。
	err := s2.Submit(api.JobSpec{ID: "big", Tasks: []api.TaskSpec{
		{Name: "x", Resources: api.Resource{CPU: 5, Memory: 1}, Duration: 1},
	}})
	if !errors.Is(err, api.ErrInsufficientResources) {
		t.Fatalf("capacity must remain shrunk across restart, got %v", err)
	}
}

// TestRecoverRejectsMismatchedTopology 恢复时快照节点缺失或容量不符应报错。
func TestRecoverRejectsMismatchedTopology(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.json")
	s1 := New(Config{StateFile: path, Logger: testLogger(t)})
	if err := s1.AddNode("n1", api.Resource{CPU: 4, Memory: 8}); err != nil {
		t.Fatal(err)
	}
	if err := s1.DrainNode("n1"); err != nil {
		t.Fatal(err)
	}

	// 少注册节点。
	s2 := New(Config{StateFile: path, Logger: testLogger(t)})
	if _, err := s2.Recover(); err == nil {
		t.Fatal("recover with missing registered node must fail")
	}

	// 容量不符。
	s3 := New(Config{StateFile: path, Logger: testLogger(t)})
	if err := s3.AddNode("n1", api.Resource{CPU: 9, Memory: 9}); err != nil {
		t.Fatal(err)
	}
	if _, err := s3.Recover(); err == nil {
		t.Fatal("recover with mismatched capacity must fail")
	}
}

// TestDrainConcurrentWithTicksAndSubmit 排空与调度推进、作业提交并发交错，
// 不丢作业、不重复占资源，最终全部作业恰好成功一次（-race 验证）。
func TestDrainConcurrentWithTicksAndSubmit(t *testing.T) {
	s := newTestScheduler(t)
	for _, id := range []string{"n3", "n4", "n5"} {
		if err := s.AddNode(id, api.Resource{CPU: 4, Memory: 8}); err != nil {
			t.Fatal(err)
		}
	}
	stop := make(chan struct{})
	ticksDone := make(chan struct{})
	var wg sync.WaitGroup

	// tick 循环（独立等待，不能加入下方 wg：它靠 stop 退出，而 stop 在 wg 完成后才关闭）。
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

	// 提交者。
	var accepted int64
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := s.Submit(api.JobSpec{
				ID: fmt.Sprintf("dj-%d", i), Priority: i % 4,
				Tasks: []api.TaskSpec{
					{Name: "a", Resources: api.Resource{CPU: 1, Memory: 1}, Duration: 1 + i%2},
					{Name: "b", Resources: api.Resource{CPU: 1, Memory: 1}, Duration: 1 + i%2},
				},
			})
			if err == nil {
				atomic.AddInt64(&accepted, 1)
			} else if !errors.Is(err, api.ErrInsufficientResources) {
				t.Errorf("submit %d: %v", i, err)
			}
		}(i)
	}

	// 排空发起者：只排空其中 2 台，保证仍有充足健康容量让全部作业完成。
	drains := []string{"n1", "n2"}
	for _, id := range drains {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			_ = s.DrainNode(id) // 并发下可能已被排空，忽略“已在排空中”错误
		}(id)
	}

	wg.Wait()
	for i := 0; i < 300; i++ {
		s.Tick()
	}
	close(stop)
	<-ticksDone

	// 被接受的作业全部恰好成功，无 Pending/Running 残留。
	for _, jb := range s.store.List() {
		if jb.State != api.JobSucceeded {
			t.Fatalf("job %s state=%s, want Succeeded", jb.Spec.ID, jb.State)
		}
	}
	// 被请求排空的节点必须已退役且账本为零；健康节点终态账本也为零。
	for _, n := range s.Nodes() {
		if n.State == api.NodeDraining {
			t.Fatalf("node %s still draining after ample drain time", n.ID)
		}
		if n.State == api.NodeRetired {
			if n.Used != (api.Resource{}) {
				t.Fatalf("retired node %s must be zero-used: %+v", n.ID, n.Used)
			}
			continue
		}
		if n.State == api.NodeHealthy && n.Used != (api.Resource{}) {
			t.Fatalf("healthy node %s leaked: %+v", n.ID, n.Used)
		}
	}
	t.Logf("concurrent drain accepted=%d jobs, all succeeded", atomic.LoadInt64(&accepted))
}

// TestDrainThenNodeFailure 排空期间节点又故障（心跳停止）：走统一故障回收，
// 任务复位重调度、节点最终死亡，账目仍然自洽。
func TestDrainThenNodeFailure(t *testing.T) {
	// 仅 n1(4)、n2(2)：t1(3) 占住 n1 后只剩 1cpu，t2(2) 无处可迁。
	s := newTestScheduler(t)
	mustSubmit(t, s, api.JobSpec{ID: "job", Tasks: []api.TaskSpec{
		{Name: "t1", Resources: api.Resource{CPU: 3, Memory: 4}, Duration: 4},
		{Name: "t2", Resources: api.Resource{CPU: 2, Memory: 3}, Duration: 9},
	}})
	s.Tick() // t1->n1, t2->n2
	if err := s.DrainNode("n2"); err != nil {
		t.Fatal(err)
	}
	// t2 迁不出去：保持在排空中的 n2 继续运行。
	s.Tick()
	if got := nodeOf(t, s, "job", "t2"); got.NodeID != "n2" {
		t.Fatalf("t2 should be stuck on draining n2, got %s", got.NodeID)
	}
	// 此时 n2 又发生故障（心跳停止）：走统一超时回收，t2 复位重调度。
	s.TriggerNodeFailure("n2")
	for i := 0; i < 16; i++ {
		s.Tick()
		assertLedger(t, s)
	}
	if st := findNode(s, "n2").State; st != api.NodeDead {
		t.Fatalf("draining node that lost heartbeat must become dead, got %s", st)
	}
	j, _ := s.GetJob("job")
	if j.State != api.JobSucceeded {
		t.Fatalf("job must succeed via failure reclaim, got %s", j.State)
	}
	if got := nodeOf(t, s, "job", "t2"); got.NodeID != "n1" {
		t.Fatalf("t2 should be rescheduled to n1 after reclaim, got %s", got.NodeID)
	}
}

// TestShrunkCapacityQueuedVsNewSubmit 锁定关键区分：容量因退役永久收缩后，
// 收缩前已收下、收缩后总需求超过剩余健康容量的作业继续排队（保持 Pending，
// 不会误判成功/失败、不超发）；而收缩后新提交的同样作业被明确拒绝。
func TestShrunkCapacityQueuedVsNewSubmit(t *testing.T) {
	s := newTestScheduler(t) // n1=4, n2=2；总 6cpu

	// 先提交一个需要 5cpu 的作业（6cpu 拓扑下可行），让它排队：
	// 用一个只占 n1(4cpu) 的长占位作业，使 big-queued 启动不了，同时 n2 空闲。
	mustSubmit(t, s, api.JobSpec{ID: "big-queued", Priority: 1, Tasks: []api.TaskSpec{
		{Name: "a", Resources: api.Resource{CPU: 3, Memory: 1}, Duration: 1},
		{Name: "b", Resources: api.Resource{CPU: 2, Memory: 1}, Duration: 1},
	}})
	mustSubmit(t, s, api.JobSpec{ID: "hold", Priority: 10, Tasks: []api.TaskSpec{
		{Name: "h", Resources: api.Resource{CPU: 4, Memory: 1}, Duration: 8},
	}})
	s.Tick() // h -> n1；n2 空闲；big-queued 需要 5cpu 但只剩 n2 的 2cpu，排队
	assertJobState(t, s, "big-queued", api.JobPending)

	// n2 排空时本就空闲：立即退役，健康容量永久收缩为 n1 的 4cpu。
	if err := s.DrainNode("n2"); err != nil {
		t.Fatal(err)
	}
	s.Tick()
	if st := findNode(s, "n2").State; st != api.NodeRetired {
		t.Fatalf("setup: n2 should retire, got %s", st)
	}

	// 新提交的 5cpu 作业按收缩后容量直接拒绝。
	err := s.Submit(api.JobSpec{ID: "big-new", Tasks: []api.TaskSpec{
		{Name: "a", Resources: api.Resource{CPU: 3, Memory: 1}, Duration: 1},
		{Name: "b", Resources: api.Resource{CPU: 2, Memory: 1}, Duration: 1},
	}})
	if !errors.Is(err, api.ErrInsufficientResources) {
		t.Fatalf("new submit beyond shrunk capacity must be rejected, got %v", err)
	}

	// 之前已收下的 big-queued：hold 释放后也只剩 4cpu，永远放不下 5cpu，
	// 但它必须保持 Pending（不判成功/失败），且账本不超发。
	for i := 0; i < 12; i++ {
		s.Tick()
		assertLedger(t, s)
	}
	assertJobState(t, s, "big-queued", api.JobPending)
	// 它的任务一个都没有启动（不允许部分启动造成超发）。
	j, _ := s.GetJob("big-queued")
	for _, tsk := range j.Tasks {
		if tsk.State != api.TaskPending || tsk.NodeID != "" {
			t.Fatalf("unplaceable queued job tasks must stay Pending/unbound: %+v", tsk)
		}
	}
	// hold 正常完成。
	assertJobState(t, s, "hold", api.JobSucceeded)
	t.Logf("queued job stayed Pending after permanent shrink; new submit rejected: %v", err)
}
