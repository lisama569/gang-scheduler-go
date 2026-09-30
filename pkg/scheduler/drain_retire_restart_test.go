package scheduler

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/lisama569/gang-scheduler-go/pkg/api"
)

// healthyCapacitySum 汇总当前健康（参与调度）节点的容量。
func healthyCapacitySum(s *Scheduler) api.Resource {
	var total api.Resource
	for _, n := range s.Nodes() {
		if n.State == api.NodeHealthy {
			total = total.Add(n.Capacity)
		}
	}
	return total
}

// TestRetiredNodeNotResurrectedOnRestart 已完成排空正式下线的节点：
// 重启后重新 AddNode 同名节点必须被拒绝（ErrNodeRetired），节点保持 Dead，
// 集群容量按收缩后计算，不会涨回去。
func TestRetiredNodeNotResurrectedOnRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.json")

	s1 := New(Config{HeartbeatTimeoutTicks: 2, StateFile: path, Logger: testLogger(t)})
	mustAdd := func(id string, c api.Resource) {
		t.Helper()
		if err := s1.AddNode(id, c); err != nil {
			t.Fatal(err)
		}
	}
	mustAdd("n1", api.Resource{CPU: 4, Memory: 8})
	mustAdd("n2", api.Resource{CPU: 2, Memory: 4})
	before := healthyCapacitySum(s1)
	if before != (api.Resource{CPU: 6, Memory: 12}) {
		t.Fatalf("setup healthy capacity=%+v", before)
	}

	// 空节点立即排空退役。
	if err := s1.DrainNode("n2"); err != nil {
		t.Fatal(err)
	}
	if err := s1.CompleteNodeDrain("n2"); err != nil {
		t.Fatal(err)
	}
	if got := healthyCapacitySum(s1); got != (api.Resource{CPU: 4, Memory: 8}) {
		t.Fatalf("capacity must shrink immediately after retire: %+v", got)
	}
	if got := nodeState(t, s1, "n2").State; got != api.NodeDead {
		t.Fatalf("n2 should be Dead, got %s", got)
	}

	// 重启：全新调度器 + 同一份快照。
	s2 := New(Config{HeartbeatTimeoutTicks: 2, StateFile: path, Logger: testLogger(t)})
	if err := s2.AddNode("n1", api.Resource{CPU: 4, Memory: 8}); err != nil {
		t.Fatal(err)
	}
	// 关键断言：已经下线的节点不能靠重新注册复活。
	err := s2.AddNode("n2", api.Resource{CPU: 2, Memory: 4})
	if !errors.Is(err, api.ErrNodeRetired) {
		t.Fatalf("re-register retired node must fail with ErrNodeRetired, got %v", err)
	}
	if _, err := s2.Recover(); err != nil {
		t.Fatal(err)
	}
	// Recover 之后再注册一次，仍然被拒绝（两种注册/恢复顺序都安全）。
	err = s2.AddNode("n2", api.Resource{CPU: 2, Memory: 4})
	if !errors.Is(err, api.ErrNodeRetired) {
		t.Fatalf("retired node stays rejected after Recover, got %v", err)
	}

	if got := healthyCapacitySum(s2); got != (api.Resource{CPU: 4, Memory: 8}) {
		t.Fatalf("capacity must stay shrunken across restart: %+v", got)
	}
	// n2 在 s2 的节点视图中根本不存在（注册被拒），也绝不影响容量判断。
	for _, n := range s2.Nodes() {
		if n.ID == "n2" {
			t.Fatalf("retired n2 must not be present after rejected registration: %+v", n)
		}
	}

	// 收缩后的容量判断：{5,*} 的作业永远放不下 -> 拒绝，且原因可解释。
	err = s2.Submit(api.JobSpec{ID: "too-big", Tasks: []api.TaskSpec{
		{Name: "x", Resources: api.Resource{CPU: 5, Memory: 1}, Duration: 1},
	}})
	if !errors.Is(err, api.ErrInsufficientResources) {
		t.Fatalf("job beyond shrunken capacity must be rejected, got %v", err)
	}
	t.Logf("retired node not resurrected, capacity stays %+v, rejection: %v",
		healthyCapacitySum(s2), err)
}

// TestRetiredMarkerEffectiveBeforeRecover 节点标记在 New 时就已从快照预加载：
// 即使调用方在 Recover 之前先 AddNode，退役节点也会立刻被拒，
// 不存在“Recover 前的窗口期把已下线机器注册成健康节点”的漏洞。
func TestRetiredMarkerEffectiveBeforeRecover(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.json")

	s1 := New(Config{HeartbeatTimeoutTicks: 2, StateFile: path, Logger: testLogger(t)})
	if err := s1.AddNode("n1", api.Resource{CPU: 4, Memory: 8}); err != nil {
		t.Fatal(err)
	}
	if err := s1.AddNode("n2", api.Resource{CPU: 2, Memory: 4}); err != nil {
		t.Fatal(err)
	}
	if err := s1.DrainNode("n2"); err != nil {
		t.Fatal(err)
	}
	if err := s1.CompleteNodeDrain("n2"); err != nil {
		t.Fatal(err)
	}

	// 全新进程：构造时标记即从快照预加载，Recover 还没有被调用。
	s2 := New(Config{HeartbeatTimeoutTicks: 2, StateFile: path, Logger: testLogger(t)})
	err := s2.AddNode("n2", api.Resource{CPU: 2, Memory: 4})
	if !errors.Is(err, api.ErrNodeRetired) {
		t.Fatalf("retired node must be rejected even before Recover, got %v", err)
	}
	// 健康节点注册与后续 Recover 一切正常。
	if err := s2.AddNode("n1", api.Resource{CPU: 4, Memory: 8}); err != nil {
		t.Fatal(err)
	}
	if _, err := s2.Recover(); err != nil {
		t.Fatal(err)
	}
	if got := healthyCapacitySum(s2); got != (api.Resource{CPU: 4, Memory: 8}) {
		t.Fatalf("capacity must stay shrunken: %+v", got)
	}
	t.Log("retired marker is effective immediately after New, before Recover")
}

// TestDrainingMarkerEffectiveBeforeRecover 排空中节点重启后在 Recover 之前
// 重新注册时，也会立即恢复为 Draining（不是先健康、再被 Recover 改回去）：
// 不存在任何一个 tick 让维护中的节点接收新任务。
func TestDrainingMarkerEffectiveBeforeRecover(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.json")

	s1 := New(Config{HeartbeatTimeoutTicks: 2, StateFile: path, Logger: testLogger(t)})
	if err := s1.AddNode("n1", api.Resource{CPU: 4, Memory: 8}); err != nil {
		t.Fatal(err)
	}
	if err := s1.AddNode("n2", api.Resource{CPU: 2, Memory: 4}); err != nil {
		t.Fatal(err)
	}
	if err := s1.DrainNode("n2"); err != nil {
		t.Fatal(err)
	}

	s2 := New(Config{HeartbeatTimeoutTicks: 2, StateFile: path, Logger: testLogger(t)})
	if err := s2.AddNode("n1", api.Resource{CPU: 4, Memory: 8}); err != nil {
		t.Fatal(err)
	}
	if err := s2.AddNode("n2", api.Resource{CPU: 2, Memory: 4}); err != nil {
		t.Fatalf("draining node registration itself should succeed: %v", err)
	}
	// 尚未 Recover，节点也已经是 Draining。
	if got := nodeState(t, s2, "n2").State; got != api.NodeDraining {
		t.Fatalf("n2 must be Draining immediately on registration, got %s", got)
	}
	if _, err := s2.Recover(); err != nil {
		t.Fatal(err)
	}
	if got := nodeState(t, s2, "n2").State; got != api.NodeDraining {
		t.Fatalf("n2 must remain Draining after Recover, got %s", got)
	}
	// 排空中节点不参与放置：两个 2cpu 任务在仅剩的 n1(4cpu) 上整体放不下。
	err := s2.Submit(api.JobSpec{ID: "j", Tasks: []api.TaskSpec{
		{Name: "a", Resources: api.Resource{CPU: 3, Memory: 1}, Duration: 1},
		{Name: "b", Resources: api.Resource{CPU: 3, Memory: 1}, Duration: 1},
	}})
	if !errors.Is(err, api.ErrInsufficientResources) {
		t.Fatalf("draining n2 must be excluded from capacity: %v", err)
	}
	t.Log("draining marker effective before Recover, node never accepted new tasks")
}

// TestReadmitRetiredNodeBringsCapacityBack 运维显式 ReadmitNode：
// 退役节点重新上线为健康机器，容量重新计入集群，作业可以正常调度；
// 而对从未退役的节点调用 ReadmitNode 返回 ErrNodeNotRetired。
func TestReadmitRetiredNodeBringsCapacityBack(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.json")

	s := New(Config{HeartbeatTimeoutTicks: 2, StateFile: path, Logger: testLogger(t)})
	if err := s.AddNode("n1", api.Resource{CPU: 2, Memory: 2}); err != nil {
		t.Fatal(err)
	}
	if err := s.AddNode("n2", api.Resource{CPU: 2, Memory: 2}); err != nil {
		t.Fatal(err)
	}
	if err := s.DrainNode("n2"); err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteNodeDrain("n2"); err != nil {
		t.Fatal(err)
	}
	// 隐式注册被拒。
	if err := s.AddNode("n2", api.Resource{CPU: 2, Memory: 2}); !errors.Is(err, api.ErrNodeRetired) {
		t.Fatalf("add retired node: %v", err)
	}
	// 对健康节点调用 ReadmitNode 是非法操作。
	if err := s.ReadmitNode("n1", api.Resource{CPU: 2, Memory: 2}); !errors.Is(err, api.ErrNodeNotRetired) {
		t.Fatalf("readmit healthy node must be ErrNodeNotRetired, got %v", err)
	}
	// 对从未出现的节点调用 ReadmitNode 同样拒绝（新机器请走 AddNode）。
	if err := s.ReadmitNode("ghost", api.Resource{CPU: 2, Memory: 2}); !errors.Is(err, api.ErrNodeNotRetired) {
		t.Fatalf("readmit unknown node must be ErrNodeNotRetired, got %v", err)
	}

	// 显式重新上线，可携带新的容量规格。
	if err := s.ReadmitNode("n2", api.Resource{CPU: 4, Memory: 4}); err != nil {
		t.Fatalf("readmit retired n2: %v", err)
	}
	if got := nodeState(t, s, "n2").State; got != api.NodeHealthy {
		t.Fatalf("readmitted n2 must be Healthy, got %s", got)
	}
	if got := healthyCapacitySum(s); got != (api.Resource{CPU: 6, Memory: 6}) {
		t.Fatalf("capacity must include readmitted node: %+v", got)
	}

	// 重新上线后作业可正常调度完成；节点心跳也恢复（不会被超时判死）。
	mustSubmit(t, s, api.JobSpec{ID: "job", Tasks: []api.TaskSpec{
		{Name: "t", Resources: api.Resource{CPU: 3, Memory: 1}, Duration: 2},
	}})
	s.Tick() // 只能放到 n2（4cpu）
	j, _ := s.GetJob("job")
	if j.Tasks[0].NodeID != "n2" {
		t.Fatalf("task should land on readmitted n2: %+v", j.Tasks[0])
	}
	s.Tick()
	j, _ = s.GetJob("job")
	if j.State != api.JobSucceeded {
		t.Fatalf("job on readmitted node must succeed: %s", j.State)
	}

	// 重新上线的事实也必须持久化：再次重启后 n2 是普通健康节点（退役名单已清）。
	s2 := New(Config{HeartbeatTimeoutTicks: 2, StateFile: path, Logger: testLogger(t)})
	if err := s2.AddNode("n1", api.Resource{CPU: 2, Memory: 2}); err != nil {
		t.Fatal(err)
	}
	if err := s2.AddNode("n2", api.Resource{CPU: 4, Memory: 4}); err != nil {
		t.Fatalf("readmitted node must register normally after restart: %v", err)
	}
	if _, err := s2.Recover(); err != nil {
		t.Fatal(err)
	}
	if got := nodeState(t, s2, "n2").State; got != api.NodeHealthy {
		t.Fatalf("n2 must be Healthy after readmit+restart, got %s", got)
	}
	t.Log("explicit readmit restores capacity and survives restart")
}

// TestQueuedJobStaysPendingAfterShrink 容量收缩后：能收下但暂时（乃至永久）
// 放不下的作业保持 Pending、原因可解释，不会被错误判成功/失败，也不会超发。
func TestQueuedJobStaysPendingAfterShrink(t *testing.T) {
	s := newTestScheduler(t) // n1={4,8} n2={2,4}

	// first 长期占用 n1。
	mustSubmit(t, s, api.JobSpec{ID: "first", Tasks: []api.TaskSpec{
		{Name: "a", Resources: api.Resource{CPU: 4, Memory: 4}, Duration: 8},
	}})
	s.Tick()
	// second 需要整台 n1，此刻排队。
	mustSubmit(t, s, api.JobSpec{ID: "second", Tasks: []api.TaskSpec{
		{Name: "b", Resources: api.Resource{CPU: 4, Memory: 4}, Duration: 2},
	}})
	s.Tick()
	if j, _ := s.GetJob("second"); j.State != api.JobPending {
		t.Fatalf("second should be queued: %s", j.State)
	}

	// 在 second 排队期间把唯一能放下它的 n2 之外的容量路径彻底收缩：
	// n2 本来也放不下 4cpu 的任务；退役 n2 后集群只剩被占满的 n1。
	if err := s.DrainNode("n2"); err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteNodeDrain("n2"); err != nil {
		t.Fatal(err)
	}
	// first 在 tick1 启动（duration=8），tick8 末完成、tick9 起 n1 空闲；
	// second 在 tick9 的调度阶段获得 n1，tick10 末跑完它自己的 2 拍。
	for i := 0; i < 11; i++ {
		s.Tick()
	}
	// first 结束后 n1 空闲，second 此时放得下并正常完成（作业没有被误判失败）。
	j, _ := s.GetJob("second")
	if j.State != api.JobSucceeded {
		t.Fatalf("second must run once capacity frees, not be failed: %s", j.State)
	}

	// 但收缩后一个单任务需要 5cpu 的作业永远放不下：提交即拒绝。
	err := s.Submit(api.JobSpec{ID: "never-fit", Tasks: []api.TaskSpec{
		{Name: "x", Resources: api.Resource{CPU: 5, Memory: 1}, Duration: 1},
	}})
	if !errors.Is(err, api.ErrInsufficientResources) {
		t.Fatalf("job beyond permanently shrunken capacity rejected: %v", err)
	}
	// 账本无超发、无负数。
	for _, n := range s.Nodes() {
		if n.Used.CPU > n.Capacity.CPU || n.Used.Memory > n.Capacity.Memory ||
			n.Used.CPU < 0 || n.Used.Memory < 0 {
			t.Fatalf("ledger broken after shrink: %+v", n)
		}
	}
	t.Logf("queued job handled correctly across capacity shrink; permanent reject: %v", err)
}
