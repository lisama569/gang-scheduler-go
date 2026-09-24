package scheduler

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

// testLogger 把关键调度决策同时打印到测试日志（-v 可见）并保留在内存供断言。
type testLogger struct {
	t     *testing.T
	mu    sync.Mutex
	lines []string
}

func newTestLogger(t *testing.T) *testLogger {
	t.Helper()
	return &testLogger{t: t}
}

func (l *testLogger) Printf(format string, args ...any) {
	line := format
	if len(args) > 0 {
		line = sprintf(format, args...)
	}
	l.mu.Lock()
	l.lines = append(l.lines, line)
	l.mu.Unlock()
	l.t.Logf("%s", line)
}

func (l *testLogger) contains(sub string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, x := range l.lines {
		if strings.Contains(x, sub) {
			return true
		}
	}
	return false
}

func sprintf(format string, args ...any) string {
	return sprintfImpl(format, args...)
}

func newTestScheduler(t *testing.T, store Store, vc *VirtualClock, hb time.Duration) *Scheduler {
	t.Helper()
	s := New(store, WithLogger(newTestLogger(t)), WithClock(vc), WithHeartbeatTimeout(hb))
	if err := s.Recover(context.Background()); err != nil {
		t.Fatalf("recover: %v", err)
	}
	return s
}

// assertResourceInvariant 校验全局资源账本：
// 每个存活节点的 Used == 其上 RUNNING 任务请求之和；且 Used 不超过 Capacity。
func assertResourceInvariant(t *testing.T, s *Scheduler) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, n := range s.nodes.nodes {
		var sum Resource
		for k := range n.runningTaskKeys {
			task, ok := s.taskIndex[k]
			if !ok || task.State != TaskRunning || task.NodeID != n.ID {
				t.Errorf("invariant: node %s tracks %s but task not RUNNING on it", n.ID, k)
				continue
			}
			sum = sum.Add(task.Request)
		}
		if diff := sum.Sub(n.Used); diff.CPU > 1e-9 || diff.CPU < -1e-9 ||
			diff.Memory > 1e-9 || diff.Memory < -1e-9 {
			t.Errorf("invariant: node %s used=%v but running tasks sum=%v", n.ID, n.Used, sum)
		}
		if !n.Used.FitsIn(n.Capacity) {
			t.Errorf("invariant: node %s overcommitted used=%v cap=%v", n.ID, n.Used, n.Capacity)
		}
	}
	// 每个 RUNNING 任务必须登记在且仅在其 NodeID 的节点上。
	for _, task := range s.taskIndex {
		if task.State != TaskRunning {
			continue
		}
		n, ok := s.nodes.get(task.NodeID)
		if !ok {
			t.Errorf("invariant: task %s RUNNING on missing node %s", taskKey(task.JobID, task.ID), task.NodeID)
			continue
		}
		if _, ok := n.runningTaskKeys[taskKey(task.JobID, task.ID)]; !ok {
			t.Errorf("invariant: RUNNING task %s missing from node %s ledger", taskKey(task.JobID, task.ID), n.ID)
		}
	}
}

// assertJobTasksConsistent 作业终态必须与任务结果一致：SUCCEEDED 当且仅当所有任务 SUCCEEDED。
func assertJobTasksConsistent(t *testing.T, s *Scheduler) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, j := range s.jobs {
		allOK := true
		for _, task := range j.Tasks {
			if task.State != TaskSucceeded {
				allOK = false
			}
		}
		if j.State == JobSucceeded && !allOK {
			t.Errorf("invariant: job %s SUCCEEDED but not all tasks succeeded", j.ID)
		}
		if allOK && j.State != JobSucceeded {
			t.Errorf("invariant: job %s all tasks succeeded but state=%s", j.ID, j.State)
		}
	}
}
