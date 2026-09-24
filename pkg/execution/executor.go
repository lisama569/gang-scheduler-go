package execution

import (
	"sync"

	"github.com/lisama569/gang-scheduler-go/pkg/api"
	"github.com/lisama569/gang-scheduler-go/pkg/cluster"
)

// EventKind 标识任务生命周期事件的类型。
type EventKind int

const (
	EvFinished EventKind = iota // 正常执行到节拍结束（含故障注入导致的失败）
	EvKilled                    // 节点故障被强制终止
)

// Event 是 executor 发往调度器的任务事件。
type Event struct {
	Kind     EventKind
	JobID    string
	TaskName string
	NodeID   string
	Request  api.Resource
	OK       bool
}

// Executor 在模拟节点上驱动任务执行：按 tick 推进剩余节拍，
// 支持强制终止（节点故障）与故障注入（指定任务失败）。
// 所有状态变更由自身互斥锁保护；不直接回调调度器，而是通过
// 带缓冲的事件通道投递，避免 pool/scheduler/executor 之间锁重入。
type Executor struct {
	mu     sync.Mutex
	pool   *cluster.Pool
	events chan<- Event

	// running 以 jobID/taskName/nodeID 三元组为键记录在执行行。
	running map[string]*inflight
}

type inflight struct {
	jobID     string
	taskName  string
	nodeID    string
	request   api.Resource
	remaining int
}

func inflightKey(jobID, taskName string) string { return jobID + "\x00" + taskName }

func New(pool *cluster.Pool, events chan<- Event) *Executor {
	return &Executor{pool: pool, events: events, running: map[string]*inflight{}}
}

// Launch 登记一个开始执行的任务。重复登记同一任务会被拒绝（防御重复执行）。
func (e *Executor) Launch(jobID string, t api.TaskRuntime) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	key := inflightKey(jobID, t.Spec.Name)
	if _, ok := e.running[key]; ok {
		return false
	}
	e.running[key] = &inflight{
		jobID:     jobID,
		taskName:  t.Spec.Name,
		nodeID:    t.NodeID,
		request:   t.Spec.Resources,
		remaining: t.Spec.Duration,
	}
	return true
}

// KillOnNode 终止某节点上的全部在执行行：释放节点账本资源并投递 EvKilled，
// 由调度器将这些任务复位为 Pending 重新调度（已完成任务不在执行行中，不受影响）。
func (e *Executor) KillOnNode(nodeID string) {
	e.mu.Lock()
	var killed []*inflight
	for key, inf := range e.running {
		if inf.nodeID == nodeID {
			killed = append(killed, inf)
			delete(e.running, key)
		}
	}
	e.mu.Unlock()

	for _, inf := range killed {
		e.pool.Release(nodeID, inf.request)
		e.events <- Event{
			Kind:     EvKilled,
			JobID:    inf.jobID,
			TaskName: inf.taskName,
			NodeID:   nodeID,
			Request:  inf.request,
		}
	}
}

// Tick 推进所有在执行行一个节拍；到达 0 的任务正常结束并投递 EvFinished。
func (e *Executor) Tick() {
	e.mu.Lock()
	var finished []*inflight
	for key, inf := range e.running {
		inf.remaining--
		if inf.remaining <= 0 {
			finished = append(finished, inf)
			delete(e.running, key)
		}
	}
	e.mu.Unlock()

	for _, inf := range finished {
		e.pool.Release(inf.nodeID, inf.request)
		e.events <- Event{
			Kind:     EvFinished,
			JobID:    inf.jobID,
			TaskName: inf.taskName,
			NodeID:   inf.nodeID,
			Request:  inf.request,
			OK:       true,
		}
	}
}

// InjectFailure 令指定任务立即失败：释放资源并投递失败的 EvFinished。
func (e *Executor) InjectFailure(jobID, taskName string) bool {
	e.mu.Lock()
	key := inflightKey(jobID, taskName)
	inf, ok := e.running[key]
	if !ok {
		e.mu.Unlock()
		return false
	}
	delete(e.running, key)
	e.mu.Unlock()

	e.pool.Release(inf.nodeID, inf.request)
	e.events <- Event{
		Kind:     EvFinished,
		JobID:    inf.jobID,
		TaskName: inf.taskName,
		NodeID:   inf.nodeID,
		Request:  inf.request,
		OK:       false,
	}
	return true
}

func (e *Executor) Running(jobID, taskName string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, ok := e.running[inflightKey(jobID, taskName)]
	return ok
}

func (e *Executor) Inflight() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.running)
}

// Reset 在进程重启恢复后清空执行行（模拟执行器随进程一起重建）。
func (e *Executor) Reset() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.running = map[string]*inflight{}
}
