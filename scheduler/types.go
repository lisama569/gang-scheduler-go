package scheduler

import "time"

// Resource 表示单个节点或任务请求的资源量（CPU 核数、内存 MB）。
type Resource struct {
	CPU    float64 `json:"cpu"`
	Memory float64 `json:"memory"`
}

// TaskState 任务状态。
type TaskState string

const (
	TaskPending   TaskState = "PENDING"
	TaskRunning   TaskState = "RUNNING"
	TaskSucceeded TaskState = "SUCCEEDED"
)

// JobState 作业状态。
type JobState string

const (
	JobPending   JobState = "PENDING"
	JobRunning   JobState = "RUNNING"
	JobSucceeded JobState = "SUCCEEDED"
	JobFailed    JobState = "FAILED" // 保留：当前调度器不会主动产生失败作业
)

// Task 作业中的单个任务。
type Task struct {
	ID       string        `json:"id"`
	JobID    string        `json:"job_id"`
	Index    int           `json:"index"`
	Request  Resource      `json:"request"`
	Duration time.Duration `json:"duration"`
	State    TaskState     `json:"state"`
	NodeID   string        `json:"node_id,omitempty"`
	// StartedAt 任务在模拟时钟上启动的时刻，用于判定何时执行完成。
	StartedAt time.Time `json:"started_at,omitempty"`
	// Attempts 任务被（因故障等原因）重新调度的次数。
	Attempts int `json:"attempts"`
}

// Job 一组必须整体调度（gang scheduling）的任务集合。
type Job struct {
	ID       string   `json:"id"`
	Priority int      `json:"priority"`
	Tasks    []*Task  `json:"tasks"`
	State    JobState `json:"state"`
	// SubmitSeq 提交序号，优先级相同时先提交先调度（FIFO）。
	SubmitSeq int64 `json:"submit_seq"`
}

// Node 集群中的模拟节点。
type Node struct {
	ID         string    `json:"id"`
	Capacity   Resource  `json:"capacity"`
	Used       Resource  `json:"used"`
	Alive      bool      `json:"alive"`
	LastBeatAt time.Time `json:"last_beat_at"`
	// runningTaskKeys 该节点上正在运行的 "jobID/taskID" 集合。
	runningTaskKeys map[string]struct{}
}

// Request 作业提交请求。
type SubmitRequest struct {
	JobID    string
	Priority int
	Tasks    []TaskSpec
}

// TaskSpec 提交时对单个任务的描述。
type TaskSpec struct {
	ID       string
	CPU      float64
	Memory   float64
	Duration time.Duration
}

// snapshot 是持久化到磁盘 / 内存的完整调度状态。
type snapshot struct {
	SubmitSeq  int64           `json:"submit_seq"`
	Jobs       []*Job          `json:"jobs"`
	Nodes      []*nodeSnapshot `json:"nodes"`
	Executions map[string]int  `json:"executions,omitempty"`
}

// nodeSnapshot 节点的可持久化视图（Node 里有 map，不能直接序列化）。
type nodeSnapshot struct {
	ID         string    `json:"id"`
	Capacity   Resource  `json:"capacity"`
	Used       Resource  `json:"used"`
	Alive      bool      `json:"alive"`
	LastBeatAt time.Time `json:"last_beat_at"`
	Running    []string  `json:"running"`
}

// Add 返回 r+x（不修改接收者）。
func (r Resource) Add(x Resource) Resource {
	return Resource{CPU: r.CPU + x.CPU, Memory: r.Memory + x.Memory}
}

// Sub 返回 r-x。
func (r Resource) Sub(x Resource) Resource {
	return Resource{CPU: r.CPU - x.CPU, Memory: r.Memory - x.Memory}
}

// FitsIn 判断 r 是否能放入容量 cap（两种资源都不超）。
func (r Resource) FitsIn(cap Resource) bool {
	return r.CPU <= cap.CPU+1e-9 && r.Memory <= cap.Memory+1e-9
}

// Clone 深拷贝节点（含运行中任务集合）。
func (n *Node) clone() *nodeSnapshot {
	running := make([]string, 0, len(n.runningTaskKeys))
	for k := range n.runningTaskKeys {
		running = append(running, k)
	}
	sortStrings(running)
	return &nodeSnapshot{
		ID:         n.ID,
		Capacity:   n.Capacity,
		Used:       n.Used,
		Alive:      n.Alive,
		LastBeatAt: n.LastBeatAt,
		Running:    running,
	}
}

// cloneTasks 深拷贝任务指针切片（快照隔离，避免恢复后共享底层结构）。
func cloneTasks(tasks []*Task) []*Task {
	out := make([]*Task, len(tasks))
	for i, t := range tasks {
		cp := *t
		out[i] = &cp
	}
	return out
}
