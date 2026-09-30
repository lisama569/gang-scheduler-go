package api

// Resource 表示一个节点或一个任务请求的计算资源量。
type Resource struct {
	CPU    int
	Memory int
}

// 节点状态。
const (
	NodeHealthy  = "Healthy"
	NodeDraining = "Draining" // 计划内维护：不再接收新任务，存量任务迁移中
	NodeDead     = "Dead"
)

// 任务状态。
const (
	TaskPending   = "Pending"
	TaskRunning   = "Running"
	TaskSucceeded = "Succeeded"
	TaskFailed    = "Failed"
)

// 作业状态。
const (
	JobPending   = "Pending"   // 已接受，等待整体获得资源
	JobRunning   = "Running"   // 全部任务已一起启动
	JobSucceeded = "Succeeded" // 全部任务成功
	JobFailed    = "Failed"    // 有任务失败
)

// TaskSpec 描述一个任务的资源请求与模拟执行节拍数。
type TaskSpec struct {
	Name      string
	Resources Resource
	Duration  int // 模拟执行需要的 tick 数，必须 > 0
}

// JobSpec 提交一个作业所需的全部信息。
type JobSpec struct {
	ID       string
	Priority int // 数值越大优先级越高
	Tasks    []TaskSpec
}

// JobRuntime 是作业的运行时视图。
type JobRuntime struct {
	Spec   JobSpec
	State  string
	Tasks  []TaskRuntime
	Reason string // 当作业被拒绝或失败时的可区分原因
}

// TaskRuntime 是任务的运行时视图。
type TaskRuntime struct {
	Spec    TaskSpec
	State   string
	NodeID  string // 运行所在节点；Pending 时为空
	Attempt int
	// Progress 是任务已经执行完的 tick 数。
	// 计划内排空迁移属于主动安全迁移（不是故障丢失），任务带着已执行进度
	// 在新节点上接着跑，迁移前后进度保持；Attempt 也不因此增长。
	Progress int
}

// NodeRuntime 是模拟节点的运行时视图。
type NodeRuntime struct {
	ID       string
	Capacity Resource
	Used     Resource
	State    string
	LastBeat int64 // 最近一次心跳的逻辑时钟
}

// DrainInfo 描述一个节点维护/排空的进展（供观测与测试断言）。
type DrainInfo struct {
	NodeID             string
	State              string   // 节点当前状态（Draining/Dead/...）
	RemainingTasks     int      // 仍跑在该节点上、尚未迁走的任务数
	RemainingResources Resource // 这些任务仍占用的资源量
}

// jobRecord / taskRecord 是持久化到状态存储的快照记录。
type TaskRecord struct {
	Spec     TaskSpec
	State    string
	NodeID   string
	Attempt  int
	Progress int
}

type JobRecord struct {
	Spec   JobSpec
	State  string
	Reason string
	Tasks  []TaskRecord
}

// snapshot 是状态存储落盘的完整快照。
type Snapshot struct {
	Counter int
	Jobs    []JobRecord
	// Draining 记录处于维护/排空中的节点 ID，保证重启恢复后
	// 这些节点不会被当成正常机器重新投入调度。
	Draining []string
	// Retired 记录已完成排空、正式退役（Dead）的节点 ID。
	// 退役是永久容量收缩：重启后重新注册同名节点不会让它复活、
	// 容量也不会涨回去，除非运维显式调用 ReadmitNode 重新上线。
	Retired []string
}
