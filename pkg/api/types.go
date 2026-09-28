package api

// Resource 表示一个节点或一个任务请求的计算资源量。
type Resource struct {
	CPU    int
	Memory int
}

// 节点状态。
const (
	NodeHealthy  = "Healthy"  // 正常服役：可接收新任务
	NodeDraining = "Draining" // 计划内维护/排空中：不接收新任务，已有任务安全迁出
	NodeDead     = "Dead"     // 心跳超时故障：任务按丢失回收重调度
	NodeRetired  = "Retired"  // 排空完成后正式退役：容量从集群可用容量中扣除
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
}

// NodeRuntime 是模拟节点的运行时视图。
type NodeRuntime struct {
	ID       string
	Capacity Resource
	Used     Resource
	State    string
	LastBeat int64 // 最近一次心跳的逻辑时钟
}

// NodeRecord 是持久化到状态存储的节点记录：拓扑容量 + 生命周期状态。
type NodeRecord struct {
	ID       string
	Capacity Resource
	State    string
}

// jobRecord / taskRecord 是持久化到状态存储的快照记录。
type TaskRecord struct {
	Spec    TaskSpec
	State   string
	NodeID  string
	Attempt int
}

type JobRecord struct {
	Spec   JobSpec
	State  string
	Reason string
	Tasks  []TaskRecord
}

// DrainProgress 描述一个计划内维护节点的排空进展（供观测与测试断言）。
type DrainProgress struct {
	NodeID    string
	State     string // NodeDraining（排空中）或 NodeRetired（已退役）
	Remaining int    // 尚未迁走的在执行行任务数；退役时必为 0
}

// snapshot 是状态存储落盘的完整快照。
type Snapshot struct {
	Counter int
	Jobs    []JobRecord
	Nodes   []NodeRecord
}
