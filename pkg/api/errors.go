package api

import "errors"

// 作业提交被拒绝的三类可区分原因（另含作业不存在等运行期错误）。
var (
	// ErrDuplicateJob 重复提交：作业 ID 已存在（含已结束作业，结束作业不会复活）。
	ErrDuplicateJob = errors.New("duplicate job: job id already exists")
	// ErrInvalidRequest 请求非法：ID 为空、无任务、资源非法、执行节拍非法等。
	ErrInvalidRequest = errors.New("invalid job request")
	// ErrInsufficientResources 资源不足：作业在当前集群拓扑下永远无法被整体调度。
	ErrInsufficientResources = errors.New("insufficient resources: job can never fit the cluster")
	// ErrJobNotFound 查询/等待的作业不存在。
	ErrJobNotFound = errors.New("job not found")

	// 节点维护/排空相关的可区分错误。
	// ErrNodeNotFound 操作的节点未注册。
	ErrNodeNotFound = errors.New("node not found")
	// ErrNodeNotHealthy 排空目标节点不是健康状态（已排空中或已死亡）。
	ErrNodeNotHealthy = errors.New("node is not healthy")
	// ErrNodeNotDraining 完成退役的目标节点不在排空状态。
	ErrNodeNotDraining = errors.New("node is not draining")
	// ErrNodeNotEmpty 节点上仍有未迁走的任务，不能退役。
	ErrNodeNotEmpty = errors.New("node still has running tasks")
	// ErrNodeRetired 节点此前已完成排空并永久退役。重启后直接 AddNode
	// 重新注册同名节点会得到此错误：已下线节点不能靠重新注册复活，
	// 必须由运维显式调用 ReadmitNode 才能重新上线。
	ErrNodeRetired = errors.New("node is retired")
	// ErrNodeNotRetired ReadmitNode 的目标节点并不在已退役名单中。
	ErrNodeNotRetired = errors.New("node is not retired")
)

// RejectError 包装拒绝原因，便于调用方 errors.Is 判断并打印明细。
type RejectError struct {
	Kind error
	Msg  string
}

func (e *RejectError) Error() string { return e.Kind.Error() + ": " + e.Msg }
func (e *RejectError) Unwrap() error { return e.Kind }
