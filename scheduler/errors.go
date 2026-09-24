package scheduler

import "errors"

// 提交 / 调度过程中可区分的失败原因。
var (
	// ErrDuplicateJob 重复提交：作业 ID 已经存在。
	ErrDuplicateJob = errors.New("duplicate job: job id already exists")
	// ErrInvalidJob 请求非法：空 ID、无任务、资源请求非法等。
	ErrInvalidJob = errors.New("invalid job request")
	// ErrTaskTooLarge 单个任务的资源请求超过所有节点容量，永远无法调度。
	ErrTaskTooLarge = errors.New("task request exceeds any node capacity")
	// ErrInsufficientCapacity 作业所需总资源超过集群可用总量，无法整体调度。
	ErrInsufficientCapacity = errors.New("insufficient cluster capacity for gang scheduling")
)
