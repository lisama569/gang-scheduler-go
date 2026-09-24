package scheduler

import "sort"

// jobQueue 待调度作业队列：优先级降序，同优先级按提交序号升序（FIFO）。
// 不做增量堆，每次调度前重排，顺序完全确定、可解释。
type jobQueue struct {
	jobs []*Job
}

func newJobQueue() *jobQueue {
	return &jobQueue{}
}

func (q *jobQueue) len() int { return len(q.jobs) }

func (q *jobQueue) push(j *Job) {
	q.jobs = append(q.jobs, j)
}

// ordered 返回按调度策略排好序的作业副本。
// 规则：priority 大者优先；priority 相同则 submitSeq 小者（先提交）优先。
func (q *jobQueue) ordered() []*Job {
	out := make([]*Job, len(q.jobs))
	copy(out, q.jobs)
	sort.SliceStable(out, func(i, k int) bool {
		if out[i].Priority != out[k].Priority {
			return out[i].Priority > out[k].Priority
		}
		return out[i].SubmitSeq < out[k].SubmitSeq
	})
	return out
}
