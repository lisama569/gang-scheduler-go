package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Clock 可替换的时钟，测试使用虚拟时钟保证调度顺序确定。
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// 默认心跳超时：超过该时间没有心跳的节点判定为故障。
const defaultHeartbeatTimeout = 3 * time.Second

// Scheduler 是单进程内的 gang 调度器核心：所有状态变更都在 mu 保护下进行，
// 每个对外操作结束后原子持久化一次快照。
type Scheduler struct {
	mu               sync.Mutex
	cond             *sync.Cond
	store            Store
	log              Logger
	clock            Clock
	nodes            *nodePool
	queue            *jobQueue
	jobs             map[string]*Job
	taskIndex        map[string]*Task // "jobID/taskID" -> task
	submitSeq        int64
	heartbeatTimeout time.Duration

	recovered bool
	ready     chan struct{}

	// executions 统计每个任务实际启动执行的次数（故障重试会增加），
	// 已成功的任务不得重复执行。仅保存在内存，用于断言不变量。
	executions map[string]int
}

// Option 调度器构造可选项。
type Option func(*Scheduler)

func WithLogger(l Logger) Option { return func(s *Scheduler) { s.log = l } }
func WithClock(c Clock) Option   { return func(s *Scheduler) { s.clock = c } }

// WithHeartbeatTimeout 设置心跳超时阈值。
func WithHeartbeatTimeout(d time.Duration) Option {
	return func(s *Scheduler) { s.heartbeatTimeout = d }
}

// New 创建调度器（尚未恢复，必须先调用 Recover）。
func New(store Store, opts ...Option) *Scheduler {
	s := &Scheduler{
		store:            store,
		log:              newDefaultLogger(),
		clock:            realClock{},
		nodes:            newNodePool(),
		queue:            newJobQueue(),
		jobs:             map[string]*Job{},
		taskIndex:        map[string]*Task{},
		ready:            make(chan struct{}),
		executions:       map[string]int{},
		heartbeatTimeout: defaultHeartbeatTimeout,
	}
	for _, o := range opts {
		o(s)
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// waitReady 等待 Recover 完成。调用方不得持锁。
func (s *Scheduler) waitReady() {
	if !s.recovered {
		// 先在锁外观察一次，避免与 Recover 关闭 channel 产生竞争。
		s.mu.Lock()
		done := s.recovered
		s.mu.Unlock()
		if !done {
			<-s.ready
		}
	}
}

// Recover 从持久化存储恢复状态。必须且只能在任何 AddNode/Submit/Tick 之前调用一次。
// 恢复语义：
//   - 已结束（SUCCEEDED/FAILED）的作业原样保留，绝不复活、不再调度；
//   - 崩溃前 RUNNING 的任务视为执行中断，全部回退为 PENDING（Attempts+1），
//     由后续 Tick 连同其他未完成任务一起重新 gang 放置；
//   - 节点资源账本按任务实际放置重建，杜绝资源重复占用；
//   - 重启后节点一律视为待重新注册（Alive=false），重新上报心跳后才承载任务。
func (s *Scheduler) Recover(ctx context.Context) error {
	s.mu.Lock()
	if s.recovered {
		s.mu.Unlock()
		return errors.New("recover called twice")
	}
	snap, found, err := s.store.Load(ctx)
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("load snapshot: %w", err)
	}
	if found {
		s.restoreLocked(snap)
		s.log.Printf("recover: restored snapshot: %d job(s), %d node(s), nextSeq=%d",
			len(snap.Jobs), len(snap.Nodes), snap.SubmitSeq)
	} else {
		s.log.Printf("recover: no existing snapshot, starting with empty state")
	}
	s.recovered = true
	close(s.ready)
	s.mu.Unlock()
	return nil
}

// restoreLocked 用快照重建内存状态。调用方持锁。
func (s *Scheduler) restoreLocked(snap *snapshot) {
	s.submitSeq = snap.SubmitSeq
	for _, js := range snap.Nodes {
		// 重启后不假定任何节点仍然存活：节点必须重新注册（重新上报心跳）后，
		// 其上任务才可能被重新放置。资源账本从零重建，杜绝重复占用。
		n := &Node{
			ID:              js.ID,
			Capacity:        js.Capacity,
			Used:            Resource{},
			Alive:           false,
			LastBeatAt:      js.LastBeatAt,
			runningTaskKeys: map[string]struct{}{},
		}
		s.nodes.add(n)
		s.log.Printf("recover: node %s requires re-registration after restart", js.ID)
	}
	for _, j := range snap.Jobs {
		jc := &Job{ID: j.ID, Priority: j.Priority, State: j.State, SubmitSeq: j.SubmitSeq}
		jc.Tasks = cloneTasks(j.Tasks)
		hasPending := false
		for _, t := range jc.Tasks {
			if t.State == TaskRunning {
				// 执行中断：回退为待调度，尝试次数 +1，等待整体重新放置。
				s.log.Printf("recover: task %s/%s was RUNNING, reset to PENDING (attempts=%d)",
					jc.ID, t.ID, t.Attempts+1)
				t.State = TaskPending
				t.NodeID = ""
				t.StartedAt = time.Time{}
				t.Attempts++
			}
			if t.State == TaskPending {
				hasPending = true
			}
			s.taskIndex[taskKey(jc.ID, t.ID)] = t
		}
		s.jobs[jc.ID] = jc
		if jc.State != JobSucceeded && jc.State != JobFailed && hasPending {
			s.queue.push(jc)
		}
	}
	for k, v := range snap.Executions {
		s.executions[k] = v
	}
}

// AddNode 注册一个新节点，或在故障节点恢复后重新注册同 ID 节点。
func (s *Scheduler) AddNode(id string, cap Resource) error {
	if id == "" {
		return fmt.Errorf("%w: empty node id", ErrInvalidJob)
	}
	if cap.CPU <= 0 || cap.Memory <= 0 {
		return fmt.Errorf("%w: node capacity must be positive", ErrInvalidJob)
	}
	s.waitReady()
	ctx := context.Background()
	s.mu.Lock()
	if existing, ok := s.nodes.get(id); ok {
		if existing.Alive {
			s.mu.Unlock()
			return fmt.Errorf("node %s already registered and alive", id)
		}
		// 故障节点重新注册：账本应为空（故障时已回收），重新上线。
		existing.Capacity = cap
		existing.Used = Resource{}
		existing.Alive = true
		existing.LastBeatAt = s.clock.Now()
		existing.runningTaskKeys = map[string]struct{}{}
		s.log.Printf("node %s re-registered alive capacity=cpu%.0f/mem%.0f", id, cap.CPU, cap.Memory)
	} else {
		s.nodes.add(&Node{
			ID:              id,
			Capacity:        cap,
			Alive:           true,
			LastBeatAt:      s.clock.Now(),
			runningTaskKeys: map[string]struct{}{},
		})
		s.log.Printf("node %s registered capacity=cpu%.0f/mem%.0f", id, cap.CPU, cap.Memory)
	}
	s.scheduleLocked(s.clock.Now())
	if err := s.persistLocked(ctx); err != nil {
		s.mu.Unlock()
		return err
	}
	s.cond.Broadcast()
	s.mu.Unlock()
	return nil
}

// Heartbeat 节点上报心跳，刷新存活时间。
func (s *Scheduler) Heartbeat(nodeID string, at time.Time) error {
	s.waitReady()
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes.get(nodeID)
	if !ok {
		return fmt.Errorf("unknown node %s", nodeID)
	}
	if !n.Alive {
		return fmt.Errorf("node %s is dead, needs re-registration", nodeID)
	}
	n.LastBeatAt = at
	return nil
}

// Submit 提交作业。
// 拒绝原因（可区分）：
//   - ErrInvalidJob：ID 为空 / 没有任务 / 任务 ID 为空或重复 / 资源请求非正；
//   - ErrDuplicateJob：作业 ID 已存在；
//   - ErrTaskTooLarge：单个任务超过任何单节点容量；
//   - ErrInsufficientCapacity：所有任务需求之和超过存活集群总容量。
//
// 资源“暂时”不足（总量够但当前被占用）不会被拒绝，作业进入队列等待。
func (s *Scheduler) Submit(ctx context.Context, req SubmitRequest) error {
	if err := validateRequest(req); err != nil {
		return err
	}
	s.waitReady()
	s.mu.Lock()
	if _, dup := s.jobs[req.JobID]; dup {
		s.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrDuplicateJob, req.JobID)
	}
	// 提交时做静态可行性判断，永不可行的作业直接拒绝。
	alive := s.nodes.aliveNodes()
	var totalCap Resource
	maxCap := Resource{}
	for _, n := range alive {
		totalCap = totalCap.Add(n.Capacity)
		if n.Capacity.CPU > maxCap.CPU {
			maxCap.CPU = n.Capacity.CPU
		}
		if n.Capacity.Memory > maxCap.Memory {
			maxCap.Memory = n.Capacity.Memory
		}
	}
	var totalReq Resource
	for _, ts := range req.Tasks {
		r := Resource{CPU: ts.CPU, Memory: ts.Memory}
		if !r.FitsIn(maxCap) {
			s.mu.Unlock()
			s.log.Printf("submit rejected job=%s reason=task_too_large task=%s req=cpu%.0f/mem%.0f maxNode=cpu%.0f/mem%.0f",
				req.JobID, ts.ID, r.CPU, r.Memory, maxCap.CPU, maxCap.Memory)
			return fmt.Errorf("%w: job %s task %s (cpu%.0f/mem%.0f)", ErrTaskTooLarge,
				req.JobID, ts.ID, r.CPU, r.Memory)
		}
		totalReq = totalReq.Add(r)
	}
	if !totalReq.FitsIn(totalCap) {
		s.mu.Unlock()
		s.log.Printf("submit rejected job=%s reason=insufficient_capacity req=cpu%.0f/mem%.0f cluster=cpu%.0f/mem%.0f",
			req.JobID, totalReq.CPU, totalReq.Memory, totalCap.CPU, totalCap.Memory)
		return fmt.Errorf("%w: job %s needs cpu%.0f/mem%.0f, cluster has cpu%.0f/mem%.0f",
			ErrInsufficientCapacity, req.JobID, totalReq.CPU, totalReq.Memory, totalCap.CPU, totalCap.Memory)
	}

	s.submitSeq++
	job := &Job{
		ID:        req.JobID,
		Priority:  req.Priority,
		State:     JobPending,
		SubmitSeq: s.submitSeq,
		Tasks:     make([]*Task, 0, len(req.Tasks)),
	}
	for i, ts := range req.Tasks {
		t := &Task{
			ID:       ts.ID,
			JobID:    job.ID,
			Index:    i,
			Request:  Resource{CPU: ts.CPU, Memory: ts.Memory},
			Duration: ts.Duration,
			State:    TaskPending,
		}
		job.Tasks = append(job.Tasks, t)
		s.taskIndex[taskKey(job.ID, t.ID)] = t
	}
	s.jobs[job.ID] = job
	s.queue.push(job)
	s.log.Printf("submit accepted job=%s priority=%d tasks=%d totalReq=cpu%.0f/mem%.0f seq=%d",
		job.ID, job.Priority, len(job.Tasks), totalReq.CPU, totalReq.Memory, job.SubmitSeq)

	s.scheduleLocked(s.clock.Now())
	if err := s.persistLocked(ctx); err != nil {
		s.mu.Unlock()
		return err
	}
	s.cond.Broadcast()
	s.mu.Unlock()
	return nil
}

// SubmitBatch 批量提交多个作业：全部入队后只统一调度一轮。
// 用于在同一资源视图下比较队列内的优先级 / FIFO 顺序；
// 任一请求非法或重复，整批拒绝、不产生任何部分提交。
func (s *Scheduler) SubmitBatch(ctx context.Context, reqs ...SubmitRequest) error {
	for _, req := range reqs {
		if err := validateRequest(req); err != nil {
			return err
		}
	}
	s.waitReady()
	s.mu.Lock()
	// 先做全部冲突/可行性校验，再统一落状态，保证原子性。
	type prepared struct {
		job *Job
	}
	prep := make([]prepared, 0, len(reqs))
	alive := s.nodes.aliveNodes()
	var totalCap, maxCap Resource
	for _, n := range alive {
		totalCap = totalCap.Add(n.Capacity)
		if n.Capacity.CPU > maxCap.CPU {
			maxCap.CPU = n.Capacity.CPU
		}
		if n.Capacity.Memory > maxCap.Memory {
			maxCap.Memory = n.Capacity.Memory
		}
	}
	for _, req := range reqs {
		if _, dup := s.jobs[req.JobID]; dup {
			s.mu.Unlock()
			return fmt.Errorf("%w: %s", ErrDuplicateJob, req.JobID)
		}
		var totalReq Resource
		for _, ts := range req.Tasks {
			r := Resource{CPU: ts.CPU, Memory: ts.Memory}
			if !r.FitsIn(maxCap) {
				s.mu.Unlock()
				return fmt.Errorf("%w: job %s task %s", ErrTaskTooLarge, req.JobID, ts.ID)
			}
			totalReq = totalReq.Add(r)
		}
		if !totalReq.FitsIn(totalCap) {
			s.mu.Unlock()
			return fmt.Errorf("%w: job %s", ErrInsufficientCapacity, req.JobID)
		}
		s.submitSeq++
		job := &Job{ID: req.JobID, Priority: req.Priority, State: JobPending, SubmitSeq: s.submitSeq,
			Tasks: make([]*Task, 0, len(req.Tasks))}
		for i, ts := range req.Tasks {
			job.Tasks = append(job.Tasks, &Task{
				ID: ts.ID, JobID: job.ID, Index: i,
				Request: Resource{CPU: ts.CPU, Memory: ts.Memory}, Duration: ts.Duration,
				State: TaskPending,
			})
		}
		prep = append(prep, prepared{job: job})
	}
	for _, p := range prep {
		s.jobs[p.job.ID] = p.job
		s.queue.push(p.job)
		for _, t := range p.job.Tasks {
			s.taskIndex[taskKey(p.job.ID, t.ID)] = t
		}
		s.log.Printf("submit accepted(batch) job=%s priority=%d tasks=%d seq=%d",
			p.job.ID, p.job.Priority, len(p.job.Tasks), p.job.SubmitSeq)
	}
	s.scheduleLocked(s.clock.Now())
	if err := s.persistLocked(ctx); err != nil {
		s.mu.Unlock()
		return err
	}
	s.cond.Broadcast()
	s.mu.Unlock()
	return nil
}

func validateRequest(req SubmitRequest) error {
	if req.JobID == "" {
		return fmt.Errorf("%w: empty job id", ErrInvalidJob)
	}
	if len(req.Tasks) == 0 {
		return fmt.Errorf("%w: job %s has no tasks", ErrInvalidJob, req.JobID)
	}
	seen := map[string]struct{}{}
	for _, ts := range req.Tasks {
		if ts.ID == "" {
			return fmt.Errorf("%w: job %s contains task with empty id", ErrInvalidJob, req.JobID)
		}
		if _, ok := seen[ts.ID]; ok {
			return fmt.Errorf("%w: job %s has duplicate task id %s", ErrInvalidJob, req.JobID, ts.ID)
		}
		seen[ts.ID] = struct{}{}
		if ts.CPU <= 0 || ts.Memory <= 0 {
			return fmt.Errorf("%w: job %s task %s requests non-positive resource", ErrInvalidJob, req.JobID, ts.ID)
		}
	}
	return nil
}

// Tick 推进一次调度：故障检测 -> 任务完成判定 -> gang 调度 -> 持久化。
func (s *Scheduler) Tick(ctx context.Context) {
	s.waitReady()
	s.mu.Lock()
	now := s.clock.Now()
	s.detectFailuresLocked(now)
	s.completeTasksLocked(now)
	s.scheduleLocked(now)
	if err := s.persistLocked(ctx); err != nil {
		s.log.Printf("persist failed: %v (in-memory state retained)", err)
	}
	s.cond.Broadcast()
	s.mu.Unlock()
}

// detectFailuresLocked 找出血跳超时的节点，回收资源并把其上未完成任务退回队列。
func (s *Scheduler) detectFailuresLocked(now time.Time) {
	for _, n := range s.nodes.aliveNodes() {
		if now.Sub(n.LastBeatAt) <= s.heartbeatTimeout {
			continue
		}
		s.log.Printf("FAILURE detected node=%s lastBeat=%s now=%s timeout=%s; reclaiming resources",
			n.ID, n.LastBeatAt.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano), s.heartbeatTimeout)
		// 收集该节点上未完成任务（快照顺序确定）。
		keys := make([]string, 0, len(n.runningTaskKeys))
		for k := range n.runningTaskKeys {
			keys = append(keys, k)
		}
		sortStrings(keys)
		for _, k := range keys {
			t := s.taskIndex[k]
			if t == nil || t.State != TaskRunning {
				continue // 账本与状态不一致时以任务状态为准，不回收已完成任务
			}
			// 归还资源。
			n.release(k, t.Request)
			t.State = TaskPending
			t.NodeID = ""
			t.StartedAt = time.Time{}
			t.Attempts++
			j := s.jobs[t.JobID]
			if j.State == JobRunning {
				// 部分完成、部分中断的作业重新入队，剩余任务仍作为整体放置。
				if !s.isQueuedLocked(j.ID) {
					s.queue.push(j)
				}
			}
			s.log.Printf("reclaim: task %s state RUNNING->PENDING attempts=%d (succeeded tasks of job keep results)",
				k, t.Attempts)
		}
		n.Alive = false
		n.Used = Resource{}
		n.runningTaskKeys = map[string]struct{}{}
		s.log.Printf("node %s marked dead, resources fully released", n.ID)
	}
	// 回收后重新收敛受影响作业的状态（仅含 PENDING/SUCCEEDED 任务）。
	for _, j := range s.jobs {
		if j.State == JobRunning {
			refreshJobStateLocked(j)
		}
	}
}

func (s *Scheduler) isQueuedLocked(jobID string) bool {
	for _, j := range s.queue.jobs {
		if j.ID == jobID {
			return true
		}
	}
	return false
}

// completeTasksLocked 判定已到达执行时长的 RUNNING 任务为 SUCCEEDED，
// 仅以任务自身状态和启动时间为准，任何节点消息都不会让它二次完成。
func (s *Scheduler) completeTasksLocked(now time.Time) {
	for _, j := range s.jobs {
		changed := false
		for _, t := range j.Tasks {
			if t.State != TaskRunning {
				continue
			}
			if now.Sub(t.StartedAt) < t.Duration {
				continue
			}
			n, ok := s.nodes.get(t.NodeID)
			if !ok {
				// 不应发生（故障检测会先把任务退回）；防御性处理。
				t.State = TaskPending
				t.NodeID = ""
				t.StartedAt = time.Time{}
				changed = true
				continue
			}
			n.release(taskKey(t.JobID, t.ID), t.Request)
			t.State = TaskSucceeded
			changed = true
			s.log.Printf("complete: task %s/%s SUCCEEDED on node=%s (released cpu%.0f/mem%.0f)",
				j.ID, t.ID, n.ID, t.Request.CPU, t.Request.Memory)
		}
		if changed {
			refreshJobStateLocked(j)
		}
	}
	// 任务完成释放了资源：在同一 tick 立即重新尝试 gang 调度，
	// 让排队作业不必再等一个 Tick 周期。
	s.scheduleLocked(now)
}

// refreshJobStateLocked 根据任务结果收敛作业状态：
// 全部 SUCCEEDED 才是 SUCCEEDED；有任务在跑是 RUNNING；
// 否则（仅 PENDING）保持/回退为 PENDING 等待整体调度。
func refreshJobStateLocked(j *Job) {
	allDone := true
	anyRunning := false
	for _, t := range j.Tasks {
		if t.State != TaskSucceeded {
			allDone = false
		}
		if t.State == TaskRunning {
			anyRunning = true
		}
	}
	switch {
	case allDone:
		if j.State != JobSucceeded {
			j.State = JobSucceeded
		}
	case anyRunning:
		j.State = JobRunning
	default:
		j.State = JobPending
	}
}

// scheduleLocked 按优先级 + FIFO 尝试 gang 放置。
// 工作守恒：队首作业放不下时跳过，继续尝试后续作业；每个决策都打印资源依据。
func (s *Scheduler) scheduleLocked(now time.Time) {
	// 一轮放置可能因前面作业占用而跳过后面作业；任务时长可能相同、
	// 完成后在同一时刻释放资源，因此循环到“本轮没有新作业启动”为止，
	// 保证单个 Tick 内调度收敛。
	for {
		if s.queue.len() == 0 {
			return
		}
		ordered := s.queue.ordered()
		s.queue = newJobQueue()
		startedAny := false
		for _, j := range ordered {
			pending := pendingTasksSorted(j)
			if len(pending) == 0 {
				continue
			}
			placements, ok := s.tryPlaceLocked(pending)
			if !ok {
				var need Resource
				for _, t := range pending {
					need = need.Add(t.Request)
				}
				s.log.Printf("defer job=%s priority=%d pendingTasks=%d need=cpu%.0f/mem%.0f reason=not enough free capacity in one gang placement",
					j.ID, j.Priority, len(pending), need.CPU, need.Memory)
				s.queue.push(j)
				continue
			}
			for _, pl := range placements {
				t := pl.t
				n := pl.node
				n.allocate(taskKey(j.ID, t.ID), t.Request)
				t.State = TaskRunning
				t.NodeID = n.ID
				t.StartedAt = now
				s.executions[taskKey(j.ID, t.ID)]++
				s.log.Printf("place: job=%s task=%s -> node=%s consumes cpu%.0f/mem%.0f nodeFreeAfter=cpu%.1f/mem%.1f",
					j.ID, t.ID, n.ID, t.Request.CPU, t.Request.Memory,
					n.available().CPU, n.available().Memory)
			}
			j.State = JobRunning
			startedAny = true
			s.log.Printf("GANG START job=%s: %d task(s) started together", j.ID, len(placements))
		}
		if !startedAny {
			return
		}
	}
}

type placement struct {
	t    *Task
	node *Node
}

// tryPlaceLocked 在存活节点（按 ID 排序）上对任务（按 Index 排序）做首次适配。
// 使用临时余量账本试算：任一任务放不下则整体放弃，绝不部分提交。
func (s *Scheduler) tryPlaceLocked(tasks []*Task) ([]placement, bool) {
	alive := s.nodes.aliveNodes()
	free := make(map[string]Resource, len(alive))
	for _, n := range alive {
		free[n.ID] = n.available()
	}
	result := make([]placement, 0, len(tasks))
	for _, t := range tasks {
		placed := false
		for _, n := range alive {
			if t.Request.FitsIn(free[n.ID]) {
				free[n.ID] = free[n.ID].Sub(t.Request)
				result = append(result, placement{t: t, node: n})
				placed = true
				break
			}
		}
		if !placed {
			return nil, false
		}
	}
	return result, true
}

func pendingTasksSorted(j *Job) []*Task {
	out := make([]*Task, 0, len(j.Tasks))
	for _, t := range j.Tasks {
		if t.State == TaskPending {
			out = append(out, t)
		}
	}
	// Tasks 本身按提交 Index 排列，这里保持该顺序。
	return out
}

func taskKey(jobID, taskID string) string { return jobID + "/" + taskID }

// persistLocked 在持锁状态下生成快照并交给 Store 原子保存。
func (s *Scheduler) persistLocked(ctx context.Context) error {
	snap := s.buildSnapshotLocked()
	return s.store.Save(ctx, snap)
}

func (s *Scheduler) buildSnapshotLocked() *snapshot {
	snap := &snapshot{SubmitSeq: s.submitSeq, Executions: map[string]int{}}
	for k, v := range s.executions {
		snap.Executions[k] = v
	}
	// Job 按 ID 排序输出，快照可 diff、可解释。
	ids := make([]string, 0, len(s.jobs))
	for id := range s.jobs {
		ids = append(ids, id)
	}
	sortStrings(ids)
	for _, id := range ids {
		j := s.jobs[id]
		jc := &Job{ID: j.ID, Priority: j.Priority, State: j.State, SubmitSeq: j.SubmitSeq}
		jc.Tasks = cloneTasks(j.Tasks)
		snap.Jobs = append(snap.Jobs, jc)
	}
	nodeIDs := make([]string, 0, len(s.nodes.nodes))
	for id := range s.nodes.nodes {
		nodeIDs = append(nodeIDs, id)
	}
	sortStrings(nodeIDs)
	for _, id := range nodeIDs {
		snap.Nodes = append(snap.Nodes, s.nodes.nodes[id].clone())
	}
	return snap
}

// JobState 返回作业当前状态（并发安全）。
func (s *Scheduler) JobState(jobID string) (JobState, bool) {
	s.waitReady()
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[jobID]
	if !ok {
		return "", false
	}
	return j.State, true
}

// TaskState 返回单个任务的状态与所在节点。
func (s *Scheduler) TaskState(jobID, taskID string) (TaskState, string, bool) {
	s.waitReady()
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.taskIndex[taskKey(jobID, taskID)]
	if !ok {
		return "", "", false
	}
	return t.State, t.NodeID, true
}

// Executions 返回任务被实际启动执行的次数（用于验证成功任务不重复执行）。
func (s *Scheduler) Executions(jobID, taskID string) int {
	s.waitReady()
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.executions[taskKey(jobID, taskID)]
}

// aliveNodeIDs 供 Runner 周期性心跳使用。
func (s *Scheduler) aliveNodeIDs() []string {
	s.waitReady()
	s.mu.Lock()
	defer s.mu.Unlock()
	ns := s.nodes.aliveNodes()
	out := make([]string, 0, len(ns))
	for _, n := range ns {
		out = append(out, n.ID)
	}
	return out
}

// WaitFor 阻塞等待作业到达终态；ctx 取消/超时时返回错误。
func (s *Scheduler) WaitFor(ctx context.Context, jobID string) (JobState, error) {
	s.waitReady()
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			s.mu.Lock()
			s.cond.Broadcast()
			s.mu.Unlock()
		case <-stop:
		}
	}()
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		if j, ok := s.jobs[jobID]; ok {
			if j.State == JobSucceeded || j.State == JobFailed {
				return j.State, nil
			}
		}
		if err := ctx.Err(); err != nil {
			return "", err
		}
		s.cond.Wait()
	}
}
