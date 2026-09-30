package scheduler

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"log/slog"

	"github.com/lisama569/gang-scheduler-go/pkg/api"
	"github.com/lisama569/gang-scheduler-go/pkg/cluster"
	"github.com/lisama569/gang-scheduler-go/pkg/execution"
	"github.com/lisama569/gang-scheduler-go/pkg/state"
)

// Config 调度器配置。
type Config struct {
	// HeartbeatTimeoutTicks 心跳超时的 tick 数：now-lastBeat > 该值即判死。
	HeartbeatTimeoutTicks int64
	// StateFile 快照持久化路径；为空表示仅内存模式。
	StateFile string
	// Logger 可选日志器；为 nil 时使用 slog 默认 logger。
	Logger *slog.Logger
}

// Scheduler 是单进程内的模拟集群 gang 调度器。
type Scheduler struct {
	mu      sync.Mutex
	pool    *cluster.Pool
	store   *state.Store
	exec    *execution.Executor
	events  chan execution.Event
	timeout int64
	clock   int64
	log     *slog.Logger

	// alive 记录本 tick 仍“存活”的节点；TriggerNodeFailure 将其摘除，
	// 随后该节点心跳停止刷新，超时后被统一的检测路径回收。
	alive map[string]bool

	// waiters 为 WaitJob 提供作业终态通知。
	waiters map[string][]chan struct{}
}

func New(cfg Config) *Scheduler {
	if cfg.HeartbeatTimeoutTicks <= 0 {
		cfg.HeartbeatTimeoutTicks = 2
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	events := make(chan execution.Event, 1024)
	pool := cluster.NewPool()
	store := state.New(cfg.StateFile)
	// 先把快照中的节点标记（排空/退役名单）加载进内存：
	// 即使调用方在 Recover 之前就 AddNode，已退役节点也会被立刻拦住，
	// 不会被当成新机器注册、让容量涨回去。
	if err := store.LoadMarkers(); err != nil {
		log.Warn("failed to preload node markers from snapshot", "err", err.Error())
	}
	s := &Scheduler{
		pool:    pool,
		store:   store,
		exec:    execution.New(pool, events),
		events:  events,
		timeout: cfg.HeartbeatTimeoutTicks,
		alive:   map[string]bool{},
		waiters: map[string][]chan struct{}{},
		log:     log,
	}
	return s
}

// AddNode 注册一个模拟节点。
//
// 若该节点 ID 在持久化快照中已被标记为“正式退役”（排空完成后的永久下线），
// AddNode 会返回 api.ErrNodeRetired 拒绝注册：已下线节点不会因为重启后
// 重新注册而被当成健康机器复活，集群容量也不会因此涨回去。运维确实要把
// 这台机器重新上线时，必须显式调用 ReadmitNode。
func (s *Scheduler) AddNode(id string, cap api.Resource) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.store.IsRetired(id) {
		s.log.Warn("node registration rejected: node is retired (capacity permanently removed)",
			"node", id)
		return api.ErrNodeRetired
	}
	if err := s.pool.AddNode(id, cap); err != nil {
		return err
	}
	s.alive[id] = true
	// 若该节点在恢复的快照中被标记为排空中（例如重启后重新注册），
	// 立即恢复其排空状态，绝不把维护中的节点当成正常机器用。
	if s.store.IsDraining(id) {
		if err := s.pool.BeginDrain(id); err != nil {
			s.log.Warn("failed to re-apply drain state on registered node",
				"node", id, "err", err.Error())
		}
	}
	s.log.Info("node registered", "node", id, "capacity", cap)
	return nil
}

// ReadmitNode 显式让一台此前已退役（CompleteNodeDrain 正式下线）的节点重新上线：
// 节点恢复为 Healthy、以给定容量重新计入集群并恢复心跳。
// 这是运维的**显式重新上线**操作，与“重启后重新 AddNode”不同——后者会被拒绝，
// 保证已下线节点不可能被隐式复活。
// 节点既不在退役名单、节点池里也没有（从未出现过的 ID）时返回
// ErrNodeNotRetired；普通新机器请走 AddNode。
func (s *Scheduler) ReadmitNode(id string, cap api.Resource) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.store.IsRetired(id) {
		s.log.Warn("readmit rejected: node was never retired", "node", id)
		return api.ErrNodeNotRetired
	}
	if err := s.pool.ReadmitNode(id, cap); err != nil {
		return err
	}
	if err := s.store.ClearNodeRetired(id); err != nil {
		return fmt.Errorf("persist readmit state: %w", err)
	}
	s.alive[id] = true
	s.log.Info("retired node explicitly readmitted as healthy", "node", id, "capacity", cap)
	return nil
}

// Submit 校验并提交一个作业。三类可区分拒绝原因：
// ErrInvalidRequest / ErrInsufficientResources / ErrDuplicateJob。
func (s *Scheduler) Submit(spec api.JobSpec) error {
	if err := validate(spec); err != nil {
		s.log.Warn("job rejected: invalid request", "job", spec.ID, "reason", err.Error())
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// 重复提交（含已结束作业）直接拒绝，已结束作业不会复活。
	if _, ok := s.store.Get(spec.ID); ok {
		s.log.Warn("job rejected: duplicate", "job", spec.ID)
		return &api.RejectError{Kind: api.ErrDuplicateJob, Msg: "job id " + spec.ID}
	}

	// 容量校验：任一任务单节点放不下，或健康资源总量永远不足，
	// 则该作业在当前集群拓扑下永不可调度，直接拒绝（不入队）。
	if reason, ok := s.fitsClusterLocked(spec); !ok {
		s.log.Warn("job rejected: insufficient resources", "job", spec.ID, "reason", reason)
		return &api.RejectError{Kind: api.ErrInsufficientResources, Msg: reason}
	}

	j := &api.JobRuntime{
		Spec:  spec,
		State: api.JobPending,
		Tasks: make([]api.TaskRuntime, len(spec.Tasks)),
	}
	for i, t := range spec.Tasks {
		j.Tasks[i] = api.TaskRuntime{Spec: t, State: api.TaskPending}
	}
	// Add（查重+落盘）在 s.mu 内完成：与 Recover 的整表替换互斥，
	// 杜绝“提交后被恢复快照覆盖”造成的作业丢失。
	if err := s.store.Add(j); err != nil {
		if errors.Is(err, api.ErrDuplicateJob) {
			return err
		}
		return fmt.Errorf("persist job: %w", err)
	}

	s.log.Info("job accepted and queued",
		"job", spec.ID, "priority", spec.Priority,
		"tasks", len(spec.Tasks), "totalRequest", api.SumRequests(spec.Tasks))
	return nil
}

// validate 做与集群无关的静态合法性检查。
func validate(spec api.JobSpec) error {
	if spec.ID == "" {
		return &api.RejectError{Kind: api.ErrInvalidRequest, Msg: "empty job id"}
	}
	if len(spec.Tasks) == 0 {
		return &api.RejectError{Kind: api.ErrInvalidRequest, Msg: "job has no tasks"}
	}
	names := map[string]bool{}
	for i, t := range spec.Tasks {
		if t.Name == "" {
			return &api.RejectError{Kind: api.ErrInvalidRequest, Msg: fmt.Sprintf("task[%d] empty name", i)}
		}
		if names[t.Name] {
			return &api.RejectError{Kind: api.ErrInvalidRequest, Msg: "duplicate task name " + t.Name}
		}
		names[t.Name] = true
		if t.Resources.CPU < 0 || t.Resources.Memory < 0 {
			return &api.RejectError{Kind: api.ErrInvalidRequest, Msg: "negative resource in task " + t.Name}
		}
		if t.Duration <= 0 {
			return &api.RejectError{Kind: api.ErrInvalidRequest, Msg: "duration must be positive for task " + t.Name}
		}
	}
	return nil
}

// fitsClusterLocked 判断作业在当前（静态）集群拓扑下是否“有可能”整体放下：
// 1) 每个任务必须能放进单个节点；2) 所有任务请求之和不超过健康容量之和。
// 注意：这是可行性判断，不是本轮可调度判断；当前被占用导致的暂时不足仍排队等待。
func (s *Scheduler) fitsClusterLocked(spec api.JobSpec) (string, bool) {
	var totalCap, totalReq api.Resource
	var maxNode api.Resource
	var healthyIDs []string
	for _, n := range s.pool.Snapshot() {
		if n.State == api.NodeHealthy {
			totalCap = totalCap.Add(n.Capacity)
			if n.Capacity.CPU > maxNode.CPU {
				maxNode.CPU = n.Capacity.CPU
			}
			if n.Capacity.Memory > maxNode.Memory {
				maxNode.Memory = n.Capacity.Memory
			}
			healthyIDs = append(healthyIDs, n.ID)
		}
	}
	if len(healthyIDs) == 0 {
		return "no healthy node in cluster", false
	}
	for _, t := range spec.Tasks {
		if !t.Resources.Fits(maxNode) {
			return fmt.Sprintf("task %s request %+v exceeds largest single-node capacity %+v", t.Name, t.Resources, maxNode), false
		}
		totalReq = totalReq.Add(t.Resources)
	}
	if !totalReq.Fits(totalCap) {
		return fmt.Sprintf("total request %+v exceeds healthy capacity %+v", totalReq, totalCap), false
	}
	return "", true
}

// GetJob 返回作业当前状态的副本。
func (s *Scheduler) GetJob(id string) (api.JobRuntime, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.store.Get(id)
	if !ok {
		return api.JobRuntime{}, api.ErrJobNotFound
	}
	return *j, nil
}

// TriggerNodeFailure 模拟节点故障注入：停止为该节点刷新心跳，
// 之后由统一的超时检测路径完成标记死亡、回收资源与任务重调度。
func (s *Scheduler) TriggerNodeFailure(nodeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.alive[nodeID] {
		return
	}
	delete(s.alive, nodeID)
	s.log.Warn("node failure injected, heartbeat stopped", "node", nodeID, "clock", s.clock)
}

// DrainNode 主动把一个健康节点置为维护/排空中：
//   - 从这一刻起它不再接收新任务（放置与容量可行性判断都把它排除，
//     集群可用容量立即按收缩后计算）；
//   - 上面仍在运行的任务不被判丢：从下一个 Tick 起被安全驱逐、复位为
//     Pending，随调度循环迁移到其它有余量的健康节点上继续跑；
//   - 节点心跳照常刷新（这是计划内维护，不是故障）。
//
// 节点不存在返回 ErrNodeNotFound；节点已排空中或已死亡返回 ErrNodeNotHealthy。
// 迁移随 Tick 异步推进：用 DrainStatus 观察还剩多少任务没迁完，
// 全部迁走后用 CompleteNodeDrain 正式退役该节点。
func (s *Scheduler) DrainNode(nodeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.pool.BeginDrain(nodeID); err != nil {
		s.log.Warn("drain rejected", "node", nodeID, "reason", err.Error())
		return err
	}
	// 先落盘排空标记，任务驱逐由后续 Tick 推进：即使此刻进程崩溃，
	// 恢复后该节点仍是排空中，不会被当成健康机器重新投入调度。
	if err := s.store.SetDraining(nodeID, true); err != nil {
		return fmt.Errorf("persist drain state: %w", err)
	}
	info := s.drainInfoLocked(nodeID)
	s.log.Info("node marked draining, tasks will be evicted on next ticks",
		"node", nodeID, "clock", s.clock,
		"remainingTasks", info.RemainingTasks,
		"remainingResources", info.RemainingResources)
	return nil
}

// CompleteNodeDrain 在节点排空完成（其上任务全部迁走或本来就没任务）后
// 将其正式退役：节点置为 Dead，容量从集群可用容量中永久扣除。
// 仍有任务未迁完时返回 ErrNodeNotEmpty，节点保持排空中。
func (s *Scheduler) CompleteNodeDrain(nodeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 以调度器的任务视图为准复核：不允许还有 Running 任务挂在该节点上
	// 就退役（pool 账本与任务视图在 s.mu 下一致，这里双保险）。
	info := s.drainInfoLocked(nodeID)
	if info.RemainingTasks > 0 {
		s.log.Warn("drain complete rejected: tasks still migrating",
			"node", nodeID, "remainingTasks", info.RemainingTasks)
		return api.ErrNodeNotEmpty
	}
	if err := s.pool.CompleteDrain(nodeID); err != nil {
		return err
	}
	// 退役后把节点从排空名单移入“已退役”名单并一次性落盘：
	// 这是永久容量收缩——重启后重新注册同名节点会被 AddNode 拒绝，
	// 容量不会涨回来；只有运维显式 ReadmitNode 才能重新上线。
	// 节点不再参与心跳（已 Dead）。
	if err := s.store.MarkNodeRetired(nodeID); err != nil {
		return fmt.Errorf("persist retired state: %w", err)
	}
	delete(s.alive, nodeID)
	s.log.Info("node drain complete, node retired",
		"node", nodeID, "clock", s.clock)
	return nil
}

// DrainStatus 返回节点维护/排空的进展：还剩多少任务、多少资源没迁完。
func (s *Scheduler) DrainStatus(nodeID string) (api.DrainInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, n := range s.pool.Snapshot() {
		if n.ID == nodeID {
			return s.drainInfoLocked(nodeID), nil
		}
	}
	return api.DrainInfo{}, api.ErrNodeNotFound
}

// drainInfoLocked 汇总指定节点的排空进展（调用方须持 s.mu）。
func (s *Scheduler) drainInfoLocked(nodeID string) api.DrainInfo {
	info := api.DrainInfo{NodeID: nodeID}
	for _, n := range s.pool.Snapshot() {
		if n.ID == nodeID {
			info.State = n.State
			info.RemainingResources = n.Used
			break
		}
	}
	for _, j := range s.store.List() {
		for _, t := range j.Tasks {
			if t.State == api.TaskRunning && t.NodeID == nodeID {
				info.RemainingTasks++
			}
		}
	}
	return info
}

// Nodes 返回全部节点的资源/状态快照（供观测与测试断言）。
func (s *Scheduler) Nodes() []api.NodeRuntime {
	return s.pool.Snapshot()
}

// Clock 返回当前逻辑时钟（主要供测试断言与日志解释）。
func (s *Scheduler) Clock() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.clock
}

// WaitJob 阻塞等待作业进入终态（Succeeded/Failed）。
func (s *Scheduler) WaitJob(id string) (api.JobRuntime, error) {
	ch := make(chan struct{}, 1)
	s.mu.Lock()
	j, ok := s.store.Get(id)
	if !ok {
		s.mu.Unlock()
		return api.JobRuntime{}, api.ErrJobNotFound
	}
	if j.State == api.JobSucceeded || j.State == api.JobFailed {
		s.mu.Unlock()
		return *j, nil
	}
	s.waiters[id] = append(s.waiters[id], ch)
	s.mu.Unlock()

	<-ch
	s.mu.Lock()
	defer s.mu.Unlock()
	j2, _ := s.store.Get(id)
	return *j2, nil
}

func (s *Scheduler) notifyWaiters(id string) {
	for _, ch := range s.waiters[id] {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	delete(s.waiters, id)
}

// Tick 推进一个逻辑时钟周期。事件驱动测试可直接逐 tick 调用，
// Run 则在 goroutine 中按固定时间间隔反复调用 Tick。
func (s *Scheduler) Tick() {
	s.mu.Lock()
	now := s.clock + 1

	// 1) 处理上一轮 executor 投递的任务事件（成功/失败/被杀）。
	s.drainEventsLocked()

	// 2) 心跳：仍存活的健康节点刷新时间戳。
	s.pool.BeatAll(now, s.alive)

	// 3) 超时检测：回收死亡节点并复位其上未完成任务。
	for _, dead := range s.pool.DetectDead(now, s.timeout) {
		s.log.Warn("node heartbeat timeout, reclaiming",
			"node", dead.ID, "clock", now, "released", dead.Released)
		s.exec.KillOnNode(dead.ID) // 产生的 EvKilled 进入通道
		s.drainEventsLocked()      // 同步完成任务复位，保证本 tick 状态可断言
		// 若该节点此前处于计划内排空中，故障回收使它直接进入 Dead：
		// 排空生命周期到此终止，清除残留排空标记（它不是退役，
		// 也不进入退役名单），避免重启后标记悬空。
		if s.store.IsDraining(dead.ID) {
			if err := s.store.SetDraining(dead.ID, false); err != nil {
				s.log.Warn("clear stale draining marker failed",
					"node", dead.ID, "err", err.Error())
			}
		}
		delete(s.alive, dead.ID)
	}

	// 3.5) 计划内维护：驱逐排空中节点上仍在运行的任务（安全迁移，不算故障丢失）。
	// 被驱逐任务复位为 Pending 后，在本 tick 第 4 步即有机会整体迁到健康节点。
	for _, id := range s.store.Draining() {
		s.exec.EvictOnNode(id) // 产生的 EvDrained 进入通道
	}
	s.drainEventsLocked()

	// 4) gang 调度队列：优先级高者先尝试，放不下则跳过尝试后续作业。
	s.schedulePendingLocked(now)

	// 5) 推进执行。未结束任务的最新进度按作业聚合后落盘：
	// 计划内迁移或崩溃都能带着进度续跑，而不是从头重跑。
	progress := s.exec.Tick()
	s.drainEventsLocked() // 立即吸纳本轮完成事件，使 Tick 返回时状态确定
	s.persistProgressLocked(progress)

	s.clock = now
	s.mu.Unlock()
}

// persistProgressLocked 把本 tick 各在执行行的最新进度聚合到作业快照中。
// 同一作业的多个任务只触发一次落盘，避免每个任务一次磁盘写入。
func (s *Scheduler) persistProgressLocked(ticks []execution.ProgressTick) {
	if len(ticks) == 0 {
		return
	}
	byJob := map[string]map[string]int{}
	for _, tk := range ticks {
		if byJob[tk.JobID] == nil {
			byJob[tk.JobID] = map[string]int{}
		}
		byJob[tk.JobID][tk.TaskName] = tk.Progress
	}
	for jobID, taskProgress := range byJob {
		if _, err := s.store.Update(jobID, func(live *api.JobRuntime) {
			for i := range live.Tasks {
				t := &live.Tasks[i]
				if p, ok := taskProgress[t.Spec.Name]; ok && t.State == api.TaskRunning && p > t.Progress {
					t.Progress = p
				}
			}
		}); err != nil {
			s.log.Warn("persist task progress failed", "job", jobID, "err", err.Error())
		}
	}
}

// drainEventsLocked 非阻塞地处理完当前通道内的全部任务事件。
// 幂等：只对处于预期状态的任务生效，重复/迟到事件被忽略，
// 已完成任务不会被重复执行或重复计数。
func (s *Scheduler) drainEventsLocked() {
	for {
		select {
		case ev := <-s.events:
			s.handleEventLocked(ev)
		default:
			return
		}
	}
}

func (s *Scheduler) handleEventLocked(ev execution.Event) {
	j, ok := s.store.Get(ev.JobID)
	if !ok {
		s.log.Warn("event for unknown job, dropped", "job", ev.JobID, "task", ev.TaskName)
		return
	}
	idx := -1
	for i := range j.Tasks {
		if j.Tasks[i].Spec.Name == ev.TaskName {
			idx = i
			break
		}
	}
	if idx < 0 {
		return
	}
	task := &j.Tasks[idx]

	switch ev.Kind {
	case execution.EvKilled:
		// 故障语义：只有“当时确实跑在该节点上”的运行中任务才复位；
		// 已完成（Succeeded/Failed）任务保留结果，绝不重跑。
		// 故障丢失执行现场，任务从头重跑（Progress 清零）、Attempt+1。
		if task.State != api.TaskRunning || task.NodeID != ev.NodeID {
			s.log.Info("stale kill event ignored",
				"job", ev.JobID, "task", ev.TaskName,
				"state", task.State, "node", task.NodeID, "srcNode", ev.NodeID)
			return
		}
		task.State = api.TaskPending
		task.NodeID = ""
		task.Attempt++
		task.Progress = 0
		recomputeJobAfterReset(j)
		s.store.Update(ev.JobID, func(live *api.JobRuntime) {
			*live = *j
		})
		s.log.Warn("task killed by node failure, restarted from scratch and queued for reschedule",
			"job", ev.JobID, "task", ev.TaskName,
			"deadNode", ev.NodeID, "attempt", task.Attempt)

	case execution.EvDrained:
		// 计划内维护语义（与故障严格区分）：任务是被安全驱逐、不是丢失，
		// 复位为 Pending 但保留已执行进度、Attempt 不增长，
		// 随本 tick 的 gang 调度整体迁到健康节点接着原来的进度跑。
		if task.State != api.TaskRunning || task.NodeID != ev.NodeID {
			s.log.Info("stale drain event ignored",
				"job", ev.JobID, "task", ev.TaskName,
				"state", task.State, "node", task.NodeID, "srcNode", ev.NodeID)
			return
		}
		task.State = api.TaskPending
		task.NodeID = ""
		if ev.Progress > task.Progress {
			task.Progress = ev.Progress
		}
		// Attempt 刻意不变：主动维护迁移不是一次故障重试。
		recomputeJobAfterReset(j)
		s.store.Update(ev.JobID, func(live *api.JobRuntime) {
			*live = *j
		})
		s.log.Info("task evicted for planned maintenance, migrating with progress preserved",
			"job", ev.JobID, "task", ev.TaskName,
			"drainingNode", ev.NodeID, "progress", task.Progress,
			"attempt", task.Attempt)

	case execution.EvFinished:
		// 只接受 Running 任务的完成事件；重复事件被忽略，防止重复计数。
		if task.State != api.TaskRunning || task.NodeID != ev.NodeID {
			s.log.Info("stale finish event ignored",
				"job", ev.JobID, "task", ev.TaskName, "state", task.State)
			return
		}
		if ev.OK {
			task.State = api.TaskSucceeded
			if ev.Progress > 0 {
				task.Progress = ev.Progress
			}
			s.log.Info("task succeeded",
				"job", ev.JobID, "task", ev.TaskName, "node", ev.NodeID,
				"progress", task.Progress)
		} else {
			task.State = api.TaskFailed
			s.log.Warn("task failed by fault injection",
				"job", ev.JobID, "task", ev.TaskName, "node", ev.NodeID)
		}
		terminal := recomputeJobAfterProgress(j)
		s.store.Update(ev.JobID, func(live *api.JobRuntime) {
			*live = *j
		})
		if terminal {
			s.log.Info("job reached terminal state", "job", j.Spec.ID, "state", j.State, "reason", j.Reason)
			s.notifyWaiters(ev.JobID)
		}
	}
}

// recomputeJobAfterProgress 在任务完成事件后重算作业聚合状态。
// 返回作业是否进入终态。
func recomputeJobAfterProgress(j *api.JobRuntime) bool {
	allDone := true
	for _, t := range j.Tasks {
		if t.State == api.TaskFailed {
			j.State = api.JobFailed
			j.Reason = "task failed: " + t.Spec.Name
			return true
		}
		if t.State != api.TaskSucceeded {
			allDone = false
		}
	}
	if allDone {
		j.State = api.JobSucceeded
		j.Reason = ""
		return true
	}
	// 仍有未完成任务：若作业此前是 Pending（例如部分任务先成功的边界情况），
	// 归一为 Running。
	if j.State == api.JobPending {
		j.State = api.JobRunning
	}
	return false
}

// recomputeJobAfterReset 在任务被故障复位后重算作业状态：
// 若仍有任务在运行（部分故障），作业保持 Running；
// 若全部任务都待放置（整作业落于故障节点），回到 Pending 整体排队。
func recomputeJobAfterReset(j *api.JobRuntime) {
	hasRunning := false
	for _, t := range j.Tasks {
		if t.State == api.TaskRunning {
			hasRunning = true
			break
		}
	}
	if !hasRunning {
		j.State = api.JobPending
	}
}

// schedulePendingLocked 按优先级（高->低）、同优先级按 ID 排序遍历 Pending 作业，
// 对每个作业做全有或全无的放置；放不下则保留 Pending，继续尝试后续作业
// （队头不阻塞），保证不同调度顺序下结果确定且可解释。
func (s *Scheduler) schedulePendingLocked(now int64) {
	jobs := s.store.List()
	var candidates []*api.JobRuntime
	for _, jb := range jobs {
		// 候选条件：存在 Pending 任务。无论作业标签是 Pending（尚未启动过）
		// 还是 Running（故障后部分任务在别处运行、部分任务待重放置），
		// 都必须继续参与 gang 放置，避免剩余任务被饿死。
		for _, t := range jb.Tasks {
			if t.State == api.TaskPending {
				candidates = append(candidates, jb)
				break
			}
		}
	}
	sort.Slice(candidates, func(i, k int) bool {
		if candidates[i].Spec.Priority != candidates[k].Spec.Priority {
			return candidates[i].Spec.Priority > candidates[k].Spec.Priority
		}
		return candidates[i].Spec.ID < candidates[k].Spec.ID
	})

	for _, j := range candidates {
		s.tryPlaceJobLocked(j, now)
	}
}

func (s *Scheduler) tryPlaceJobLocked(j *api.JobRuntime, now int64) {
	// 重新从 store 取最新视图，避免候选快照过期。
	live, ok := s.store.Get(j.Spec.ID)
	if !ok || (live.State != api.JobPending && live.State != api.JobRunning) {
		return
	}
	hasPending := false
	for _, t := range live.Tasks {
		if t.State == api.TaskPending {
			hasPending = true
			break
		}
	}
	if !hasPending {
		return
	}
	j = live

	reqs := map[string]api.Resource{}
	var order []string
	for _, t := range j.Tasks {
		if t.State == api.TaskPending {
			reqs[t.Spec.Name] = t.Spec.Resources
			order = append(order, t.Spec.Name)
		}
	}
	if len(order) == 0 {
		return
	}

	placements := s.pool.Plan(reqs, order)
	if placements == nil {
		s.log.Info("gang placement deferred: not all tasks fit together this tick",
			"job", j.Spec.ID, "pendingTasks", len(order), "clock", now)
		return
	}

	// Plan 与 Commit 之间本调度器无其他提交者（均持 s.mu），
	// Commit 的原子性再兜底防止任何超发。
	if !s.pool.TryCommit(placements) {
		s.log.Warn("placement commit rejected by pool, aborting gang launch",
			"job", j.Spec.ID)
		return
	}

	byName := map[string]string{}
	for _, pl := range placements {
		byName[pl.TaskName] = pl.NodeID
	}

	s.store.Update(j.Spec.ID, func(cur *api.JobRuntime) {
		for i := range cur.Tasks {
			if cur.Tasks[i].State == api.TaskPending {
				nodeID := byName[cur.Tasks[i].Spec.Name]
				cur.Tasks[i].State = api.TaskRunning
				cur.Tasks[i].NodeID = nodeID
				// 在 executor 登记执行；同一任务重复登记会失败（防御重复执行）。
				if !s.exec.Launch(cur.Spec.ID, cur.Tasks[i]) {
					s.log.Warn("executor refused duplicate launch",
						"job", cur.Spec.ID, "task", cur.Tasks[i].Spec.Name)
				}
			}
		}
		cur.State = api.JobRunning
	})
	s.log.Info("gang placement committed and all tasks launched",
		"job", j.Spec.ID, "placements", logPlacements(placements), "clock", now)
}

// Recover 在进程重启后从快照恢复作业状态：
//   - 已结束（Succeeded/Failed）作业原样保留，重复提交时按 ErrDuplicateJob 拒绝；
//   - 未完成作业中，崩溃前处于 Running 的任务因执行行已不存在，统一复位为
//     Pending，随下一次 Tick 重新 gang 放置；节点池为空，无残留占用。
//
// 与并发调度的互斥由 s.mu 保证：Recover 返回前任何 Tick/Submit 的状态读取
// 都被阻塞，因此不会出现资源重复占用或作业丢失。
func (s *Scheduler) Recover() (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	counter, recovered, err := s.store.Recover()
	if err != nil {
		return 0, err
	}
	s.exec.Reset()
	// 恢复持久化的排空标记：维护中的节点重启后仍是排空中，
	// 不会被当成健康机器重新投入调度（AddNode 在 Recover 之后
	// 注册节点时也会按标记补应用，两种顺序都安全）。
	for _, id := range s.store.Draining() {
		if err := s.pool.BeginDrain(id); err == nil {
			s.log.Info("re-applied draining state after restart", "node", id)
		}
	}
	// 恢复退役名单：已正式下线的节点即使重启后被重新注册，也必须
	// 保持 Dead、容量不计入集群（AddNode 会按名单拒绝隐式复活；
	// 若节点在 Recover 之前已注册，这里立即把它压回退役态）。
	for _, id := range s.store.Retired() {
		if err := s.pool.MarkRetired(id); err == nil {
			delete(s.alive, id)
			s.log.Info("re-applied retired state after restart, capacity stays shrunken",
				"node", id)
		}
	}
	for _, j := range recovered {
		reset := false
		if j.State == api.JobSucceeded || j.State == api.JobFailed {
			s.log.Info("recovered terminal job, will not be resurrected",
				"job", j.Spec.ID, "state", j.State)
			continue
		}
		for i := range j.Tasks {
			t := &j.Tasks[i]
			if t.State == api.TaskRunning {
				// 进程崩溃属于非计划中断（执行行随进程消失）：
				// Attempt+1 记录这次重启重试，但崩溃前已执行的进度
				// （随每次 Tick 落盘）予以保留，任务重新放置后接着进度跑。
				t.State = api.TaskPending
				t.NodeID = ""
				t.Attempt++
				reset = true
			}
		}
		if reset || j.State == api.JobRunning {
			j.State = api.JobPending
		}
		s.store.Update(j.Spec.ID, func(live *api.JobRuntime) {
			*live = *j
		})
		s.log.Info("recovered unfinished job for continued scheduling",
			"job", j.Spec.ID, "state", j.State, "tasks", len(j.Tasks))
	}
	s.log.Info("recovery complete", "jobs", len(recovered), "snapshotVersion", counter)
	return counter, nil
}

// InjectTaskFailure 对一个正在运行的任务注入执行失败（测试用）。
// 任务不在运行中（未启动/已结束）时返回 false。
func (s *Scheduler) InjectTaskFailure(jobID, taskName string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	ok := s.exec.InjectFailure(jobID, taskName)
	if ok {
		s.drainEventsLocked()
	}
	return ok
}

// Run 按固定节拍反复调用 Tick，直到 stop 关闭。
func (s *Scheduler) Run(stop <-chan struct{}) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			s.Tick()
		}
	}
}

func logPlacements(ps []cluster.Placement) []string {
	out := make([]string, len(ps))
	for i, p := range ps {
		out[i] = fmt.Sprintf("%s->%s(cpu=%d,mem=%d)", p.TaskName, p.NodeID, p.Request.CPU, p.Request.Memory)
	}
	return out
}
