package scheduler

import (
	"context"
	"sync"
	"time"
)

// Runner 用后台 goroutine 周期性驱动 Tick，并让存活的模拟节点定时心跳。
// 对某个节点调用 SkipHeartbeat(true) 即可模拟该节点失联：心跳超时后
// 调度器会在下一个 Tick 自动判定故障并回收。
type Runner struct {
	sched          *Scheduler
	tickInterval   time.Duration
	heartbeatEvery time.Duration

	mu            sync.Mutex
	skipHeartbeat map[string]bool
}

// NewRunner 创建驱动器。
func NewRunner(s *Scheduler, tickInterval, heartbeatEvery time.Duration) *Runner {
	return &Runner{
		sched:          s,
		tickInterval:   tickInterval,
		heartbeatEvery: heartbeatEvery,
		skipHeartbeat:  map[string]bool{},
	}
}

// SkipHeartbeat 设置是否停止给某节点发送心跳（模拟失联 / 恢复）。
func (r *Runner) SkipHeartbeat(nodeID string, skip bool) {
	r.mu.Lock()
	r.skipHeartbeat[nodeID] = skip
	r.mu.Unlock()
}

func (r *Runner) skipped(nodeID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.skipHeartbeat[nodeID]
}

// Run 阻塞运行直到 ctx 取消。启动时会立即做一次心跳与一次 Tick。
func (r *Runner) Run(ctx context.Context) error {
	r.sendHeartbeats()
	r.sched.Tick(ctx)
	tickTicker := time.NewTicker(r.tickInterval)
	defer tickTicker.Stop()
	hbTicker := time.NewTicker(r.heartbeatEvery)
	defer hbTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-hbTicker.C:
			r.sendHeartbeats()
		case <-tickTicker.C:
			r.sched.Tick(ctx)
		}
	}
}

func (r *Runner) sendHeartbeats() {
	now := r.sched.clock.Now()
	for _, id := range r.sched.aliveNodeIDs() {
		if r.skipped(id) {
			continue
		}
		if err := r.sched.Heartbeat(id, now); err != nil {
			r.sched.log.Printf("heartbeat for %s failed: %v", id, err)
		}
	}
}
