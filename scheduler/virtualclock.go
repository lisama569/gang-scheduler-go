package scheduler

import (
	"sync/atomic"
	"time"
)

// VirtualClock 是测试用的可控时钟：时间只能通过 Advance 推进，
// 调度决策因此与 wall clock 无关、可重复。
type VirtualClock struct {
	nanos atomic.Int64
}

// NewVirtualClock 从固定基准时刻 t0 创建虚拟时钟。
func NewVirtualClock() *VirtualClock {
	vc := &VirtualClock{}
	vc.nanos.Store(1_000_000_000) // 固定 t0=1s，避免零值歧义
	return vc
}

func (c *VirtualClock) Now() time.Time {
	return time.Unix(0, c.nanos.Load())
}

// Advance 把虚拟时间向前推 d。
func (c *VirtualClock) Advance(d time.Duration) {
	c.nanos.Add(int64(d))
}
