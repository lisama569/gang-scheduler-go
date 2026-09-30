// Command gangscheduler 在单进程内演示 gang 作业调度：
// 模拟节点 + 逻辑时钟心跳驱动多作业并发执行，并演示节点故障回收与重启恢复。
package main

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/lisama569/gang-scheduler-go/pkg/api"
	"github.com/lisama569/gang-scheduler-go/pkg/scheduler"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	stateDir := filepath.Join(os.TempDir(), "gang-scheduler-demo")
	_ = os.RemoveAll(stateDir)
	stateFile := filepath.Join(stateDir, "snapshot.json")

	// ---------- 第一次“进程”启动 ----------
	fmt.Println("==== phase 1: first process start ====")
	s1 := scheduler.New(scheduler.Config{
		HeartbeatTimeoutTicks: 2,
		StateFile:             stateFile,
		Logger:                logger,
	})
	must(s1.AddNode("node-a", api.Resource{CPU: 4, Memory: 8}))
	must(s1.AddNode("node-b", api.Resource{CPU: 2, Memory: 4}))

	// 高优先级作业：两个任务必须整体启动。
	must(s1.Submit(api.JobSpec{
		ID: "job-app", Priority: 10,
		Tasks: []api.TaskSpec{
			{Name: "t1", Resources: api.Resource{CPU: 3, Memory: 4}, Duration: 4},
			{Name: "t2", Resources: api.Resource{CPU: 2, Memory: 3}, Duration: 4},
		},
	}))

	// 超大作业会被明确拒绝（资源不足，永远放不下）。
	err := s1.Submit(api.JobSpec{
		ID: "job-big", Priority: 1,
		Tasks: []api.TaskSpec{
			{Name: "huge", Resources: api.Resource{CPU: 99, Memory: 99}, Duration: 1},
		},
	})
	fmt.Printf("submit job-big rejected as: %v\n", err)

	// 重复提交会被明确拒绝。
	must(s1.Submit(api.JobSpec{ID: "job-appx", Priority: 1,
		Tasks: []api.TaskSpec{{Name: "x", Resources: api.Resource{CPU: 1, Memory: 1}, Duration: 1}}}))
	err = s1.Submit(api.JobSpec{ID: "job-appx", Priority: 1,
		Tasks: []api.TaskSpec{{Name: "x", Resources: api.Resource{CPU: 1, Memory: 1}, Duration: 1}}})
	fmt.Printf("duplicate submit rejected as: %v\n", err)

	// 跑一拍：job-app 整体启动（tick 末尾执行行推进一拍）。
	s1.Tick()
	// 在任务仍在运行时让 node-b 故障：t2 心跳超时后须被复位并重新调度到 node-a。
	s1.TriggerNodeFailure("node-b")
	for i := 0; i < 8; i++ {
		s1.Tick() // 超时检测 -> t2 复位 -> 在 node-a 重新 gang 放置 -> 完成
	}
	printJob(s1, "job-app")
	printNodes(s1)

	// ---------- 计划内维护：排空并下线 node-c ----------
	fmt.Println("==== phase 1.5: planned maintenance: drain and retire node-c ====")
	// 扩容一台 node-c，再把 w2 调度上去，随后对它做计划内排空。
	must(s1.AddNode("node-c", api.Resource{CPU: 4, Memory: 8}))
	must(s1.Submit(api.JobSpec{ID: "job-web", Priority: 5,
		Tasks: []api.TaskSpec{
			{Name: "w1", Resources: api.Resource{CPU: 3, Memory: 4}, Duration: 4},
			{Name: "w2", Resources: api.Resource{CPU: 2, Memory: 2}, Duration: 3},
		}}))
	s1.Tick() // w1->node-a, w2->node-c

	// 主动把健康的 node-c 置为维护/排空中：不再收新任务，存量任务安全迁出。
	must(s1.DrainNode("node-c"))
	printDrain(s1, "node-c") // 还剩 1 个任务没迁完
	s1.Tick()                // w2 被驱逐；node-a 余量不足，w2 排队等待
	printDrain(s1, "node-c") // 节点已空
	// 任务全部迁走后正式退役，容量从集群中扣除。
	must(s1.CompleteNodeDrain("node-c"))
	printNodes(s1)

	// 收缩后的集群（只剩 node-a={4,8}）放不下总量 {5,4} 的作业，会被明确拒绝；
	// 而排空前 node-a+node-c={8,16} 是可以放下的。
	err = s1.Submit(api.JobSpec{ID: "job-after-shrink", Priority: 1,
		Tasks: []api.TaskSpec{
			{Name: "x", Resources: api.Resource{CPU: 3, Memory: 2}, Duration: 1},
			{Name: "y", Resources: api.Resource{CPU: 2, Memory: 2}, Duration: 1},
		}})
	fmt.Printf("submit after capacity shrink rejected as: %v\n", err)

	for i := 0; i < 10; i++ {
		s1.Tick() // w1 完成后 w2 在 node-a 上继续，job-web 最终成功
	}
	printJob(s1, "job-web")

	// ---------- 第二次“进程”启动：从快照恢复 ----------
	fmt.Println("==== phase 2: simulated process restart, recover from snapshot ====")
	s2 := scheduler.New(scheduler.Config{
		HeartbeatTimeoutTicks: 2,
		StateFile:             stateFile,
		Logger:                logger,
	})
	must(s2.AddNode("node-a", api.Resource{CPU: 4, Memory: 8}))
	// node-c 在第一次进程里已完成排空、正式退役。重启后直接重新注册
	// 同名节点会被拒绝：已下线节点不会被隐式复活，容量不会涨回去。
	err = s2.AddNode("node-c", api.Resource{CPU: 4, Memory: 8})
	fmt.Printf("re-register retired node-c rejected as: %v\n", err)
	ver, err := s2.Recover()
	must(err)
	fmt.Printf("recovered snapshot version=%d\n", ver)
	fmt.Println("nodes after recovery (node-c absent -> capacity stays shrunken):")
	printNodes(s2)

	// 已结束作业不会复活，重复提交被拒绝。
	err = s2.Submit(api.JobSpec{ID: "job-app", Priority: 10,
		Tasks: []api.TaskSpec{{Name: "t1", Resources: api.Resource{CPU: 1, Memory: 1}, Duration: 1}}})
	fmt.Printf("resubmit finished job rejected as: %v\n", err)

	// 新作业可以继续调度。
	must(s2.Submit(api.JobSpec{ID: "job-after-restart", Priority: 5,
		Tasks: []api.TaskSpec{
			{Name: "a", Resources: api.Resource{CPU: 2, Memory: 2}, Duration: 2},
			{Name: "b", Resources: api.Resource{CPU: 2, Memory: 2}, Duration: 2},
		}}))
	for i := 0; i < 4; i++ {
		s2.Tick()
	}
	printJob(s2, "job-after-restart")
	printNodes(s2)

	// 运维显式让 node-c 重新上线（区别于隐式重新注册）：容量重新计入集群。
	must(s2.ReadmitNode("node-c", api.Resource{CPU: 4, Memory: 8}))
	fmt.Println("node-c explicitly readmitted:")
	printNodes(s2)
	must(s2.Submit(api.JobSpec{ID: "job-after-readmit", Priority: 1,
		Tasks: []api.TaskSpec{
			{Name: "p", Resources: api.Resource{CPU: 3, Memory: 2}, Duration: 2},
		}}))
	s2.Tick() // 放到重新上线的 node-c
	s2.Tick()
	printJob(s2, "job-after-readmit")
	fmt.Println("demo finished")
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func printJob(s *scheduler.Scheduler, id string) {
	j, err := s.GetJob(id)
	if err != nil {
		fmt.Println(id, "err:", err)
		return
	}
	fmt.Printf("job %s state=%s reason=%q\n", id, j.State, j.Reason)
	for _, t := range j.Tasks {
		fmt.Printf("  task %s state=%s node=%s attempt=%d progress=%d/%d\n",
			t.Spec.Name, t.State, t.NodeID, t.Attempt, t.Progress, t.Spec.Duration)
	}
}

func printNodes(s *scheduler.Scheduler) {
	// GetJob 之外通过新增的 Nodes 视图打印（见 scheduler.Nodes）。
	for _, n := range s.Nodes() {
		fmt.Printf("node %s state=%s used={cpu:%d mem:%d} cap={cpu:%d mem:%d}\n",
			n.ID, n.State, n.Used.CPU, n.Used.Memory, n.Capacity.CPU, n.Capacity.Memory)
	}
}

func printDrain(s *scheduler.Scheduler, id string) {
	info, err := s.DrainStatus(id)
	if err != nil {
		fmt.Println(id, "drain status err:", err)
		return
	}
	fmt.Printf("drain %s state=%s remainingTasks=%d remaining={cpu:%d mem:%d}\n",
		info.NodeID, info.State, info.RemainingTasks,
		info.RemainingResources.CPU, info.RemainingResources.Memory)
}
