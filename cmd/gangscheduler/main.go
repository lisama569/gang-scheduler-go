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

	// ---------- 第二次“进程”启动：从快照恢复 ----------
	fmt.Println("==== phase 2: simulated process restart, recover from snapshot ====")
	s2 := scheduler.New(scheduler.Config{
		HeartbeatTimeoutTicks: 2,
		StateFile:             stateFile,
		Logger:                logger,
	})
	must(s2.AddNode("node-a", api.Resource{CPU: 4, Memory: 8}))
	must(s2.AddNode("node-b", api.Resource{CPU: 2, Memory: 4}))
	ver, err := s2.Recover()
	must(err)
	fmt.Printf("recovered snapshot version=%d\n", ver)

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

	// ---------- 第三次“进程”启动：演示计划内维护/排空 ----------
	fmt.Println("==== phase 3: planned maintenance / node drain ====")
	s3 := scheduler.New(scheduler.Config{
		HeartbeatTimeoutTicks: 2,
		StateFile:             filepath.Join(stateDir, "snapshot3.json"),
		Logger:                logger,
	})
	must(s3.AddNode("node-a", api.Resource{CPU: 4, Memory: 8}))
	must(s3.AddNode("node-b", api.Resource{CPU: 2, Memory: 4}))
	must(s3.AddNode("node-c", api.Resource{CPU: 4, Memory: 8}))
	if _, err := s3.Recover(); err != nil {
		must(err)
	}

	must(s3.Submit(api.JobSpec{ID: "job-maint", Priority: 5,
		Tasks: []api.TaskSpec{
			{Name: "a", Resources: api.Resource{CPU: 3, Memory: 4}, Duration: 5},
			{Name: "b", Resources: api.Resource{CPU: 2, Memory: 3}, Duration: 5},
		}}))
	s3.Tick() // a -> node-a, b -> node-b
	printJob(s3, "job-maint")

	// 在任务运行中主动把 node-b 置为维护/排空。
	must(s3.DrainNode("node-b"))
	printDrain(s3)
	s3.Tick() // b 在线迁移到 node-c，node-b 迁空后退役
	fmt.Println("-- after one drain tick --")
	printJob(s3, "job-maint")
	printDrain(s3)
	printNodes(s3)

	// 容量已收缩为 node-a(4)+node-c(4)=8cpu：9cpu 新作业按收缩后容量被拒。
	err = s3.Submit(api.JobSpec{ID: "job-too-big-after-shrink", Priority: 1,
		Tasks: []api.TaskSpec{
			{Name: "x", Resources: api.Resource{CPU: 3, Memory: 1}, Duration: 1},
			{Name: "y", Resources: api.Resource{CPU: 3, Memory: 1}, Duration: 1},
			{Name: "z", Resources: api.Resource{CPU: 3, Memory: 1}, Duration: 1},
		}})
	fmt.Printf("submit after shrink rejected as: %v\n", err)

	for i := 0; i < 6; i++ {
		s3.Tick()
	}
	printJob(s3, "job-maint")
	printNodes(s3)
	fmt.Println("demo finished")
}

func printDrain(s *scheduler.Scheduler) {
	for _, p := range s.DrainProgress() {
		fmt.Printf("drain node %s state=%s remainingTasks=%d\n", p.NodeID, p.State, p.Remaining)
	}
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
		fmt.Printf("  task %s state=%s node=%s attempt=%d\n", t.Spec.Name, t.State, t.NodeID, t.Attempt)
	}
}

func printNodes(s *scheduler.Scheduler) {
	// GetJob 之外通过新增的 Nodes 视图打印（见 scheduler.Nodes）。
	for _, n := range s.Nodes() {
		fmt.Printf("node %s state=%s used={cpu:%d mem:%d} cap={cpu:%d mem:%d}\n",
			n.ID, n.State, n.Used.CPU, n.Used.Memory, n.Capacity.CPU, n.Capacity.Memory)
	}
}
