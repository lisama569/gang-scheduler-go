package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/lisama569/gang-scheduler-go/scheduler"
)

func main() {
	ctx := context.Background()
	stateDir := envOr("GANGSIM_STATE", "./state")
	stateFile := filepath.Join(stateDir, "gang-sim.json")

	mode := "demo"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}

	switch mode {
	case "demo":
		// 每次演示从干净状态开始。
		_ = os.RemoveAll(stateDir)
		if err := runDemo(ctx, stateFile); err != nil {
			fmt.Fprintf(os.Stderr, "demo failed: %v\n", err)
			os.Exit(1)
		}
	case "resume":
		// 用一个新的“进程”（新 Scheduler 实例）读取同一份快照：
		// 已结束作业仍为终态，未完成作业继续调度直至完成。
		if err := runResume(ctx, stateFile); err != nil {
			fmt.Fprintf(os.Stderr, "resume failed: %v\n", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown mode %q (use 'demo' or 'resume')\n", mode)
		os.Exit(2)
	}
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

const (
	tickInterval = 100 * time.Millisecond
	heartbeat    = 100 * time.Millisecond
	hbTimeout    = 400 * time.Millisecond
)

func registerNodes(ctx context.Context, s *scheduler.Scheduler) {
	must(s.AddNode("n1", scheduler.Resource{CPU: 4, Memory: 400}))
	must(s.AddNode("n2", scheduler.Resource{CPU: 4, Memory: 400}))
	must(s.AddNode("n3", scheduler.Resource{CPU: 8, Memory: 800}))
}

func runDemo(ctx context.Context, stateFile string) error {
	store := scheduler.NewFileStore(stateFile)
	s1 := scheduler.New(store, scheduler.WithHeartbeatTimeout(hbTimeout))
	must(s1.Recover(ctx))
	registerNodes(ctx, s1)

	fmt.Println("=== 1) 提交被拒绝的作业（失败原因必须可区分）===")
	showReject := func(name string, err error) {
		switch {
		case errors.Is(err, scheduler.ErrDuplicateJob):
			fmt.Printf("[%s] rejected: DUPLICATE -> %v\n", name, err)
		case errors.Is(err, scheduler.ErrTaskTooLarge):
			fmt.Printf("[%s] rejected: TASK_TOO_LARGE -> %v\n", name, err)
		case errors.Is(err, scheduler.ErrInsufficientCapacity):
			fmt.Printf("[%s] rejected: INSUFFICIENT_CAPACITY -> %v\n", name, err)
		case errors.Is(err, scheduler.ErrInvalidJob):
			fmt.Printf("[%s] rejected: INVALID -> %v\n", name, err)
		default:
			fmt.Printf("[%s] unexpected: %v\n", name, err)
		}
	}

	// 非法：没有任务
	showReject("empty", s1.Submit(ctx, scheduler.SubmitRequest{JobID: "bad-empty", Priority: 1}))
	// 非法：资源请求非正
	showReject("badres", s1.Submit(ctx, scheduler.SubmitRequest{
		JobID: "bad-res", Priority: 1,
		Tasks: []scheduler.TaskSpec{{ID: "t0", CPU: 0, Memory: 100}},
	}))
	// 单个任务超过任何节点
	showReject("toolarge", s1.Submit(ctx, scheduler.SubmitRequest{
		JobID: "too-large", Priority: 1,
		Tasks: []scheduler.TaskSpec{{ID: "t0", CPU: 100, Memory: 100}},
	}))
	// 总需求超过集群总量（集群 16 CPU / 1600 MB）
	bigTasks := make([]scheduler.TaskSpec, 0, 6)
	for i := 0; i < 6; i++ {
		bigTasks = append(bigTasks, scheduler.TaskSpec{ID: fmt.Sprintf("t%d", i), CPU: 3, Memory: 300})
	}
	showReject("overcap", s1.Submit(ctx, scheduler.SubmitRequest{
		JobID: "over-capacity", Priority: 1, Tasks: bigTasks,
	}))

	fmt.Println("=== 2) 正常提交：优先级 + gang 整体启动 ===")
	// quick：独立小作业，会很快结束；重启后必须保持 SUCCEEDED 且不复活。
	must(s1.Submit(ctx, scheduler.SubmitRequest{
		JobID: "quick", Priority: 1,
		Tasks: []scheduler.TaskSpec{{ID: "q0", CPU: 1, Memory: 100, Duration: 200 * time.Millisecond}},
	}))
	// etl：4 个任务，每个 2 CPU，必须一起启动。filler 占满 n1/n2 后，
	// 仅 n3（8 CPU）放不下 4 个任务，etl 只能排队，直到 n1 故障回收资源。
	etlTasks := make([]scheduler.TaskSpec, 0, 4)
	for i := 0; i < 4; i++ {
		etlTasks = append(etlTasks, scheduler.TaskSpec{
			ID: fmt.Sprintf("e%d", i), CPU: 2, Memory: 200, Duration: 1200 * time.Millisecond,
		})
	}
	must(s1.Submit(ctx, scheduler.SubmitRequest{JobID: "etl", Priority: 5, Tasks: etlTasks}))
	// filler：低优先级后台作业占满 n1/n2（8 CPU），使 etl 无法整体放置而排队。
	must(s1.Submit(ctx, scheduler.SubmitRequest{
		JobID: "filler", Priority: 1,
		Tasks: []scheduler.TaskSpec{
			{ID: "f0", CPU: 4, Memory: 400, Duration: 3 * time.Second},
			{ID: "f1", CPU: 4, Memory: 400, Duration: 3 * time.Second},
		},
	}))
	// 重复提交（etl 已存在）
	showReject("dup", s1.Submit(ctx, scheduler.SubmitRequest{
		JobID: "etl", Priority: 1,
		Tasks: []scheduler.TaskSpec{{ID: "x", CPU: 1, Memory: 100}},
	}))

	r1 := scheduler.NewRunner(s1, tickInterval, heartbeat)
	runCtx1, cancel1 := context.WithCancel(ctx)
	done1 := make(chan struct{})
	go func() {
		_ = r1.Run(runCtx1)
		close(done1)
	}()

	// 等 quick 完成（崩溃前就结束，重启后必须保持 SUCCEEDED 且不复活）。
	waitCtx, waitCancel := context.WithTimeout(ctx, 3*time.Second)
	if _, err := s1.WaitFor(waitCtx, "quick"); err != nil {
		waitCancel()
		cancel1()
		<-done1
		return err
	}
	waitCancel()
	fmt.Println("job quick finished before failure/recovery")

	// 高优先级 web 随后提交：单任务 7 CPU 只能放在空闲的 n3 上并立即执行。
	// etl 仍排队（n1/n2 被 filler 占满，n3 被 web 占 7 CPU）。
	// 随后让 n1 失联：filler/f0 被回收，etl 才凑齐 4 个任务的整体放置资源。
	must(s1.Submit(ctx, scheduler.SubmitRequest{
		JobID: "web", Priority: 10,
		Tasks: []scheduler.TaskSpec{
			{ID: "w0", CPU: 7, Memory: 700, Duration: 1500 * time.Millisecond},
		},
	}))
	time.Sleep(400 * time.Millisecond) // 等 web 在 n3 上启动

	fmt.Println("=== 3) 模拟节点 n1 失联（停止心跳 -> 超时回收 -> etl 整体重调度）===")
	r1.SkipHeartbeat("n1", true)
	time.Sleep(900 * time.Millisecond) // 超过心跳超时：filler/f0 回收，etl 获得资源整体启动
	r1.SkipHeartbeat("n1", false)
	must(s1.AddNode("n1", scheduler.Resource{CPU: 4, Memory: 400})) // 故障节点重新上线
	time.Sleep(300 * time.Millisecond)

	fmt.Println("=== 4) 模拟进程崩溃：停止旧实例，用同一文件存储新建实例恢复 ===")
	// 崩溃前快照里：quick 已结束；web/filler/etl 中仍有未完成任务。
	cancel1()
	<-done1

	s2 := scheduler.New(scheduler.NewFileStore(stateFile), scheduler.WithHeartbeatTimeout(hbTimeout))
	must(s2.Recover(ctx))
	// 新进程重新注册节点；快照里 n1 可能仍存活/已死亡，AddNode 对存活节点会报错，
	// 这里只注册当前不存活的节点。
	if st, ok := s2.JobState("quick"); ok {
		fmt.Printf("after restart: quick state=%s (must stay SUCCEEDED, never revived)\n", st)
	}
	reregisterIfNeeded(ctx, s2)

	r2 := scheduler.NewRunner(s2, tickInterval, heartbeat)
	runCtx2, cancel2 := context.WithCancel(ctx)
	defer cancel2()
	go func() { _ = r2.Run(runCtx2) }()

	fmt.Println("=== 5) 恢复后继续调度，等待全部作业结束 ===")
	for _, j := range []string{"web", "filler", "etl"} {
		wctx, wcancel := context.WithTimeout(ctx, 8*time.Second)
		st, err := s2.WaitFor(wctx, j)
		wcancel()
		if err != nil {
			return fmt.Errorf("wait %s: %w", j, err)
		}
		fmt.Printf("job %s final state=%s\n", j, st)
	}
	for _, t := range []string{"e0", "e1", "e2", "e3", "w0", "f0", "f1"} {
		job := "etl"
		switch t[0] {
		case 'w':
			job = "web"
		case 'f':
			job = "filler"
		}
		st, node, _ := s2.TaskState(job, t)
		fmt.Printf("task %s/%s state=%s node=%s executions=%d\n",
			job, t, st, node, s2.Executions(job, t))
	}
	fmt.Println("demo finished; snapshot at", stateFile)
	fmt.Println("tip: run `gangsim resume` to start another process from the same snapshot")
	return nil
}

func runResume(ctx context.Context, stateFile string) error {
	s := scheduler.New(scheduler.NewFileStore(stateFile), scheduler.WithHeartbeatTimeout(hbTimeout))
	if err := s.Recover(ctx); err != nil {
		return err
	}
	reregisterIfNeeded(ctx, s)
	r := scheduler.NewRunner(s, tickInterval, heartbeat)
	rc, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	go func() { _ = r.Run(rc) }()

	// 新进程视角：快照中的终态作业保持终态。
	for _, j := range []string{"quick", "web", "etl"} {
		if st, ok := s.JobState(j); ok {
			fmt.Printf("resumed process sees job %s state=%s\n", j, st)
		}
	}
	<-rc.Done()
	fmt.Println("resume process ran for 1.5s against the same snapshot and exited")
	return nil
}

// reregisterIfNeeded 把快照中不存活的节点重新注册（模拟新进程连回集群）。
func reregisterIfNeeded(ctx context.Context, s *scheduler.Scheduler) {
	caps := map[string]scheduler.Resource{
		"n1": {CPU: 4, Memory: 400},
		"n2": {CPU: 4, Memory: 400},
		"n3": {CPU: 8, Memory: 800},
	}
	for id, c := range caps {
		if err := s.AddNode(id, c); err != nil {
			fmt.Printf("node %s not re-registered: %v\n", id, err)
		}
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
