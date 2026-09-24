# Gang Scheduler（单进程模拟集群作业调度器）

一个完全在单进程内运行的集群作业调度模拟器：用模拟节点 + 心跳驱动多作业
并发执行，验证 **gang 调度（整作业一起启动）**、**优先级 / FIFO 队列**、
**节点故障回收与重调度**、以及 **进程重启后的状态恢复**。所有调度决策
（放置、拒绝、回收、完成、恢复）都会打印资源变更依据，行为可解释、可复现。

## 快速开始

```bash
# 运行端到端演示：拒绝原因 -> gang 调度 -> 节点失联回收 -> 模拟崩溃 -> 新进程恢复
go run ./cmd/gangsim demo

# 用一个全新的进程读取演示留下的同一份快照（已结束作业保持终态）
go run ./cmd/gangsim resume

# 全部单元测试（-race 开启竞态检测；-v 可看到关键调度决策日志）
go test ./... -race -v
```

状态文件默认在 `./state/gang-sim.json`，可用环境变量 `GANGSIM_STATE` 覆盖。
`demo` 每次启动会清空该目录；`resume` 直接复用已有快照。

## 模型

- **资源**：每个节点有 `CPU`、`Memory` 两类容量；任务声明请求量。
- **任务 / 作业**：一个作业包含若干任务；任务状态为
  `PENDING -> RUNNING -> SUCCEEDED`；作业状态为
  `PENDING / RUNNING / SUCCEEDED`（`FAILED` 为保留枚举）。
- **执行模型**：任务带模拟耗时，`RUNNING` 任务在调度时钟上到达
  `StartedAt + Duration` 即由调度器判定完成并归还资源（没有真实进程，
  因此“执行”完全确定、不会因调度顺序不同产生不同结果）。
- **时钟**：生产用真实时钟；测试用 `VirtualClock`，时间只能显式推进，
  保证不同调度顺序下结果一致。

## 调度规则

1. **Gang（整体）调度，绝不部分提交**
   一个作业的所有 `PENDING` 任务必须在同一轮决策里全部放下，才会一起启动；
   只要有一个任务放不下，本轮该作业一个任务都不启动，继续排队。
   放置时使用“临时余量账本”试算，提交是全有或全无的。
2. **优先级 + FIFO**
   队列按 `Priority` 降序；优先级相同按提交序号 `SubmitSeq` 升序（先到先得）。
3. **工作守恒（work-conserving）**
   队首作业放不下时会跳过它，继续尝试后面放得下的作业，资源不闲置。
4. **确定性放置**
   节点按 ID 排序、任务按提交顺序排序、首次适配（first-fit），
   相同输入与调度顺序永远得到相同放置结果。
5. **提交时的可区分拒绝原因**

   | 错误哨兵 | 触发条件 |
   |---|---|
   | `ErrInvalidJob` | 作业 ID 为空、无任务、任务 ID 为空或重复、资源请求非正 |
   | `ErrDuplicateJob` | 作业 ID 已存在（含并发提交竞争，保证恰有一个成功） |
   | `ErrTaskTooLarge` | 某个任务的请求超过任一存活节点的容量，永远放不下 |
   | `ErrInsufficientCapacity` | 作业总请求超过存活集群总容量 |

   “**暂时**放不下”（总量够但当前被占用）**不是错误**：作业进入队列，
   等资源释放后自动获得调度。
6. **收敛性**
   单个 Tick 内会循环调度直到本轮没有任何新作业启动，因此一批任务同时
   完成释放资源后，排队作业在同一 Tick 内即可获得资源，不依赖外部轮询次数。

## 节点心跳与故障恢复语义

- 存活节点通过 `Heartbeat(nodeID, at)` 周期性上报；`now - LastBeatAt`
  超过心跳超时阈值（默认 3s，演示为 400ms）即在 Tick 中判定故障。
- 判定故障后（均在调度器互斥锁内一次性完成）：
  1. 该节点上**仍在运行**的任务全部回退为 `PENDING`（`Attempts+1`）、
     清空节点绑定与启动时间，资源账本归零；
  2. 所属作业若仍有未完成任务则重新入队，**剩余任务在下一轮仍作为整体放置**；
  3. 节点标记为 `Alive=false`，需重新 `AddNode` 注册后才会再承载任务。
- **已 `SUCCEEDED` 的任务不受故障影响**：完成只以任务自身状态与启动时间
  为准，任何后续节点消息都不会让它二次完成或重复计入统计
  （`Executions(jobID, taskID)` 持久化了每个任务的实际启动次数）。
- 作业终态严格由任务结果收敛：所有任务 `SUCCEEDED` ⇔ 作业 `SUCCEEDED`。

## 进程重启恢复语义

- 每次对外操作结束后，调度器把完整状态**原子落盘**为 JSON 快照
  （写临时文件 + `fsync` + `rename`，崩溃不会留下半份文件）。
- 新进程必须先调用 `Recover`：
  - 已结束的作业原样恢复为终态，**绝不复活、不再调度、不重复执行**；
  - 崩溃前 `RUNNING` 的任务视为执行中断，一律回退为 `PENDING`（`Attempts+1`），
    由后续 Tick 连同其余未完成任务整体重新放置；
  - 节点资源账本**从零按任务实际放置重建**，不会出现重复占用；
  - 重启后所有节点都视为 `Alive=false`，必须重新注册（重新上报心跳）后
    才能承载任务——调度器不假设旧节点仍然存活；
  - `SubmitSeq` 与每个任务的执行计数 `Executions` 一并恢复，
    新提交不会与旧序号冲突，成功任务的执行次数可跨重启断言。
- `Recover` 与并发调度的关系：`Tick` / `Submit` / `AddNode` 会阻塞到
  `Recover` 返回（`ready` 通道），因此恢复与调度并发发生时也不会读到
  半初始化状态、不会丢作业或重复占用资源。

## 代码结构

| 文件 | 职责 |
|---|---|
| `scheduler/types.go` | 资源、任务、作业、节点、快照等类型与资源账本原语 |
| `scheduler/errors.go` | 可区分的提交 / 调度失败哨兵错误 |
| `scheduler/queue.go` | 优先级 + FIFO 的确定性队列排序 |
| `scheduler/nodepool.go` | 节点池、存活过滤、容量汇总、占用 / 归还 |
| `scheduler/store.go` | `Store` 接口；原子 JSON 文件存储与测试内存存储 |
| `scheduler/scheduler.go` | 核心状态机：提交校验、gang 放置、故障检测、完成判定、持久化、恢复 |
| `scheduler/runner.go` | 后台 Tick 与心跳驱动器（支持停发心跳模拟失联） |
| `scheduler/virtualclock.go` | 测试用确定性虚拟时钟 |
| `scheduler/log.go` | 日志接口（测试实现同时输出到 `t.Log`） |
| `cmd/gangsim/main.go` | 本地可运行的端到端演示（`demo` / `resume`） |

## 验证的不变量（测试中显式断言）

1. 每个节点 `Used` 等于其上 `RUNNING` 任务请求之和，且不超过 `Capacity`；
2. 每个 `RUNNING` 任务恰好登记在其绑定节点的账本中（无重复占用、无悬空任务）；
3. 作业 `SUCCEEDED` 当且仅当其全部任务 `SUCCEEDED`；
4. 已成功任务跨故障 / 跨重启执行次数不增加；
5. 重复并发提交同一作业 ID：恰有一个成功，其余全部 `ErrDuplicateJob`；
6. 恢复后已结束作业不复活，未完成作业最终全部收敛到终态。

## 测试场景对照

| 测试 | 覆盖点 |
|---|---|
| `TestSubmitRejectionsAreDistinguishable` | 非法 / 重复 / 单任务过大 / 总量不足，失败原因可区分 |
| `TestGangSchedulingAllOrNothing` | 资源不足时全不启动、资源到位后整体启动 |
| `TestSchedulingPriorityAndFIFO` / `TestSchedulingFIFOAmongEqualPriority` | 优先级抢占与同优先级 FIFO |
| `TestNodeFailureReschedulesUnfinishedTasks` | 心跳超时回收、未完成任务换到活节点、死节点账本归零 |
| `TestPartialGangFailureKeepsSucceededResults` | 部分成功的 gang 故障后不重跑已完成任务、剩余整体重放置 |
| `TestRestartRecoveryContinuesUnfinishedAndDoesNotReviveFinished` | 重启恢复：终态不复活、RUNNING 回退、重注册后继续调度至完成 |
| `TestSnapshotSaveLoadRoundTrip` | 快照原子保存 / 加载与副本隔离 |
| `TestConcurrentDuplicateSubmit` | 32 路并发重复提交，恰一成功 |
| `TestConcurrentDistinctSubmitNoDoubleOrLost` | 100 路并发提交：64 运行 / 36 排队，账本守恒，最终全部成功 |
| `TestRecoveryConcurrentWithTick` | Recover 与 Tick / Submit / AddNode 并发时状态一致 |
