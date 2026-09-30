# Gang 集群作业调度器（单进程模拟）

在单个 Go 进程内用**模拟节点 + 逻辑时钟心跳**驱动多个作业并发执行，
演示 gang（整作业）调度、优先级队列、节点故障回收、计划内维护/排空（drain）、
以及进程重启后的状态恢复。
设计目标是：**在不同调度顺序下，资源分配、故障恢复与作业状态保持一致且可解释**。

## 1. 目录结构

| 路径 | 职责 |
| --- | --- |
| `pkg/api` | 核心领域类型（Resource/Node/Task/Job）、资源运算、可区分错误 |
| `pkg/cluster` | 模拟节点与节点池：容量账本、心跳、超时检测、gang 放置的“全有或全无”提交 |
| `pkg/execution` | 模拟执行器：按 tick 推进任务、故障终止、故障注入，通过事件通道上报 |
| `pkg/state` | 作业状态存储：内存表 + JSON 快照原子落盘（临时文件 rename）+ 合并恢复 |
| `pkg/scheduler` | 调度器：提交校验、队列调度、心跳回收、事件聚合、恢复 |
| `cmd/gangscheduler` | 可本地运行的端到端演示 |

## 2. 模型与概念

- **Resource**：`{CPU, Memory}` 标量资源；`Fits / Add / Sub` 运算见 `pkg/api/resource.go`。
- **逻辑时钟（tick）**：`Scheduler.Tick()` 推进一个时钟周期。
  既可用事件方式逐 tick 调用（测试，确定可断言），也可用 `Run(stop)` 按 10ms 自动推进。
- **任务时长 Duration**：任务启动后需推进的 tick 数；启动发生在 tick 内的调度阶段，
  tick 末尾执行器统一推进一拍。
- **节点**：有 `Capacity` 与 `Used` 账本、最近心跳时间戳，以及三种状态：
  - `Healthy`：正常参与调度；
  - `Draining`：计划内维护中——不再接收新任务，存量任务被安全迁出，心跳照常；
  - `Dead`：故障被判死 **或** 排空完成后退役，容量退出集群。
    其中“退役”这一来源会写入快照 `Retired`：**跨进程持久**，重启同名注册仍是 Dead；
    “故障判死”只在当前进程有效，重启后同名注册视为一台新机器。
- **作业状态**：`Pending → Running → Succeeded/Failed`；
  **任务状态**：`Pending → Running → Succeeded/Failed`。
  `TaskRuntime` 另带 `Attempt`（故障/重启导致的重放次数；计划内迁移不计）
  与 `Progress`（本次启动前已完成 tick 数；仅计划内迁移保留，随快照落盘）。

## 3. 调度规则

### 3.1 提交校验（三类可区分拒绝原因）

`Submit` 返回的错误可用标准库 `errors.Is` 判定：

| 哨兵错误 | 触发条件 |
| --- | --- |
| `api.ErrInvalidRequest` | 作业 ID 为空、无任务、任务名为空/重复、资源为负、`Duration<=0` |
| `api.ErrInsufficientResources` | 作业在**当前集群拓扑下永远放不下**：存在任务超过最大单节点容量，或全部任务资源之和超过健康节点容量之和 |
| `api.ErrDuplicateJob` | 作业 ID 已存在——**包括已结束作业**，结束作业不会复活 |

- “当前资源被占用导致这一轮放不下”属于**暂时不足**，作业会进入队列等待，不会被拒绝。
- 拒绝时日志打印具体 `reason`（哪个任务、多少资源、对比容量），失败原因可区分、可解释。

### 3.2 Gang（整作业）放置

- 一个作业的**全部 `Pending` 任务必须作为整体获得资源后才一起启动**：
  `Pool.Plan` 先做纯计算的放置方案，任一任务放不下则整体返回 `nil`，账本零改动；
  `Pool.TryCommit` 再做原子提交，任一节点占用失败则整体不生效。
- **确定性**：节点按 ID 排序、任务按提交顺序处理，先在暂记账本上模拟、通过后才落账，
  因此无论调度遍历顺序如何，放置结果都一致。
- **优先级队列**：每个 tick 按 `Priority` 降序、同优先级按作业 ID 升序尝试；
  队头作业这一轮放不下时**不阻塞**后续作业（non head-of-line blocking）。
- 故障后作业可能同时含 `Running` 与 `Pending` 任务：只要存在 `Pending` 任务，
  该作业就继续参与 gang 放置（剩余任务不会被饿死）。

### 3.3 作业终态与任务结果一致

- 全部任务 `Succeeded` → 作业 `Succeeded`；任一任务 `Failed`（故障注入）→ 作业 `Failed`，
  `JobRuntime.Reason` 记录 `task failed: <name>`。
- 完成/终止事件处理是**幂等**的：只对“当前确为 Running 且位于该节点”的任务生效，
  迟到/重复事件被忽略并打印日志，已完成任务不会重复执行或重复计入。

## 4. 心跳与节点故障回收

1. 每个 tick，仍存活（未被故障注入摘除）的健康节点刷新心跳时间戳。
2. `TriggerNodeFailure(nodeID)` 模拟节点故障：停止为该节点刷新心跳。
3. 统一的超时检测路径：当 `now - lastBeat > HeartbeatTimeoutTicks` 时：
   - 节点标记 `Dead`，其占用账本清零并记录 `Released`（日志可解释）；
   - `Executor.KillOnNode` 终止该节点上的在执行行任务，释放资源、发 `EvKilled` 事件；
   - 调度器把这些**当时确在该节点运行**的任务按**故障语义**复位为 `Pending`、
     `Attempt+1`、**执行进度清零**（`Progress=0`，从头重跑）、清空 NodeID，
     随后由正常 gang 调度重新放置到其他健康节点。
4. **已完成任务不受影响**：死亡节点上已 `Succeeded/Failed` 的任务保留结果，不重跑、不重计。
   迟到心跳也不会让已判死节点复活。
5. 资源账本不变量：健康节点 `Used` 之和 == 执行器在执行行请求之和；终态后全部归零，
   不允许超发（`Used > Capacity`）或负数占用。

## 4.5 计划内维护 / 排空（Drain）

与“故障回收”被动等心跳超时不同，排空是**主动**把一个健康节点摘出集群的运维操作，
三个 API 均可在任意时刻与调度推进、作业提交并发调用（全部由 `s.mu` 串行化）：

1. **`DrainNode(nodeID)`**：把健康节点置为 `Draining`。
   - 从这一刻起节点**不再接收新任务**：gang 放置（`Pool.Plan/TryCommit`）与提交时的
     容量可行性判断（`fitsClusterLocked`）都只统计 `Healthy` 节点，
     即集群可用容量**立即按收缩后计算**；
   - 排空标记**先落盘再生效**（随作业快照持久化），此时崩溃也不会在重启后
     把维护中的节点当成正常机器；
   - 节点**心跳照常刷新**——它是计划内维护，不会被超时检测误判死。
   - 节点不存在 / 已排空中 / 已死亡分别返回
     `ErrNodeNotFound` / `ErrNodeNotHealthy`，可区分。
2. **迁移（随 Tick 推进）**：每个 Tick 驱逐所有 `Draining` 节点上的在执行任务
   （`Executor.EvictOnNode` → `EvDrained` 事件，事件携带任务**剩余节拍数**）。
   被驱逐任务是**安全迁移、不算丢失**：
   - 复位为 `Pending` 但**保留执行进度**（`TaskRuntime.Progress` = 已完成的 tick 数，
     executor 在新节点上按 `Duration-Progress` 的剩余节拍继续跑），
     **`Attempt` 不增加**——迁移不是一次重试；
   - 随同一 tick 的 gang 调度整体迁到其它有余量的健康节点；暂时放不下就继续排队
     （`Pending`，进度与尝试次数原样保留），不超发、不错误判终态；
   - 迁移在同一 tick 内完成时节拍不丢不重（驱逐发生在 tick 内执行推进之前，
     任务在新节点上正常吃掉这一拍）；因此“迁移完成时刻”恰好等于无迁移时的完成时刻；
   - 已完成（`Succeeded/Failed`）任务不在执行行中，不受驱逐影响，绝不重跑、不重复统计。
   - 对照：**节点故障**（`EvKilled`）走另一条分支——执行现场丢失，任务**从头重跑**
     （`Progress` 清零）且 **`Attempt+1`**；进程**重启恢复**中的在执行任务同样
     从头重跑并 `Attempt+1`（见第 5 节）。三条链路语义严格区分：

     | 链路 | Attempt | 已执行进度 | 触发 |
     | --- | --- | --- | --- |
     | 计划内排空迁移 `EvDrained` | 不变 | **保留**，接着剩余节拍跑 | 主动 `DrainNode` |
     | 节点故障回收 `EvKilled` | +1 | 清零，从头重跑 | 心跳超时 |
     | 进程重启恢复 | +1 | 清零，从头重跑 | 进程重启 + `Recover` |

3. **`DrainStatus(nodeID)`**：返回 `DrainInfo{State, RemainingTasks, RemainingResources}`，
   即“还剩多少任务/资源没迁完”，排空推进的每个 tick 都可观测，迁完归零。
4. **`CompleteNodeDrain(nodeID)`**：节点上任务全部迁走（或本来就没任务）后正式退役：
   状态转 `Dead`、停止心跳，容量从此永久退出集群；同时**原子地**清除排空标记、
   把节点 ID 写入快照的 `Retired` 集合并落盘。仍有任务未迁完时返回 `ErrNodeNotEmpty`，
   节点保持 `Draining`，迁移继续推进。

### 4.6 退役节点的重启语义（下线状态持久化）

“已下线”是**跨进程**事实，由快照中的 `Retired` 列表保证（与仅在排空期间有效的
`Draining` 标记分开）：

- `CompleteNodeDrain` 成功即落盘 `Retired += nodeID`；
- 重启后无论节点是在 `Recover()` **之前**还是**之后**重新 `AddNode` 注册：
  调度器都按退役记录把它置为 `Dead`，不参与放置、不刷新心跳，
  **健康容量不会涨回**（注册本身不报错，但节点始终保持 Dead 并打日志说明）；
- 之后提交的作业严格按收缩后的健康容量判定：永远放不下 → `ErrInsufficientResources`；
  能收下但暂时放不下 → 排队 `Pending`，容量释放后再调度，不会误判成功/失败或超发；
- 要让该 ID 真正重新上岗，属于运维显式“重新上线”动作（当前模拟未提供该 API：
  同名注册一律视为已退役机器，保持 Dead）。

**与相近机制的语义边界：**

| 机制 | 触发方式 | 节点上运行中任务 | 容量何时扣除 | 节点可否回来 |
| --- | --- | --- | --- | --- |
| 故障回收 | 被动：心跳超时 | 判死时被杀、进度清零、`Attempt+1`、从头重调度 | 判死时刻 | 不会（本模拟中 Dead 即终态） |
| 计划内排空 | 主动：`DrainNode` | **安全驱逐、进度保留、Attempt 不变**，迁到健康节点接着剩余节拍跑 | 标记排空即退出新任务分配；退役后永久扣除 | 退役后不会（`Retired` 持久化，重启同名注册仍 Dead；未提供重新上线 API） |
| 容量收缩 | 排空/故障的副作用 | — | 新提交按收缩后容量判定；已排队作业继续等待，不被误判成功/失败 | — |
| 重启恢复 | 进程重启 + `Recover` | 执行行随进程消失，统一复位为 `Pending`、进度清零、`Attempt+1` 重调度 | 排空标记/退役记录随快照恢复：维护中节点保持 Draining、已退役节点保持 Dead | — |

**边界与兼容范围：**

- 排空中的节点若在维护期间真的发生故障（停止心跳），走与故障一致的
  超时回收路径转 `Dead`，其排空标记随即被清除（重启后不会被错误恢复成 Draining），
  也不能再对它调用 `CompleteNodeDrain`（返回 `ErrNodeNotDraining`）。
  注意 Tick 内“安全驱逐”先于“超时检测”，因此带任务的排空中节点总会先完成安全迁移，
  故障路径只可能作用到已迁空、尚未退役的节点。
- 多台节点可同时/先后排空，各自的迁移与退役独立推进，账本始终一致；
  排空剩余任务/资源经 `DrainStatus` 持续可观测。
- 排空做到一半进程重启：`Recover` 会把持久化的排空标记重新应用到已注册节点
  （`AddNode` 在 `Recover` 之后调用时也会按标记补应用，两种顺序都安全），
  迁到一半的任务按**重启恢复语义**（进度清零、`Attempt+1`）复位后继续调度，
  不会丢失或重复统计；恢复后可继续 `CompleteNodeDrain` 完成退役。
- 退役记录（`Retired`）按并集参与恢复合并：任何交错顺序下，已退役节点都不复活。
- 已退役节点不会被复活；同名 ID 重新 `AddNode` 会被接受但立即保持 Dead。
  如需真正重新上线，必须新增显式 recommission 操作（当前模拟未实现）。
- 进度保留的粒度是“已完成的整数 tick”，记录在 `TaskRuntime.Progress` 并随快照持久化；
  它只在**排空迁移**这一条链路被使用，故障与重启均按清零语义处理。

## 5. 进程重启恢复

- 每次作业状态变更（接纳、启动、完成、失败、复位）与节点维护状态变更
  （`DrainNode` / `CompleteNodeDrain` / 排空中节点故障后的标记清除）都立即用
  **临时文件 + 原子 rename** 写一张完整 JSON 快照（含作业表、`Draining` 列表、
  `Retired` 退役列表），并将版本号 `Counter` +1。
- 进程重启后构造新的调度器并重新注册节点（节点池与执行行从空白开始），
  调用 `Scheduler.Recover()`：
  - **已结束作业**（Succeeded/Failed）原样保留，不参与调度，重新提交时返回 `ErrDuplicateJob`，
    **不会复活**；
  - **未完成作业**继续调度；崩溃前处于 `Running` 的任务因执行行已不存在，
    按重启恢复语义统一复位为 `Pending`、清空 NodeID、`Attempt+1`、进度清零，
    随下一次 tick 重新 gang 放置（从头跑）；
  - **排空中节点**按快照中的排空标记恢复为 `Draining`（不会被当成健康机器
    重新投入调度），其上未迁完的任务随复位后的 gang 调度迁往健康节点，
    之后可继续 `CompleteNodeDrain` 完成退役；
  - **已退役节点**按快照中的 `Retired` 记录恢复为 `Dead`，即使在恢复后以同一 ID
    重新 `AddNode` 也保持 Dead，**已收缩的容量不反弹**；
  - 新节点池账本初始为零，因此**不会重复占用**崩溃前的资源。
- **并发安全**：所有作业状态访问都由调度器的单把 `s.mu` 串行化；
  `state.Store` 不再自持锁，保证“查重 + 写入”“恢复合并”等复合操作原子。
  `Recover` 采用**合并而非整表覆盖**：内存中已存在的更新事实保留，
  仅并入快照中缺失的作业，版本号取较大值，避免恢复把并发提交冲掉。
  **重启契约：`Recover()` 在调度循环启动前执行一次**，之后 Tick 才开始，
  从而杜绝恢复与调度同时进行时的重复占用与作业丢失。

## 6. 可观测性

- 调度器通过 `log/slog` 输出关键决策：作业接纳/三类拒绝原因、gang 放置明细
  （任务→节点及资源量）、延迟放置原因、心跳超时与回收资源量、任务被杀与重调度、
  作业终态。测试通过 `t.Log` 桥接，`go test -v` 可直接看到这些依据。

## 7. 验证方法

```bash
# 构建与静态检查
go build ./...
go vet ./...

# 全部单测（-v 可看到关键调度决策日志）
go test ./... -v

# 竞态检测
go test ./... -race

# 高重复次数验证调度结果确定性 / 无偶发
go test ./pkg/scheduler/ -count=200

# 竞态 + 高重复，重点压排空/恢复交错路径
go test ./pkg/scheduler/ -race -count=10 -run 'Concurrent|Drain|Recover'

# 端到端演示：提交→三类拒绝→故障重调度→计划内排空下线（带进度迁移）→重启不复活
go run ./cmd/gangscheduler
```

本地复现/人工验证要点（演示程序会直接打印）：

1. `job-web` 的 `w2` 在 node-c 排空调离后于 node-a 上完成，
   打印为 `attempt=0 progress=1/3`——迁移保留进度、不增加尝试次数；
   对照 `job-app` 的 `t2` 经历节点故障，打印为 `attempt=1`——故障计数。
2. phase 2 重启后日志出现 `re-applied retired state`，node-c 仍为
   `state=Dead`，且“超收缩容量”的作业继续被拒绝。
3. 自动化等价断言：`TestDrainMigrationPreservesProgressAndAttempt`
   （迁移后恰好在原完成时刻 tick6 完成）与
   `TestFailureRescheduleRunsFromStartAndCountsAttempt`
   （故障后必须完整重跑 6 拍、tick9 才完成）成对锁定两条链路的语义差异。

### 测试覆盖

| 场景 | 测试 |
| --- | --- |
| 非法 / 重复 / 资源不足三类拒绝原因可区分 | `TestSubmitInvalid` `TestSubmitInsufficient` `TestSubmitDuplicate` |
| 暂时不足排队、释放后按优先级调度 | `TestQueueAndPriority` |
| gang 全有或全无、放置确定性、原子提交 | `pkg/cluster`：`TestPoolPlanAllOrNothing` 等 |
| 运行中节点故障→任务复位→换节点重调度 | `TestNodeFailureReschedule` |
| 已完成任务在节点死后不重跑、不重计 | `TestCompletedTaskNotRerun` |
| 任务失败→作业 Failed 且原因可区分、失败幂等 | `TestTaskFailureFailsJob` |
| 全流程资源账本不变量（占用==在执行行、终态归零） | `TestResourceAccountingInvariant` |
| 并发重复提交恰好一个成功 | `TestConcurrentSubmitSameID` |
| 调度与提交并发下资源/状态不变量 | `TestConcurrentTicksAndSubmit`（-race） |
| 终态作业恢复不复活、重复提交仍拒绝 | `TestRecoverTerminalNotResurrected` |
| 崩溃时运行中任务恢复后继续并完成、不重复占用 | `TestRecoverRunningTasksResumed` |
| 运行期快照可跨实例继续、版本号单调 | `TestRecoverPersistRoundTrip` |
| 恢复后并发提交+调度不丢作业、不泄漏资源 | `TestRecoverConcurrentWithTicks` |
| 存储深拷贝隔离、持久化往返 | `pkg/state` 全部测试 |
| 排空迁移：任务安全迁到健康节点、进展可见、退役后容量收缩 | `TestDrainMigratesTasks` |
| 排空迁移保留执行进度与尝试次数（attempt 不增、tick6 准时完成）；与故障链路对照（attempt+1、从头跑） | `TestDrainMigrationPreservesProgressAndAttempt`、`TestFailureRescheduleRunsFromStartAndCountsAttempt` |
| 空节点立即退役；收缩后提交拒绝、排队作业不误判 | `TestDrainEmptyNodeAndCapacityShrink` |
| 排空操作的可区分错误（未知节点/重复排空/未排空就退役等） | `TestDrainErrors`、`pkg/cluster`：`TestPoolCompleteDrain` 等 |
| 已完成任务不受排空影响 | `TestDrainCompletedTaskNotDisturbed` |
| 排空中节点心跳照常；维护期间真故障仍被回收且标记清除 | `TestDrainingNodeKeepsHeartbeat`、`TestPoolDrainExcludesPlacement`、`TestDrainInterleavedWithFailure` |
| 排空驱逐事件携带剩余节拍、迁移后按剩余节拍完成 | `pkg/execution`：`TestEvictOnNodeCarriesProgress` |
| 排空到一半重启：节点保持排空、任务继续迁移、可继续退役 | `TestDrainRecoverMidDrain`、`TestDrainInterleavedWithRestart` |
| 退役节点重启后同名注册仍 Dead、容量不反弹（两种注册顺序） | `TestRetiredNodeSurvivesRestart`、`TestRetiredNodeReRegisteredAfterRecoverStaysDead`、`pkg/state`：`TestStoreRetiredAndProgressRoundTrip` |
| 多节点先后排空与 Tick/提交交错：不丢作业、不重复统计 attempt、账本逐拍一致 | `TestMultiDrainInterleaveDeterministic` |
| 排空与调度推进/提交并发交错不丢作业、不超发 | `TestDrainConcurrentWithTicksAndSubmit`（-race）、`TestDrainSubmitTickConcurrentInterleave`（-race） |

## 8. 已知边界与取舍

- 容量可行性是基于**提交时刻的静态集群拓扑**判断；若之后故障或计划内退役导致集群容量
  永久缩减，一个原本可行的作业可能长期停留 `Pending`（不会错误地成功或超发）。
- 计划内迁移保留进度的前提是任务被安全驱逐（`EvDrained` 携带剩余节拍）；
  进程重启不保留执行行，恢复时在执行任务一律按故障语义从头重跑（与真实系统一致：
  崩溃丢失的是执行现场，而排空是编排层可控的状态移交）。
- 当前模拟未提供已退役节点的“重新上线（recommission）”操作：同名 `AddNode` 会保持
  Dead；ID 复用语义按“同一台已退役机器”处理。
- 模拟执行不区分抢占：已运行任务不会被高优先级作业抢占，只在节点故障或排空时被迁走。
- 快照为全量 JSON、每次变更同步落盘，面向“可解释、易验证”的教学/演示用途，非高吞吐生产实现。
