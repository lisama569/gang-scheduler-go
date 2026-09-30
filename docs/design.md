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
  - `Dead`：故障被判死、排空完成后退役，或重启后按退役名单压回的下线态，容量不参与调度。
- **作业状态**：`Pending → Running → Succeeded/Failed`；
  **任务状态**：`Pending → Running → Succeeded/Failed`。
- **任务执行计量**：`Attempt` 记录非计划中断后的重试次数（节点故障、进程崩溃各 +1）；
  **计划内排空迁移不增加 `Attempt`**。`Progress` 记录已执行完的 tick 数，
  计划内迁移与重启恢复都保留进度续跑；只有节点故障杀死会把进度清零、从头重跑。

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
   - 调度器把这些**当时确在该节点运行**的任务复位为 `Pending`、`Attempt+1`、清空 NodeID，
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
2. **迁移（随 Tick 推进）——安全续跑，不是故障重跑**：每个 Tick 驱逐所有
   `Draining` 节点上的在执行任务（`Executor.EvictOnNode` → `EvDrained` 事件）。
   计划内驱逐与故障杀死（`EvKilled`）是**两条严格区分的链路**：

   | | 计划内排空 `EvDrained` | 节点故障 `EvKilled` |
   | --- | --- | --- |
   | 触发 | 主动：`DrainNode` 后随 Tick 驱逐 | 被动：心跳超时判死 |
   | 任务状态 | 复位 `Pending`，随同 tick 的 gang 调度整体迁到健康节点 | 复位 `Pending`，等后续 tick 重新放置 |
   | **`Attempt`** | **不变**（主动迁移不是一次故障重试） | **+1** |
   | **`Progress`** | **保留**：带已执行 tick 数迁移，在新节点上接着原来的进度跑 | **清零**：执行现场随故障丢失，从头重跑 |
   | 作业终态 | 绝不因迁移失败；暂时放不下就继续排队 | 同样不判终态，任务重跑到成功/注入失败为止 |

   `TaskRuntime.Progress` 记录任务已执行完的 tick 数：执行器每个 tick 推进后由调度器
   按作业聚合并落盘，因此“迁到一半 / 跑到一半”的进度在驱逐和崩溃两种情况下都可续。
   已完成（`Succeeded/Failed`）任务不在执行行中，不受驱逐影响，绝不重跑、不重复统计。
3. **`DrainStatus(nodeID)`**：返回 `DrainInfo{State, RemainingTasks, RemainingResources}`，
   即“还剩多少任务/资源没迁完”，供运维观测排空进展。注意“任务已驱逐但还在排队等
   健康节点余量”时 `RemainingTasks` 即为 0（节点视角已空），任务在作业视角仍是
   `Pending`，账本不超发。
4. **`CompleteNodeDrain(nodeID)`**：节点上任务全部迁走（或本来就没任务）后正式退役：
   状态转 `Dead`，节点 ID 从排空名单移入**退役名单**并在**同一次快照写入**中落盘，
   停止心跳，容量从此永久退出集群。仍有任务未迁完时返回 `ErrNodeNotEmpty`，
   节点保持 `Draining`，迁移继续推进。
5. **退役的持久性与重新上线**：
   - **已退役节点不能靠重启后重新注册复活**：`AddNode` 发现节点 ID 在退役名单中时
     返回 `ErrNodeRetired` 拒绝注册，节点不会出现在新节点池里，集群容量保持收缩。
     退役/排空名单在调度器 `New` 时就从快照**预加载**（`Store.LoadMarkers`），
     所以即使调用方在 `Recover` 之前先 `AddNode`，拦截也已经生效，不存在窗口期；
     `Recover` 会再次把名单应用到已注册节点（先注册后恢复的顺序同样安全）。
   - 运维确实要让该机器重新上线时，必须**显式**调用 `ReadmitNode(nodeID, capacity)`：
     节点恢复 `Healthy`、清除退役标记并落盘、恢复心跳、容量重新计入集群（可携带新规格）。
     对从未退役的节点/未知 ID 调用返回 `ErrNodeNotRetired`（新机器请走 `AddNode`）。

**与相近机制的语义边界：**

| 机制 | 触发方式 | 节点上运行中任务 | Attempt / Progress | 容量何时扣除 | 节点可否回来 |
| --- | --- | --- | --- | --- | --- |
| 故障回收 | 被动：心跳超时 | 判死时被杀、复位重调度 | Attempt+1、Progress 清零从头跑 | 判死时刻 | 不会（本模拟中 Dead 即终态） |
| 计划内排空 | 主动：`DrainNode` | 安全驱逐、迁移到健康节点继续跑 | **Attempt 不变、Progress 保留续跑** | 标记排空即退出新任务分配；退役后永久扣除 | 退役后不会被隐式复活；只有显式 `ReadmitNode` 可重新上线 |
| 容量收缩 | 排空/故障的副作用 | — | — | 新提交按收缩后容量判定；已排队作业继续等待，不被误判成功/失败 | — |
| 重启恢复 | 进程重启 + `Recover` | 执行行随进程消失，统一复位为 `Pending` 重调度 | Attempt+1（崩溃是非计划中断），**崩溃前已落盘的 Progress 保留续跑** | 排空/退役名单随快照恢复：维护中节点不当健康机器、已退役节点不复活 | 名单先于节点注册预加载，两种注册/恢复顺序都安全 |

**边界与兼容范围：**

- 排空中的节点若在维护期间真的发生故障（停止心跳），走与故障一致的
  超时回收路径转 `Dead`，其未迁完任务按**故障语义**复位重调度（Attempt+1、
  Progress 清零）；同时清除残留排空标记（它不是退役，不进退役名单）。
- 多台节点可同时/先后排空，各自的迁移与退役独立推进，账本始终一致。
- 排空做到一半进程重启：`Recover` 会把持久化的排空标记重新应用到已注册节点
  （`AddNode` 在 `Recover` 之后调用时也会按标记补应用，两种顺序都安全），
  迁到一半的任务按恢复语义复位（Attempt+1、Progress 保留）后继续调度，
  不会丢失或重复统计。
- 已退役节点不会被复活：重启后 `AddNode` 同名节点返回 `ErrNodeRetired`，
  容量保持收缩；若运维要重新上线，显式 `ReadmitNode` 重新注册（可携带新容量），
  该事实同样持久化，再次重启后节点是普通健康机器。
- 故障杀死的节点（非排空退役）不在退役名单中：本模拟里它同样是 Dead 终态，
  若要复用该机器，直接 `ReadmitNode` 即可（Dead 即“可重新上线”的统一入口）。

## 5. 进程重启恢复

- 每次作业状态变更（接纳、启动、完成、失败、复位、**进度推进**）与节点标记变更
  （`DrainNode`/`CompleteNodeDrain`/`ReadmitNode`）都立即用 **临时文件 + 原子 rename**
  写一张完整 JSON 快照（含作业表、排空中节点列表 `Draining` 与已退役节点列表 `Retired`），
  并将版本号 `Counter` +1。
- 进程重启后构造新的调度器并重新注册节点（节点池与执行行从空白开始），
  调用 `Scheduler.Recover()`：
  - **已结束作业**（Succeeded/Failed）原样保留，不参与调度，重新提交时返回 `ErrDuplicateJob`，
    **不会复活**；
  - **未完成作业**继续调度；崩溃前处于 `Running` 的任务因执行行已不存在，
    统一复位为 `Pending`、清空 NodeID、`Attempt+1`（崩溃是非计划中断），
    但崩溃前已随每次 Tick 落盘的 **`Progress` 予以保留**，重新放置后只补跑剩余节拍；
  - **排空中节点**按快照中的排空标记恢复为 `Draining`（不会被当成健康机器
    重新投入调度），其上未迁完的任务随复位后的 gang 调度迁往健康节点，
    之后可继续 `CompleteNodeDrain` 完成退役；
  - **已退役节点**按退役名单保持下线：`AddNode` 同名节点被 `ErrNodeRetired` 拒绝，
    即使节点已先注册也会在 `Recover` 时被压回 `Dead`；集群容量保持收缩，
    只有显式 `ReadmitNode` 能让它回来；
  - 节点标记在调度器 `New` 时就通过 `Store.LoadMarkers()` 预加载，
    因此“先注册节点、后 Recover”与“先 Recover、后注册节点”两种顺序结果一致；
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

# 并发/交错场景在竞态检测下多轮重复（排空混沌、提交竞态、恢复并发）
go test ./pkg/scheduler/ -race -count=50 \
  -run 'TestDrainConcurrentChaos|TestDrainConcurrentWithTicksAndSubmit|TestConcurrentTicksAndSubmit|TestRecoverConcurrentWithTicks'

# 端到端演示：提交→三类拒绝→故障重调度（attempt+1/进度清零）
#   →计划内排空续跑（attempt 不变/进度保留）→退役下线→重启不复活→显式重新上线
go run ./cmd/gangscheduler
```

### 本地手工复现要点

1. **迁移续跑 vs 故障重跑**：提交一个长任务作业后 `Tick` 若干拍，对任务所在健康节点
   调 `DrainNode` 再 `Tick`，观察任务的 `Progress` 连续、`Attempt` 不变、作业在原定
   tick 数处完成；对照 `TriggerNodeFailure` 后 `Attempt+1`、`Progress` 归 0。
2. **退役扛重启**：`CompleteNodeDrain` 后用同一 `StateFile` 新建调度器，
   `AddNode` 同名节点应得到 `ErrNodeRetired`，健康容量总和不含该节点；
   调 `ReadmitNode` 后容量才恢复。
3. **排空剩余可观测**：`DrainStatus` 在驱逐前后分别返回剩余任务/资源量；
   任务“已驱逐但排队等容量”时节点视角已空（0）、作业视角为 `Pending`。

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
| **排空迁移保留执行进度、Attempt 不增长；与故障 Attempt+1/清零严格区分；连续多次迁移进度累加** | `TestDrainMigrationPreservesProgressAndAttempt` `TestFailureRescheduleSemanticsUnchanged` `TestDrainThenMigrateAgainProgressKept` |
| **退役节点重启后重新注册被拒（`ErrNodeRetired`）、不复活、容量保持收缩；标记在 Recover 前已生效** | `TestRetiredNodeNotResurrectedOnRestart` `TestRetiredMarkerEffectiveBeforeRecover` `TestDrainingMarkerEffectiveBeforeRecover` |
| **显式 `ReadmitNode` 恢复容量并持久化；非法重新上线可区分；收缩后排队/永久放不下语义** | `TestReadmitRetiredNodeBringsCapacityBack` `TestQueuedJobStaysPendingAfterShrink` |
| **排空与提交/调度/重启恢复的确定性交错（进度连续、账本不超发、作业不复活）与并发混沌（-race）** | `TestDrainInterleavedDeterministic` `TestDrainConcurrentChaos` |
| **节点池退役/重新上线原语** | `pkg/cluster`：`TestPoolMarkRetiredAndReadmit` |
| **退役名单原子迁移、进度快照往返、标记预加载** | `pkg/state`：`TestStoreRetiredAndProgressRoundTrip` `TestStoreLoadMarkersMissingFile` |
| 空节点立即退役；收缩后提交拒绝、排队作业不误判 | `TestDrainEmptyNodeAndCapacityShrink` |
| 排空操作的可区分错误（未知节点/重复排空/未排空就退役等） | `TestDrainErrors`、`pkg/cluster`：`TestPoolCompleteDrain` 等 |
| 已完成任务不受排空影响 | `TestDrainCompletedTaskNotDisturbed` |
| 排空中节点心跳照常；维护期间真故障仍被回收 | `TestDrainingNodeKeepsHeartbeat`、`TestPoolDrainExcludesPlacement` |
| 排空到一半重启：节点保持排空、任务继续迁移、可继续退役 | `TestDrainRecoverMidDrain` |
| 排空与调度推进/提交并发交错不丢作业、不超发 | `TestDrainConcurrentWithTicksAndSubmit`（-race） |

## 8. 已知边界与取舍

- 容量可行性是基于**提交时刻的静态集群拓扑**判断；若之后故障或计划内退役导致集群容量
  永久缩减，一个原本可行的作业可能长期停留 `Pending`（不会错误地成功或超发）。
- 模拟执行不区分抢占：已运行任务不会被高优先级作业抢占，只在节点故障或排空时被迁走。
- 快照为全量 JSON、每次变更同步落盘，面向“可解释、易验证”的教学/演示用途，非高吞吐生产实现。
