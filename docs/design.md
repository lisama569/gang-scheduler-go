# Gang 集群作业调度器（单进程模拟）

在单个 Go 进程内用**模拟节点 + 逻辑时钟心跳**驱动多个作业并发执行，
演示 gang（整作业）调度、优先级队列、节点故障回收、**计划内维护/节点排空（在线迁移 + 容量收缩）**、
以及进程重启后的状态恢复。
设计目标是：**在不同调度顺序下，资源分配、故障恢复、维护迁移与作业状态保持一致且可解释**。

## 1. 目录结构

| 路径 | 职责 |
| --- | --- |
| `pkg/api` | 核心领域类型（Resource/Node/Task/Job）、资源运算、可区分错误 |
| `pkg/cluster` | 模拟节点与节点池：容量账本、心跳、超时检测、gang 放置的“全有或全无”提交，以及计划内维护的排空/原子迁移/退役 |
| `pkg/execution` | 模拟执行器：按 tick 推进任务、故障终止、在线迁移改挂、故障注入，通过事件通道上报 |
| `pkg/state` | 作业与节点状态存储：内存表 + JSON 快照原子落盘（临时文件 rename）+ 合并恢复 |
| `pkg/scheduler` | 调度器：提交校验、队列调度、心跳回收、维护排空迁移、事件聚合、恢复 |
| `cmd/gangscheduler` | 可本地运行的端到端演示 |

## 2. 模型与概念

- **Resource**：`{CPU, Memory}` 标量资源；`Fits / Add / Sub` 运算见 `pkg/api/resource.go`。
- **逻辑时钟（tick）**：`Scheduler.Tick()` 推进一个时钟周期。
  既可用事件方式逐 tick 调用（测试，确定可断言），也可用 `Run(stop)` 按 10ms 自动推进。
- **任务时长 Duration**：任务启动后需推进的 tick 数；启动发生在 tick 内的调度阶段，
  tick 末尾执行器统一推进一拍。
- **节点**：有 `Capacity` 与 `Used` 账本，以及最近心跳时间戳；生命周期状态有四种：

  | 状态 | 含义 | 是否接收新任务 | 是否计入可用容量 |
  | --- | --- | --- | --- |
  | `Healthy` | 正常服役 | 是 | 是 |
  | `Draining` | 计划内维护/排空中（节点仍在线） | 否 | 否（提交判定），但仍承载在迁任务 |
  | `Retired` | 排空完成、正式退役 | 否 | 否 |
  | `Dead` | 心跳超时故障 | 否 | 否 |

- **作业状态**：`Pending → Running → Succeeded/Failed`；
  **任务状态**：`Pending → Running → Succeeded/Failed`。

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

## 4A. 计划内维护 / 节点排空（与故障回收的区别）

计划内维护是“**我主动挑一台健康机器安全下线**”，语义与故障回收刻意区分：

| 维度 | 故障回收（第 4 节） | 计划内维护/排空（本节） |
| --- | --- | --- |
| 触发 | 心跳超时（被动判定） | `Scheduler.DrainNode(id)`（主动发起） |
| 在跑任务 | 直接按丢失处理：`Kill` → 复位 `Pending` → 重新 gang 放置 | **在线迁移**：不杀、不复位、不重跑 |
| Attempt | `+1` | **不变**，剩余执行节拍保留 |
| 心跳 | 已停止 | 排空中**继续续心跳**，不会被误判为故障 |
| 终态 | 节点 `Dead`，账本清零 | 任务全部迁走后才 `Retired`，账本为零 |
| 容量 | 故障即从可用容量消失 | 排空发起即不接新任务，退役后才从可用容量扣除 |

### 4A.1 状态机与生命周期

`Healthy --DrainNode--> Draining --(任务全部迁走)--> Retired`

- `DrainNode` 只能作用于 `Healthy` 节点；对 `Draining/Retired/Dead` 或不存在的节点返回错误。
- `Draining` 节点仍在线、继续刷新心跳，但 `Pool.Plan`/`PlanTarget` 只选健康节点，
  因此它**从此不再接收新任务**。
- `Retired` 是终态：容量从集群可用容量中永久扣除（直到进程外重新注册节点）。

### 4A.2 在线迁移的正确性（不丢、不重、不错账）

每个 tick 在“故障回收之后、正常 gang 调度之前”执行排空推进：

1. 收集排空中节点上**当前确实在执行行**的运行中任务，按作业优先级、作业 ID、
   任务下标确定**确定性迁移顺序**。
2. 对每个任务，在调度器单把锁 `s.mu` 内串行完成四步，任一步失败则整体未发生/回滚：
   - `PlanTarget`：在健康节点中按 ID 选一个放得下的目标（纯计算，零改动）；
   - `MigrateOne`：**先在目标占用成功，才从源节点归还**；目标放不下则双方账本不变；
   - `Executor.Migrate`：只把在执行行的节点归属从源改到目标（保留剩余节拍，不发事件）；
   - 任务视图 `NodeID` 原子更新并立即落盘。
3. 迁不出去（没有放得下的健康节点）时，任务**保持在排空中节点上继续执行**，
   下一 tick 再试；节点继续续心跳，因此不会被误判故障。这天然处理“腾挪”——
   先迁走的任务在健康节点腾出空间后，后续任务才放得下。
4. 以存储事实重新清点：该节点上已无运行中任务且账本为零时，`Draining → Retired`
   并立即落盘。本就空闲的节点下一次推进即退役。

- **任务不凭空消失**：迁移成功 = 目标占用 + 执行行改挂 + 视图更新三者同时成立；
  失败则全部回滚。
- **资源不重复占用**：账本迁移是“先占目标、再放源”的原子操作，全程持锁，
  与提交、调度、故障检测互斥。
- **作业/任务结果自洽**：迁移不改任务状态（始终 `Running`），作业保持 `Running`；
  已完成（`Succeeded/Failed`）任务不在执行行中，**绝不迁移、不重跑、不重计**。
- **进展可见**：`Scheduler.DrainProgress()` 返回每个维护中/已退役节点的
  `State` 与 `Remaining`（尚未迁走的在执行行任务数），退役时 `Remaining==0`。

### 4A.3 容量收缩后的提交与排队

- 提交的“永远放不下”可行性判断（§3.1）只统计 **`Healthy` 节点**：
  `Draining/Retired/Dead` 都不计入。因此排空一发起，新提交就按收缩拓扑判定。
- **已在队列中的作业不回溯重判**：排空只影响下一轮 gang 放置，不会把排队作业
  改成成功或失败。若容量永久收缩导致一个**此前已收下**的作业从此再也放不下，
  它继续保持 `Pending`（状态可解释为等待资源），任务保持未绑定、绝不部分启动或超发；
  而收缩后**新提交**的同类作业会立即收到 `ErrInsufficientResources`。
  这是“已接受作业受保护、新提交按当前拓扑裁决”的有意取舍。
- 多台节点先后进入维护、部分仍在排空中时，账本与可用容量按上述规则逐台扣减，
  始终一致；只有任务全迁走的节点才退役。

### 4A.4 排空与故障并发

排空中节点若心跳也停止（维护期间又发生故障），统一的超时检测路径同样覆盖 `Draining`
节点：它会被判 `Dead`、账本清零，其上尚未迁走的任务按故障语义 `Kill → 复位 → 重调度`，
账目依旧守恒（见 `TestDrainThenNodeFailure`）。`Retired` 节点无心跳也不会被判死。

## 5. 进程重启恢复

- 每次作业状态变更（接纳、启动、完成、失败、复位、迁移）以及节点生命周期变更
  （发起排空、退役）都立即用 **临时文件 + 原子 rename** 写一张完整 JSON 快照，
  并将版本号 `Counter` +1。快照在作业之外还包含**全量节点记录**
  （`NodeRecord{ID, Capacity, State}`）。
- 进程重启后构造新的调度器并重新注册节点（节点池与执行行从空白开始），
  调用 `Scheduler.Recover()`：
  - **节点生命周期重放**：`Draining/Retired/Dead` 状态按快照恢复，
    **排空中的节点重启后仍是排空中，不会被当成健康机器重新接任务**，
    已退役节点容量持续扣除。重放要求节点已用**相同 ID 与容量**重新注册，
    缺失或容量不符直接返回错误，避免在错误拓扑上恢复；
  - **已结束作业**（Succeeded/Failed）原样保留，不参与调度，重新提交时返回 `ErrDuplicateJob`，
    **不会复活**；
  - **未完成作业**继续调度；崩溃前处于 `Running` 的任务因执行行已不存在，
    统一复位为 `Pending`、清空 NodeID、`Attempt+1`，随下一次 tick 重新 gang 放置；
    因此“排空迁到一半时崩溃”也安全：节点仍是 `Draining`，任务复位后只会被放到
    健康节点，随后排空继续推进直至退役，不会把任务弄丢或重复占用；
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
  **节点发起排空、任务在线迁移（from→to）、排空进展（migrated/remaining）、退役扣容**、
  作业终态。测试通过 `t.Log` 桥接，`go test -v` 可直接看到这些依据。
- `Scheduler.Nodes()` 返回节点容量/占用/状态快照；`Scheduler.DrainProgress()`
  返回各维护中/已退役节点的 `State` 与 `Remaining`。

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

# 端到端演示：提交→三类拒绝→故障重调度→重启恢复→计划内维护/排空/容量收缩
go run ./cmd/gangscheduler
```

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
| 排空中任务在线迁移、attempt 不变、迁空退役、进展可见 | `TestDrainLiveMigratesTasksAndRetires` |
| 无处可迁时任务原地续跑、释放后再迁出退役 | `TestDrainWaitsForCapacity` |
| 空闲节点排空立即退役 | `TestDrainEmptyNodeRetiresImmediately` |
| 容量收缩后新提交拒绝、已排队作业保持 Pending 不超发 | `TestCapacityShrunkSubmitAndQueue` `TestShrunkCapacityQueuedVsNewSubmit` |
| 多节点先后排空、部分排空中账目守恒 | `TestDrainMultipleNodes` |
| 排空中节点不接新任务 | `TestDrainDoesNotPlaceNewTasks` |
| 排空跨重启：节点仍 Draining、任务不丢不重占、续推退役 | `TestDrainPersistsAcrossRestart` |
| 退役跨重启保持、容量持续扣除 | `TestDrainRetiredPersistsAcrossRestart` |
| 恢复时节点缺失/容量不符报错 | `TestRecoverRejectsMismatchedTopology` |
| 排空与 tick/提交并发交错（-race） | `TestDrainConcurrentWithTicksAndSubmit` |
| 排空中又故障走统一回收，账目守恒 | `TestDrainThenNodeFailure` |
| 排空/迁移/退役/恢复重放的节点账本原语 | `pkg/cluster`：`TestCordonDrainMigrateRetire` 等 |
| 在线迁移保留剩余节拍 | `pkg/execution`：`TestMigrateKeepsRemainingTicks` |
| 存储深拷贝隔离、持久化往返 | `pkg/state` 全部测试 |

## 8. 已知边界与取舍

- 容量可行性是基于**提交时刻的静态集群拓扑**判断；若之后故障或退役导致集群容量永久缩减，
  一个原本可行的作业可能长期停留 `Pending`（不会错误地成功或超发）。新提交则按收缩后
  拓扑立即拒绝；两者语义区别见 §4A.3。
- 模拟执行不区分抢占：已运行任务不会被高优先级作业抢占，只在节点故障后重调度、
  或节点计划内排空时**整任务在线迁移**（迁移不改变剩余节拍、不增加 Attempt，
  也不做检查点/续算——模拟语义下任务在新节点继续跑剩余节拍）。
- 排空是**逐任务、每 tick 推进**的协作式迁移：若集群长期没有任何健康余量可承接，
  排空中节点会一直处于 `Draining`（任务在其上继续正常运行），不会强行杀任务，
  也不会自动回滚为健康；需要等容量释放后自然迁完，或由故障路径接管。
- 节点拓扑（注册/容量）仍由进程启动时 `AddNode` 提供；快照只持久化节点**生命周期状态**，
  恢复时要求相同 ID 与容量，不支持重启后增删节点（缺失/不符会报错而非静默忽略）。
- 退役为当前进程内终态，不提供“维护完成重新上线”的反操作；如需复用，以新进程重新注册为准。
- 快照为全量 JSON、每次变更同步落盘，面向“可解释、易验证”的教学/演示用途，非高吞吐生产实现。
