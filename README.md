# sensor-telemetry-runtime-go

多传感器遥测运行时：处理持续到达、可能乱序或重复的设备测量数据，
为上层提供可复现、可审计的设备状态与归一化测量结果。
全部验证在本地完成，不依赖真实硬件或外部服务。

## 架构

| 包 | 职责 |
|---|---|
| `telemetry` | 测量数据统一表示、输入校验（可区分拒绝原因）、归一化结果类型 |
| `calibration` | 版本化校准参数；原子快照，支持并发读与整体替换 |
| `health` | 确定性设备健康状态机与故障隔离 |
| `runtime` | 接入管线：校验 → 健康门控 → 去重/重启检测 → 乱序重排 → 校准归一化；运行状态快照导出与恢复 |
| `snapshot` | 快照线格式（JSON）：编码/解码与格式兼容范围判定，不依赖运行时 |

## 数据规则

- 每条测量必须携带：`DeviceID`、`SensorID`、单调序号 `Seq`、时间戳、
  数值、单位、来源。
- 以下输入一律被拒绝，且失败原因可区分（`errors.Is` 可判定）：
  - 缺设备/传感器标识：`ErrMissingDeviceID` / `ErrMissingSensorID`
  - 单位缺失：`ErrMissingUnit`；单位未注册：`ErrUnknownUnit`
  - 来源不明确：`ErrUnknownSource`
  - 时间戳为零值：`ErrInvalidTime`
  - 数值超出该单位注册范围：`ErrOutOfRange`
- **去重**：以 `(DeviceID, SensorID, Seq)` 为键，重复上报返回
  `ErrDuplicate`，不会覆盖已接受数据。
- **乱序**：每个 (设备, 传感器) 流维护大小为 `window` 的重排缓冲，
  缓冲达到窗口大小时按序号吐出最旧样本；`Flush` 在会话结束/重连后
  吐出剩余样本。乱序样本绝不丢失。
- **设备重启**：序号回退超出重排窗口（`seq + window < maxSeq`）判定为
  重启：先吐出旧纪元缓冲样本，再重置序号空间，新纪元首条输出标记
  `DeviceRebooted=true`；窗口内的序号回退视为正常乱序。

## 校准兼容规则

- 校准参数为线性模型 `value = raw*Scale + Offset`，按传感器配置，
  整体打包为不可变的 `Config{Version, Sensors, MinSeq}`。
- `Store.Update` 启用新配置的条件（任一不满足即拒绝）：
  1. 版本号严格递增；
  2. 对每台设备，新配置的 `MinSeq` 不得高于已纳入输出的最大序号 + 1
     （`Runtime.AcceptedMaxSeq()`），否则返回 `ErrIncompatible`，
     旧配置继续生效。
- **会话隔离**：`Runtime.NewSession()` 绑定创建时刻的配置快照。
  已开始的会话永远按其快照判定，结论可复现；配置切换只影响之后
  新建的会话。每条输出 `Reading` 携带 `CalibVersion`，可审计。

## 故障状态转换

```
Healthy    --窗口内失败数 >= IsolateThreshold-->  Isolated
Healthy    --单次失败/断连----------------------->  Suspect
Suspect    --连续 RecoverAfter 次成功------------>  Healthy
Suspect    --窗口内失败数 >= IsolateThreshold-->  Isolated
Isolated   --静默 Quarantine 时长---------------->  Recovering
Recovering --连续 RecoverAfter 次成功------------>  Healthy
Recovering --任何一次失败------------------------>  Isolated（重新计时）
Isolated   --期间再次失败------------------------>  延长静默期
```

- 短暂尖峰（窗口内失败数未达阈值）或单次断连只进入 `Suspect`，
  不会被误判为永久故障。
- 隔离期间该设备样本被丢弃并返回 `ErrDeviceIsolated`；
  其他设备的处理与整体吞吐不受影响（状态按设备独立跟踪）。
- 恢复是确定性的：静默期满进入观察期，连续 `RecoverAfter`
  次有效样本后重新纳入输出。

## 并发语义

- `calibration.Store` 使用 `atomic.Pointer`：读者无锁获取完整快照，
  写者校验后整体替换；运行中的采样**不会读到半更新配置**。
- `Runtime` 以互斥锁串行化设备状态变更；不同设备的接入可安全并发，
  去重、重排与健康判定结果与串行执行一致。
- 时钟可注入（`NewWithClock` / `health.NewTracker` 的 `now` 参数），
  使故障恢复等时间相关行为在测试中确定可复现。

## 快照与恢复

面向长时间无人值守采集：进程重启或滚动升级后，新实例从快照恢复并
继续接入同一批设备/传感器，不必从头再收。全部在本地完成，不依赖
外部存储或真实硬件（上层自行决定字节存到哪里）。

### 快照覆盖的运行状态

`Runtime.ExportSnapshot()` / `ExportSnapshotBytes()` 在**运行时同一把
互斥锁**内冻结状态（与 `Ingest`/`Flush`/会话注册用的是同一把锁）：

| 状态 | 快照字段 | 恢复后行为 |
|---|---|---|
| 重排窗口 `window` | `window` | 乱序缓冲容量延续 |
| 健康策略 | `policy` | 必须与恢复时传入的策略一致，否则拒绝 |
| 每流去重集合 | `sensor.seen` | 已接纳序号不复活为新样本、不重复输出 |
| 每流最大序号/有序性标记 | `sensor.maxSeq`/`hasSeq` | 乱序与重启判定基准延续 |
| 已接纳但未吐出的在途样本 | `sensor.pending` | 继续按序号有序吐出，绝不丢失 |
| 设备重启边界 | `sensor.rebootPending` | 恢复后首条输出仍标记 `DeviceRebooted` |
| 健康状态机全部进度 | `device.health` | 状态、窗口内失败时刻、隔离起点、连续成功计数延续 |
| 已输出水位（设备级） | `device.emittedMaxSeq` | 校准 `MinSeq` 兼容校验基准延续 |
| 会话 ID 与绑定校准版本 | `sessions` | 旧会话恢复后仍按原版本归一化 |
| 当前及历史校准配置 | `calibs`+`currentCalibVersion` | Store 当前版本与旧会话快照一并复原 |
| 会话发号器 | `nextSessionId` | 新会话 ID 单调，绝不复用旧 ID |

**“已输出”与“在途”如何无歧义区分**：`seen` 是已接纳序号集合；
`pending` 是其中尚未吐出的子集（恢复时强制校验 `pending ⊆ seen`）；
`emittedMaxSeq` 是设备级已纳入输出水位。三者在锁内同一时刻采样，
快照不会出现“一部分旧状态、一部分新状态”的混合视图。导出期间
并发接入只会在锁外排队，**不会被拒绝、丢失或错序**。

### 线格式与兼容范围

- JSON 信封：`magic="sensor-telemetry-snapshot"`，`format=1`，
  时间戳为 RFC3339 纳秒文本，键名驼峰、集合按 ID/序号排序（字节确定）。
- **仅接受 magic 与 format 完全匹配**的数据：非 JSON、magic 不符、
  格式版本更高或更低，一律拒绝，绝不半装配后运行。
- 结构完整性（重复键、越界状态、待吐出样本键不匹配等）在解码层拒绝；
  跨结构引用完整性（会话引用的校准版本存在、会话 ID 不侵占发号器、
  `pending ⊆ seen`、在途样本通过输入校验、健康策略一致）在装配层拒绝。
- 错误均可 `errors.Is` 判定：`snapshot.ErrBadMagic`、
  `snapshot.ErrUnsupportedVersion`、`snapshot.ErrMalformed`、
  `runtime.ErrRestoreIncompatible`。
- 未来破坏性格式变更必须提升 `format`；旧运行时读到新版本会明确拒绝。

### 恢复失败的语义

- `Restore`/`RestoreBytes` **只读取快照并返回新实例**，不修改接收者
  运行时及其已开始的会话；任何一步失败都只返回错误，当前实例与
  进行中的接入、健康状态、校准绑定完全不受影响（上层可保留旧字节
  排查后重试）。
- 健康时间戳是绝对墙钟时刻：真实重启后若隔离静默期已自然度过，
  恢复实例会如实推进到 `Recovering`，与“一直没停”一致。
- 健康策略必须与导出时一致（阈值/窗口/静默期/恢复次数），策略不同
  会改变故障判定语义，按不兼容拒绝。

### 等价性保证

在相同的接入与收尾（`Flush`）调用序列下，“中途冻结→在新实例恢复→
继续接入”与“一直连续运行”逐字段一致：最终样本集合、每流内顺序、
归一化值、`CalibVersion`、`DeviceRebooted` 标记均无差异；恢复或
缓冲补发只影响对应设备，不阻塞其他设备，不重复输出、不静默漏掉。

### 本地验证

```sh
go build ./...
go test ./...          # 单元与关键路径测试
go test ./... -race    # 并发一致性（数据竞争检测）
go test ./runtime/ -v  # 查看输入、判定依据与归一化结果日志
```

测试覆盖：输入校验拒绝原因、乱序重排、重复上报、设备重启、
校准版本切换与兼容性拒绝、瞬时故障与持续故障的隔离/恢复、
隔离不阻塞其他设备、并发接入与并发配置更新下的数据一致性；
以及**快照往返与恢复**：并发接入高峰期间反复导出（每张快照
自洽、可解码、恢复后无丢失）、恢复后继续处理乱序与重复上报、
导出时仍有缓冲未吐出、设备重启边界延续、隔离与恢复进度延续、
每会话校准版本延续、格式/版本/策略不兼容的明确拒绝、拒绝失败
不影响当前实例、空运行时快照与连续多次快照周期。

### 最小用法

```go
// 冻结（可随时调用，与接入并发安全）
data, _ := rt.ExportSnapshotBytes()
// ... 进程重启 / 滚动升级，data 由上层自行保存 ...

// 恢复为新实例并取回旧会话句柄（按原 Session.ID() 继续接入）
rt2, sessions, err := rt.RestoreBytes(data, health.DefaultPolicy(), nil)
s := sessions[oldSessionID]
// 也可稍后 rt2.OpenSession(oldSessionID)
```

