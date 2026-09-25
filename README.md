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
| `runtime` | 接入管线：校验 → 健康门控 → 去重/重启检测 → 乱序重排 → 校准归一化 |

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

## 本地验证

```sh
go build ./...
go test ./...          # 单元与关键路径测试
go test ./... -race    # 并发一致性（数据竞争检测）
go test ./runtime/ -v  # 查看输入、判定依据与归一化结果日志
```

测试覆盖：输入校验拒绝原因、乱序重排、重复上报、设备重启、
校准版本切换与兼容性拒绝、瞬时故障与持续故障的隔离/恢复、
隔离不阻塞其他设备、并发接入与并发配置更新下的数据一致性。
