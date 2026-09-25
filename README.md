# sensor-telemetry-runtime-go

多传感器遥测运行时：处理持续到达、可能乱序或重复的设备测量数据，
为上层提供可复现、可审计的设备状态与归一化测量结果。
全部验证在本地完成，不依赖真实硬件或外部服务。

## 结构

```
telemetry/
  types.go        统一测量表示（Sample/Reading）、静态校验与可区分拒绝原因
  calibration.go  版本化校准配置，原子发布与兼容性校验
  health.go       设备健康状态机（尖峰吸收、故障隔离与恢复）
  runtime.go      接入流水线：校验 → 去重/乱序 → 健康评估 → 校准归一化
```

## 数据规则

- 每条样本必须携带：设备 ID、指标名、合法单位、有限数值、非零时间戳、
  明确来源（Kind+ID）。缺失或非法即以 `RejectError` 拒绝，
  `Reason` 可区分：`missing_unit` / `unknown_unit` / `out_of_range` /
  `unknown_source` / `missing_device` / `missing_metric` /
  `invalid_reading` / `device_isolated`。
- **去重**：同一 `(DeviceEpoch, Seq)` 只接受一次，重复上报返回 `Duplicate=true`。
- **乱序**：序号回退但仍属当前纪元的样本被接受并标记 `Stale=true`，数据不丢失；
  `Readings` 始终按 `(DeviceEpoch, Seq)` 排序输出。
- **设备重启**：`DeviceEpoch` 增大即视为重启，序号窗口重置，
  重启后从低序号重新开始的数据正常接受；旧纪元的迟到数据按规则丢弃（`Stale=true`）。

## 校准兼容规则

- 归一化公式：`normalized = raw*Scale + Offset`，结果须落在 `[Min, Max]`，否则 `out_of_range`。
- 配置不可变，通过原子指针整体发布：读者只看到完整的旧版或新版，**不会读到半更新配置**。
- `Apply` 拒绝两种情形（`errors.Is` 可区分）：
  - `ErrCalibrationVersionStale`：版本未递增；
  - `ErrCalibrationIncompatible`：`next.MinRawVersion > dataVersion`，
    即新配置无法解释既有数据，拒绝启用且当前配置不变。
- **会话可复现**：每次成功切换开启新会话（SessionID 递增）。
  每条 `Reading` 记录产生时所用的 `CalibrationVersion` 与 `SessionID`，
  切换前已产生的结论保持旧版本不变，切换后的结论使用新版本，各自可复现。

## 故障状态转换（确定规则）

```
Healthy    --连续异常 >= SuspectAfter------------------> Suspect
Suspect    --连续异常 >= IsolateAfter------------------> Isolated
Suspect    --一次正常----------------------------------> Healthy
Isolated   --隔离满 MinIsolation 后出现一次正常---------> Recovering
Recovering --连续正常 >= RecoverAfter------------------> Healthy（重新纳入输出）
Recovering --一次异常----------------------------------> Isolated（重新计时）
Isolated   --持续异常----------------------------------> 重新计时 MinIsolation
```

- 单次断连/单次异常只推进计数，达不到阈值不改变状态，不会被误判为永久故障。
- **尖峰**：相对上次有效值跳变超过 `SpikeFactor` 倍的样本判定为瞬时尖峰，
  不影响健康计数、不更新基准值。
- 隔离与恢复验证期间设备不纳入输出（`device_isolated`），
  但正常样本仍被记录用于恢复判定；恢复完成（回到 Healthy）后按规则重新纳入输出。
- 隔离是设备级的：一台设备被隔离不阻塞其他设备的接入与输出。

## 并发语义

- 不同设备的 `Ingest` 使用设备级互斥锁，互不阻塞；
  同一设备的接入串行化，保证去重与状态机判定一致。
- 校准读取无锁（`atomic.Pointer` 快照）；`Apply` 先完整校验再原子发布。
- `HealthTracker` 内部自带锁，可独立并发使用。
- 健康评估使用样本自带时间戳而非真实时钟，判定完全可复现。

## 本地验证

```sh
go build ./...
go test ./... -v        # 日志打印输入、判定依据与归一化结果
go test ./... -race     # 并发访问下数据与状态一致性
```

测试覆盖：乱序、重复、设备重启（纪元切换）、校准版本化切换与兼容性拒绝、
瞬时尖峰、单次断连、故障隔离与确定恢复、隔离期间其他设备吞吐、
多设备并发接入与并发配置切换下的一致性（`-race` 验证）。
