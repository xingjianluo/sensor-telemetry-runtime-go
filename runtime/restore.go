package runtime

import (
	"fmt"

	"github.com/xingjianluo/sensor-telemetry-runtime-go/calibration"
	"github.com/xingjianluo/sensor-telemetry-runtime-go/health"
	"github.com/xingjianluo/sensor-telemetry-runtime-go/snapshot"
	"github.com/xingjianluo/sensor-telemetry-runtime-go/telemetry"
	"time"
)

// 恢复阶段的可区分错误，均可通过 errors.Is 判定。
var (
	// ErrRestoreIncompatible 快照与恢复条件不兼容（字段引用缺失/冲突）。
	ErrRestoreIncompatible = fmt.Errorf("runtime: snapshot incompatible for restore")
)

// Restore 从快照装配出一个全新的运行实例与会话句柄，使进程重启或
// 滚动升级后能接着处理同一批设备与传感器，而不必从头再收一遍。
//
// 语义保证：
//
//   - Restore 只读取快照，绝不修改接收者 r（当前实例）与其已开始的
//     会话；装配失败时仅返回错误，当前实例继续照常运行。
//   - 恢复后：已接纳过的序号仍在去重集合中，不会被当成新样本重复
//     输出；尚未吐出的乱序样本保留在重排缓冲中，继续按序号吐出；
//     设备重启边界（rebootPending）、故障隔离与恢复进度、每个会话
//     绑定的校准版本全部延续。
//
// pol 为新实例使用的健康策略，now 为可注入时钟（nil 用真实时钟）；
// 快照中的时间戳为绝对墙钟时刻，因此真实重启后隔离/恢复计时自然延续。
//
// 返回的 sessions 按会话 ID 映射，便于上层按原句柄继续接入；
// 也可随后用 OpenSession(id) 重新打开。
func (r *Runtime) Restore(snap *snapshot.Snapshot, pol health.Policy, now func() time.Time) (*Runtime, map[uint64]*Session, error) {
	if err := validateForRestore(snap, pol); err != nil {
		return nil, nil, err
	}

	// 1) 校准：用快照中的当前版本构造 Store，并重建历史索引。
	current := importConfig(snap, snap.CurrentCalibVersion)
	store, err := calibration.NewStore(current)
	if err != nil {
		return nil, nil, restoreErr(err.Error())
	}
	nr := NewWithClock(store, pol, snap.Window, now)
	for _, c := range snap.Calibs {
		nr.calibHistory[c.Version] = importConfig(snap, c.Version)
	}

	// 2) 会话发号器与注册表：旧会话绑定各自原校准版本。
	nr.nextSessionID = snap.NextSessionID
	sessions := make(map[uint64]*Session, len(snap.Sessions))
	for _, sess := range snap.Sessions {
		cfg := nr.calibHistory[sess.CalibVersion]
		s := &Session{id: sess.ID, rt: nr, cfg: cfg}
		nr.sessions[sess.ID] = s
		sessions[sess.ID] = s
	}

	// 3) 设备：健康进度 + 每流去重/重排/重启边界 + 已输出水位。
	for _, d := range snap.Devices {
		dev := &deviceState{
			tracker: health.RestoreTracker(
				pol, nr.clock(),
				health.State(d.Health.State),
				append([]time.Time(nil), d.Health.Failures...),
				d.Health.IsolatedAt,
				d.Health.SuccessRun,
			),
			sensors: make(map[string]*sensorState, len(d.Sensors)),
		}
		for _, ss := range d.Sensors {
			st, err := importSensor(d.DeviceID, ss)
			if err != nil {
				return nil, nil, err
			}
			dev.sensors[ss.SensorID] = st
		}
		nr.devices[d.DeviceID] = dev
		nr.acceptedMaxSeq[d.DeviceID] = d.EmittedMaxSeq
	}

	return nr, sessions, nil
}

// RestoreBytes 是 Decode + Restore 的便捷形式；
// 格式/版本不兼容或装配失败都返回错误，且不影响接收者实例。
func (r *Runtime) RestoreBytes(data []byte, pol health.Policy, now func() time.Time) (*Runtime, map[uint64]*Session, error) {
	snap, err := snapshot.Decode(data)
	if err != nil {
		return nil, nil, err
	}
	return r.Restore(snap, pol, now)
}

// clock 返回运行时使用的时钟函数（nil 安全）。
func (r *Runtime) clock() func() time.Time { return r.now }

// validateForRestore 校验跨结构的语义引用完整性（格式层已由 Decode 保证）。
func validateForRestore(snap *snapshot.Snapshot, pol health.Policy) error {
	if snap == nil {
		return restoreErr("nil snapshot")
	}
	if int64(pol.Window) != snap.Policy.WindowNS ||
		int64(pol.Quarantine) != snap.Policy.QuarantineNS ||
		pol.IsolateThreshold != snap.Policy.IsolateThreshold ||
		pol.RecoverAfter != snap.Policy.RecoverAfter {
		return restoreErrf("health policy mismatch: snapshot requires window=%s threshold=%d quarantine=%s recoverAfter=%d",
			time.Duration(snap.Policy.WindowNS), snap.Policy.IsolateThreshold,
			time.Duration(snap.Policy.QuarantineNS), snap.Policy.RecoverAfter)
	}
	calibs := make(map[uint64]*snapshot.CalibConfig, len(snap.Calibs))
	for i := range snap.Calibs {
		c := &snap.Calibs[i]
		calibs[c.Version] = c
	}
	if _, ok := calibs[snap.CurrentCalibVersion]; !ok {
		return restoreErrf("current calib version %d missing", snap.CurrentCalibVersion)
	}
	for _, sess := range snap.Sessions {
		if _, ok := calibs[sess.CalibVersion]; !ok {
			return restoreErrf("session %d references missing calib version %d", sess.ID, sess.CalibVersion)
		}
	}
	// 会话 ID 必须小于下一个发号值，保证恢复后新会话不复用旧 ID。
	for _, sess := range snap.Sessions {
		if sess.ID >= snap.NextSessionID {
			return restoreErrf("session id %d >= nextSessionId %d", sess.ID, snap.NextSessionID)
		}
	}
	for _, d := range snap.Devices {
		// 在途样本必须仍属于去重集合，且每条都带合法的测量字段。
		for _, ss := range d.Sensors {
			seen := make(map[uint64]struct{}, len(ss.Seen))
			for _, seq := range ss.Seen {
				seen[seq] = struct{}{}
			}
			for _, m := range ss.Pending {
				if _, ok := seen[m.Seq]; !ok {
					return restoreErrf("device %q sensor %q pending seq %d not in seen",
						d.DeviceID, ss.SensorID, m.Seq)
				}
				if err := importMeasurement(m).Validate(); err != nil {
					return restoreErrf("device %q sensor %q pending seq %d invalid: %v",
						d.DeviceID, ss.SensorID, m.Seq, err)
				}
			}
		}
	}
	return nil
}

// importConfig 从快照重建一份不可变校准配置（深拷贝）。
func importConfig(snap *snapshot.Snapshot, version uint64) *calibration.Config {
	for i := range snap.Calibs {
		c := &snap.Calibs[i]
		if c.Version != version {
			continue
		}
		cfg := &calibration.Config{
			Version: c.Version,
			Sensors: make(map[string]calibration.Params, len(c.Sensors)),
			MinSeq:  make(map[string]uint64, len(c.MinSeq)),
		}
		for _, p := range c.Sensors {
			cfg.Sensors[p.SensorID] = calibration.Params{Scale: p.Scale, Offset: p.Offset}
		}
		for _, d := range c.MinSeq {
			cfg.MinSeq[d.DeviceID] = d.Seq
		}
		return cfg
	}
	return nil
}

// importSensor 从快照重建单流状态。
func importSensor(deviceID string, ss snapshot.SensorState) (*sensorState, error) {
	st := &sensorState{
		seen:          make(map[uint64]struct{}, len(ss.Seen)),
		maxSeq:        ss.MaxSeq,
		hasSeq:        ss.HasSeq,
		pending:       make(map[uint64]telemetry.Measurement, len(ss.Pending)),
		rebootPending: ss.RebootPending,
	}
	for _, seq := range ss.Seen {
		st.seen[seq] = struct{}{}
	}
	for _, m := range ss.Pending {
		st.pending[m.Seq] = importMeasurement(m)
	}
	return st, nil
}

// importMeasurement 将快照测量还原为 telemetry.Measurement。
func importMeasurement(m snapshot.Measurement) telemetry.Measurement {
	return telemetry.Measurement{
		DeviceID:  m.DeviceID,
		SensorID:  m.SensorID,
		Seq:       m.Seq,
		Timestamp: m.Timestamp,
		Value:     m.Value,
		Unit:      telemetry.Unit(m.Unit),
		Source:    telemetry.Source(m.Source),
	}
}

func restoreErr(msg string) error {
	return fmt.Errorf("%w: %s", ErrRestoreIncompatible, msg)
}

func restoreErrf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrRestoreIncompatible, fmt.Sprintf(format, args...))
}
