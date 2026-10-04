// Package snapshot 定义运行时状态快照的线格式（wire format）。
//
// 快照是运行时某一时刻的一致性冻结视图：由 runtime 包在自身锁保护下
// 填充并序列化为 JSON 字节，恢复时反序列化并严格校验格式与版本。
// 本包只负责格式本身（编解码与兼容范围判定），不依赖 runtime，
// 避免与运行时形成包循环；快照到运行时对象的装配由 runtime 包完成。
//
// 格式：
//
//	{
//	  "magic":   "sensor-telemetry-snapshot",
//	  "format":  1,                       // 线格式版本，不兼容则拒绝
//	  "takenAt": "2026-10-04T...Z",       // 冻结时刻（诊断信息）
//	  "window":  4,
//	  "calibs":  [ {config...}, ... ],    // 当前及历史校准配置（会话延续用）
//	  "currentCalibVersion": 2,
//	  "nextSessionID": 7,
//	  "sessions": [ {"id":3,"calibVersion":1}, ... ],
//	  "devices":  [ ... ]
//	}
//
// 兼容范围：仅接受 FormatMagic 与 FormatVersion 完全匹配的数据。
// 未来格式演进若引入破坏性变更，必须提升 FormatVersion；旧版运行时
// 读到更高版本会直接返回 ErrUnsupportedVersion，绝不半装配后运行。
package snapshot

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// FormatMagic 用于识别数据确为本运行时的快照（而非任意 JSON）。
const FormatMagic = "sensor-telemetry-snapshot"

// FormatVersion 是当前快照线格式版本。
// 版本策略：仅接受相同主格式版本；更高/更低版本一律拒绝，
// 由调用方负责保证不影响当前运行实例。
const FormatVersion = 1

// 格式拒绝原因，均可通过 errors.Is 判定。
var (
	// ErrBadMagic 数据不是本运行时快照。
	ErrBadMagic = errSnapshot("snapshot: bad magic")
	// ErrUnsupportedVersion 线格式版本不受支持。
	ErrUnsupportedVersion = errSnapshot("snapshot: unsupported format version")
	// ErrMalformed 数据结构不完整或字段非法。
	ErrMalformed = errSnapshot("snapshot: malformed snapshot")
)

// errSnapshot 是快照格式错误类型，支持 errors.Is 比较。
type errSnapshot string

func (e errSnapshot) Error() string { return string(e) }

// Measurement 是 telemetry.Measurement 的快照表示。
// 时间戳采用 RFC3339 纳秒精度文本，保证 JSON 自描述、可人工检查。
type Measurement struct {
	DeviceID  string    `json:"deviceId"`
	SensorID  string    `json:"sensorId"`
	Seq       uint64    `json:"seq"`
	Timestamp time.Time `json:"timestamp"`
	Value     float64   `json:"value"`
	Unit      string    `json:"unit"`
	Source    string    `json:"source"`
}

// CalibConfig 是 calibration.Config 的快照表示。
type CalibConfig struct {
	Version uint64             `json:"version"`
	Sensors []CalibSensorParam `json:"sensors"`
	MinSeq  []DeviceSeq        `json:"minSeq,omitempty"`
}

// CalibSensorParam 是单传感器校准参数。
type CalibSensorParam struct {
	SensorID string  `json:"sensorId"`
	Scale    float64 `json:"scale"`
	Offset   float64 `json:"offset"`
}

// DeviceSeq 是 (设备, 序号) 对。
type DeviceSeq struct {
	DeviceID string `json:"deviceId"`
	Seq      uint64 `json:"seq"`
}

// Session 记录一个进行中会话的标识与绑定的校准版本。
type Session struct {
	ID           uint64 `json:"id"`
	CalibVersion uint64 `json:"calibVersion"`
}

// HealthState 是设备健康状态机的快照表示。
// 状态以整数编码（0=Healthy,1=Suspect,2=Isolated,3=Recovering），
// 与 health.State 的取值严格一致，由装配层校验范围。
type HealthState struct {
	State      int         `json:"state"`
	Failures   []time.Time `json:"failures,omitempty"` // 统计窗口内失败时刻
	IsolatedAt time.Time   `json:"isolatedAt,omitempty"`
	SuccessRun int         `json:"successRun"`
}

// SensorState 是单个 (设备, 传感器) 流的快照表示。
type SensorState struct {
	SensorID      string        `json:"sensorId"`
	Seen          []uint64      `json:"seen"` // 已接纳序号（去重集合）
	MaxSeq        uint64        `json:"maxSeq"`
	HasSeq        bool          `json:"hasSeq"`
	Pending       []Measurement `json:"pending"`       // 尚未吐出的在途样本
	RebootPending bool          `json:"rebootPending"` // 下一条输出标记重启
	EmittedMaxSeq uint64        `json:"emittedMaxSeq"` // 已纳入输出的最大序号
}

// DeviceState 是单台设备的快照表示。
type DeviceState struct {
	DeviceID      string        `json:"deviceId"`
	Health        HealthState   `json:"health"`
	Sensors       []SensorState `json:"sensors"`
	EmittedMaxSeq uint64        `json:"emittedMaxSeq"` // 已纳入输出的最大序号（设备级，校准兼容校验用）
}

// Policy 记录导出时生效的健康策略参数。
// 恢复时必须提供同一策略，否则故障隔离/恢复语义会改变，予以拒绝。
type Policy struct {
	WindowNS         int64 `json:"windowNs"`         // 失败统计窗口（纳秒）
	IsolateThreshold int   `json:"isolateThreshold"` // 窗口内隔离阈值
	QuarantineNS     int64 `json:"quarantineNs"`     // 隔离静默期（纳秒）
	RecoverAfter     int   `json:"recoverAfter"`     // 恢复所需连续成功次数
}

// Snapshot 是运行时状态冻结视图的完整线格式。
type Snapshot struct {
	Magic               string        `json:"magic"`
	Format              int           `json:"format"`
	TakenAt             time.Time     `json:"takenAt"`
	Window              int           `json:"window"`
	Policy              Policy        `json:"policy"`
	Calibs              []CalibConfig `json:"calibs"`
	CurrentCalibVersion uint64        `json:"currentCalibVersion"`
	NextSessionID       uint64        `json:"nextSessionID"`
	Sessions            []Session     `json:"sessions"`
	Devices             []DeviceState `json:"devices"`
}

// Encode 将快照序列化为带缩进的 JSON 字节，便于本地检查与归档。
// Encode 只做序列化；传入的 Snapshot 假定已由运行时按锁内一致视图填充。
func Encode(s *Snapshot) ([]byte, error) {
	if s == nil {
		return nil, errors.New("snapshot: nil snapshot")
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	if err := enc.Encode(s); err != nil {
		return nil, errors.Join(ErrMalformed, err)
	}
	return buf.Bytes(), nil
}

// Decode 反序列化快照并做格式层校验。
//
// 校验范围（任一不满足即返回对应错误，且错误可由 errors.Is 判定）：
//   - 数据必须是合法 JSON，且 magic 为 FormatMagic；
//   - format 必须恰好等于 FormatVersion（更高/更低均拒绝）；
//   - 基础字段范围：window>=1、健康状态编码 0..3、
//     设备/传感器/校准配置无重复键、待吐出样本序号无重复。
//
// 跨结构的语义引用完整性（如会话引用的校准版本是否存在）由
// runtime.Restore 装配时校验；本函数不持有运行时知识。
func Decode(data []byte) (*Snapshot, error) {
	var s Snapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, errors.Join(ErrMalformed, err)
	}
	if s.Magic != FormatMagic {
		return nil, ErrBadMagic
	}
	if s.Format != FormatVersion {
		return nil, fmt.Errorf("%w: got %d, want %d", ErrUnsupportedVersion, s.Format, FormatVersion)
	}
	if err := s.validate(); err != nil {
		return nil, err
	}
	return &s, nil
}

// validate 执行与运行时无关的结构完整性校验。
func (s *Snapshot) validate() error {
	if s.Window < 1 {
		return malformed("window must be >= 1")
	}
	if s.Policy.WindowNS < 0 || s.Policy.QuarantineNS < 0 ||
		s.Policy.IsolateThreshold < 1 || s.Policy.RecoverAfter < 1 {
		return malformed("health policy parameters out of range")
	}
	if len(s.Calibs) == 0 {
		return malformed("calibs must not be empty")
	}
	calibVersions := make(map[uint64]struct{}, len(s.Calibs))
	for i := range s.Calibs {
		c := &s.Calibs[i]
		if c.Version == 0 {
			return malformed("calib version must be >= 1")
		}
		if _, dup := calibVersions[c.Version]; dup {
			return malformedf("duplicate calib version %d", c.Version)
		}
		calibVersions[c.Version] = struct{}{}
		params := make(map[string]struct{}, len(c.Sensors))
		for _, p := range c.Sensors {
			if p.SensorID == "" {
				return malformed("calib sensor with empty id")
			}
			if _, dup := params[p.SensorID]; dup {
				return malformedf("duplicate calib sensor %q", p.SensorID)
			}
			params[p.SensorID] = struct{}{}
		}
		devs := make(map[string]struct{}, len(c.MinSeq))
		for _, d := range c.MinSeq {
			if d.DeviceID == "" {
				return malformed("minSeq with empty device id")
			}
			if _, dup := devs[d.DeviceID]; dup {
				return malformedf("duplicate minSeq device %q", d.DeviceID)
			}
			devs[d.DeviceID] = struct{}{}
		}
	}
	if _, ok := calibVersions[s.CurrentCalibVersion]; !ok {
		return malformedf("current calib version %d not found in calibs", s.CurrentCalibVersion)
	}
	sessionIDs := make(map[uint64]struct{}, len(s.Sessions))
	for _, sess := range s.Sessions {
		if sess.ID == 0 {
			return malformed("session id must be >= 1")
		}
		if _, dup := sessionIDs[sess.ID]; dup {
			return malformedf("duplicate session id %d", sess.ID)
		}
		sessionIDs[sess.ID] = struct{}{}
		if _, ok := calibVersions[sess.CalibVersion]; !ok {
			return malformedf("session %d binds unknown calib version %d", sess.ID, sess.CalibVersion)
		}
	}
	if s.NextSessionID == 0 {
		return malformed("nextSessionId must be >= 1")
	}
	devIDs := make(map[string]struct{}, len(s.Devices))
	for i := range s.Devices {
		d := &s.Devices[i]
		if d.DeviceID == "" {
			return malformed("device with empty id")
		}
		if _, dup := devIDs[d.DeviceID]; dup {
			return malformedf("duplicate device %q", d.DeviceID)
		}
		devIDs[d.DeviceID] = struct{}{}
		if d.Health.State < 0 || d.Health.State > 3 {
			return malformedf("device %q has invalid health state %d", d.DeviceID, d.Health.State)
		}
		if d.Health.SuccessRun < 0 {
			return malformedf("device %q has negative successRun", d.DeviceID)
		}
		sensorIDs := make(map[string]struct{}, len(d.Sensors))
		for j := range d.Sensors {
			ss := &d.Sensors[j]
			if ss.SensorID == "" {
				return malformed("sensor with empty id")
			}
			if _, dup := sensorIDs[ss.SensorID]; dup {
				return malformedf("device %q duplicate sensor %q", d.DeviceID, ss.SensorID)
			}
			sensorIDs[ss.SensorID] = struct{}{}
			seen := make(map[uint64]struct{}, len(ss.Seen))
			for _, seq := range ss.Seen {
				if _, dup := seen[seq]; dup {
					return malformedf("device %q sensor %q duplicate seen seq %d", d.DeviceID, ss.SensorID, seq)
				}
				seen[seq] = struct{}{}
			}
			pending := make(map[uint64]struct{}, len(ss.Pending))
			for _, m := range ss.Pending {
				if _, dup := pending[m.Seq]; dup {
					return malformedf("device %q sensor %q duplicate pending seq %d", d.DeviceID, ss.SensorID, m.Seq)
				}
				pending[m.Seq] = struct{}{}
				if m.DeviceID != d.DeviceID || m.SensorID != ss.SensorID {
					return malformedf("pending measurement key mismatch at device %q sensor %q", d.DeviceID, ss.SensorID)
				}
			}
		}
	}
	return nil
}

func malformed(msg string) error {
	return fmt.Errorf("%w: %s", ErrMalformed, msg)
}

func malformedf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrMalformed, fmt.Sprintf(format, args...))
}
