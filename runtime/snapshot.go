// snapshot.go 实现运行状态快照与恢复：
// 运行中可将当前状态冻结为一份一致快照交给上层保存，
// 进程重启或滚动升级后从快照恢复新实例并继续接入。
//
// 一致性保证：导出与接入共用 Runtime 的同一把互斥锁，
// 导出期间并发接入只会被短暂串行化，不会被拒绝、丢失或错序；
// 快照在锁内一次性深拷贝，不会出现新旧状态混合的视图。
//
// 恢复语义：恢复出一个全新的 Runtime 与绑定了快照中校准规则的
// Session；已接纳序号（去重）、未输出的乱序缓冲、设备重启标记、
// 健康状态与恢复进度、会话校准版本全部延续。格式或版本不兼容
// 时明确返回 ErrSnapshotIncompatible，且不影响任何现有实例。
package runtime

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/xingjianluo/sensor-telemetry-runtime-go/calibration"
	"github.com/xingjianluo/sensor-telemetry-runtime-go/health"
	"github.com/xingjianluo/sensor-telemetry-runtime-go/telemetry"
)

// SnapshotFormatVersion 是当前快照格式版本。
// 格式不兼容（版本不符或数据校验失败）的快照一律被拒绝。
const SnapshotFormatVersion = 1

// ErrSnapshotIncompatible 表示快照格式/版本不兼容或内容校验失败。
var ErrSnapshotIncompatible = errors.New("runtime: incompatible snapshot")

// Snapshot 是一份一致的运行状态快照。
// 已纳入输出的数据（Seen / AcceptedMaxSeq）与仍在等待处理的
// 数据（Pending）被明确区分，恢复后不会重复输出也不会漏掉。
type Snapshot struct {
	FormatVersion  int                       `json:"formatVersion"`
	Window         int                       `json:"window"`         // 重排窗口，恢复时原样沿用
	Calib          *calibration.Config       `json:"calib"`          // 会话绑定的校准快照
	AcceptedMaxSeq map[string]uint64         `json:"acceptedMaxSeq"` // 每设备已纳入输出的最大序号
	Devices        map[string]DeviceSnapshot `json:"devices"`
}

// DeviceSnapshot 是单台设备的快照。
type DeviceSnapshot struct {
	Health  health.TrackerState       `json:"health"`  // 健康状态与恢复进度
	Sensors map[string]SensorSnapshot `json:"sensors"` // 各传感器流状态
}

// SensorSnapshot 是单个 (设备, 传感器) 流的快照。
type SensorSnapshot struct {
	Seen          []uint64                `json:"seen"` // 已接受序号（升序，去重判定依据）
	MaxSeq        uint64                  `json:"maxSeq"`
	HasSeq        bool                    `json:"hasSeq"`
	RebootPending bool                    `json:"rebootPending"` // 待输出的重启标记
	Pending       []telemetry.Measurement `json:"pending"`       // 已接纳但尚未输出的缓冲样本
}

// Snapshot 冻结当前运行状态为一份一致快照。
// 快照绑定本会话的校准配置；返回值为深拷贝，
// 之后的接入与配置切换都不会改变已导出的快照。
func (s *Session) Snapshot() *Snapshot {
	s.rt.mu.Lock()
	defer s.rt.mu.Unlock()

	snap := &Snapshot{
		FormatVersion:  SnapshotFormatVersion,
		Window:         s.rt.window,
		Calib:          copyConfig(s.cfg),
		AcceptedMaxSeq: make(map[string]uint64, len(s.rt.acceptedMaxSeq)),
		Devices:        make(map[string]DeviceSnapshot, len(s.rt.devices)),
	}
	for dev, maxSeq := range s.rt.acceptedMaxSeq {
		snap.AcceptedMaxSeq[dev] = maxSeq
	}
	for devID, dev := range s.rt.devices {
		ds := DeviceSnapshot{
			Health:  dev.tracker.Export(),
			Sensors: make(map[string]SensorSnapshot, len(dev.sensors)),
		}
		for sensorID, ss := range dev.sensors {
			ds.Sensors[sensorID] = exportSensor(ss)
		}
		snap.Devices[devID] = ds
	}
	return snap
}

// Restore 从快照恢复一个新的 Runtime 与绑定了快照中校准规则的 Session。
//
// store 用于恢复之后新建的会话；为 nil 时以快照中的校准配置新建仓库。
// 恢复出的 Session 始终绑定快照内的校准配置，与 store 当前版本无关。
// 格式或版本不兼容时返回 ErrSnapshotIncompatible；
// 恢复失败只返回错误，不影响任何现有实例与已开始的会话。
func Restore(snap *Snapshot, store *calibration.Store, policy health.Policy, now func() time.Time) (*Runtime, *Session, error) {
	if err := validateSnapshot(snap); err != nil {
		return nil, nil, err
	}
	if store == nil {
		var err error
		if store, err = calibration.NewStore(copyConfig(snap.Calib)); err != nil {
			return nil, nil, fmt.Errorf("%w: %v", ErrSnapshotIncompatible, err)
		}
	}
	rt := &Runtime{
		calib:          store,
		policy:         policy,
		window:         snap.Window,
		now:            now,
		devices:        make(map[string]*deviceState, len(snap.Devices)),
		acceptedMaxSeq: make(map[string]uint64, len(snap.AcceptedMaxSeq)),
	}
	for dev, maxSeq := range snap.AcceptedMaxSeq {
		rt.acceptedMaxSeq[dev] = maxSeq
	}
	for devID, ds := range snap.Devices {
		dev := &deviceState{
			tracker: health.NewTrackerWithState(policy, now, ds.Health),
			sensors: make(map[string]*sensorState, len(ds.Sensors)),
		}
		for sensorID, ss := range ds.Sensors {
			dev.sensors[sensorID] = importSensor(ss)
		}
		rt.devices[devID] = dev
	}
	return rt, &Session{rt: rt, cfg: copyConfig(snap.Calib)}, nil
}

// Encode 将快照序列化为可持久保存的字节串（JSON）。
func (snap *Snapshot) Encode() ([]byte, error) {
	if snap == nil {
		return nil, errors.New("runtime: nil snapshot")
	}
	return json.Marshal(snap)
}

// DecodeSnapshot 反序列化快照；格式版本不符时返回 ErrSnapshotIncompatible。
func DecodeSnapshot(data []byte) (*Snapshot, error) {
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrSnapshotIncompatible, err)
	}
	if err := validateSnapshot(&snap); err != nil {
		return nil, err
	}
	return &snap, nil
}

// validateSnapshot 校验快照格式版本与内部一致性。
func validateSnapshot(snap *Snapshot) error {
	if snap == nil {
		return fmt.Errorf("%w: nil snapshot", ErrSnapshotIncompatible)
	}
	if snap.FormatVersion != SnapshotFormatVersion {
		return fmt.Errorf("%w: format version %d, want %d",
			ErrSnapshotIncompatible, snap.FormatVersion, SnapshotFormatVersion)
	}
	if snap.Window < 1 {
		return fmt.Errorf("%w: invalid window %d", ErrSnapshotIncompatible, snap.Window)
	}
	if snap.Calib == nil || snap.Calib.Version == 0 {
		return fmt.Errorf("%w: missing calibration config", ErrSnapshotIncompatible)
	}
	for devID, ds := range snap.Devices {
		for sensorID, ss := range ds.Sensors {
			if err := validateSensor(ss); err != nil {
				return fmt.Errorf("%w: device %q sensor %q: %v",
					ErrSnapshotIncompatible, devID, sensorID, err)
			}
		}
	}
	return nil
}

// validateSensor 校验单个流：序号不得重复，缓冲样本必须已被接纳。
func validateSensor(ss SensorSnapshot) error {
	seen := make(map[uint64]struct{}, len(ss.Seen))
	for _, seq := range ss.Seen {
		if _, dup := seen[seq]; dup {
			return fmt.Errorf("duplicate seen seq %d", seq)
		}
		seen[seq] = struct{}{}
	}
	pending := make(map[uint64]struct{}, len(ss.Pending))
	for _, m := range ss.Pending {
		if _, ok := seen[m.Seq]; !ok {
			return fmt.Errorf("pending seq %d not in seen set", m.Seq)
		}
		if _, dup := pending[m.Seq]; dup {
			return fmt.Errorf("duplicate pending seq %d", m.Seq)
		}
		pending[m.Seq] = struct{}{}
	}
	return nil
}

// exportSensor 深拷贝单个流状态（Seen 升序、Pending 按序号排序，保证编码确定）。
func exportSensor(ss *sensorState) SensorSnapshot {
	out := SensorSnapshot{
		Seen:          make([]uint64, 0, len(ss.seen)),
		MaxSeq:        ss.maxSeq,
		HasSeq:        ss.hasSeq,
		RebootPending: ss.rebootPending,
		Pending:       make([]telemetry.Measurement, 0, len(ss.pending)),
	}
	for seq := range ss.seen {
		out.Seen = append(out.Seen, seq)
	}
	sort.Slice(out.Seen, func(i, j int) bool { return out.Seen[i] < out.Seen[j] })
	for _, seq := range sortedSeqs(ss.pending) {
		out.Pending = append(out.Pending, ss.pending[seq])
	}
	return out
}

// importSensor 从快照重建单个流状态。调用前须已通过 validateSensor。
func importSensor(ss SensorSnapshot) *sensorState {
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
		st.pending[m.Seq] = m
	}
	return st
}

// copyConfig 深拷贝校准配置，保证快照与恢复出的会话互不干扰。
func copyConfig(c *calibration.Config) *calibration.Config {
	if c == nil {
		return nil
	}
	out := &calibration.Config{
		Version: c.Version,
		Sensors: make(map[string]calibration.Params, len(c.Sensors)),
		MinSeq:  make(map[string]uint64, len(c.MinSeq)),
	}
	for k, v := range c.Sensors {
		out.Sensors[k] = v
	}
	for k, v := range c.MinSeq {
		out.MinSeq[k] = v
	}
	return out
}
