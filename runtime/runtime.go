// Package runtime 是多传感器遥测运行时：负责测量接入、去重、乱序重排、
// 设备重启检测、健康隔离与校准归一化，并为上层提供可复现的会话视图。
//
// 并发语义：Runtime 内部以互斥锁串行化设备状态变更；
// 校准配置通过 calibration.Store 的原子快照读取，
// 每个 Session 在创建时绑定一份不可变快照，因此会话结论可复现，
// 配置切换只影响之后新建的会话。
package runtime

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/xingjianluo/sensor-telemetry-runtime-go/calibration"
	"github.com/xingjianluo/sensor-telemetry-runtime-go/health"
	"github.com/xingjianluo/sensor-telemetry-runtime-go/telemetry"
)

// 可区分的运行时拒绝/丢弃原因。
var (
	ErrDuplicate      = errors.New("runtime: duplicate measurement")
	ErrDeviceIsolated = errors.New("runtime: device isolated")
)

// sensorState 是单个 (设备, 传感器) 流的运行时状态。
type sensorState struct {
	seen          map[uint64]struct{} // 已接受序号（去重）
	maxSeq        uint64
	hasSeq        bool
	pending       map[uint64]telemetry.Measurement // 乱序重排缓冲
	rebootPending bool                             // 下一条输出需标记设备重启
}

// deviceState 是单台设备的运行时状态。
type deviceState struct {
	tracker *health.Tracker
	sensors map[string]*sensorState
}

// Runtime 是遥测运行时。
type Runtime struct {
	calib  *calibration.Store
	policy health.Policy
	window int              // 重排窗口：缓冲超过该长度时强制吐出最旧样本
	now    func() time.Time // 可注入时钟，nil 表示真实时钟

	mu      sync.Mutex
	devices map[string]*deviceState
	// acceptedMaxSeq 记录每台设备已纳入输出的最大序号，
	// 供校准配置兼容性校验使用。
	acceptedMaxSeq map[string]uint64
	// sessions 为全部进行中会话的注册表（含其绑定的校准快照），
	// 快照/恢复据此让进程重启前已开始的会话延续同一校准版本。
	sessions map[uint64]*Session
	// nextSessionID 是单调递增的会话 ID 发号器（跨恢复延续）。
	nextSessionID uint64
	// calibHistory 留存曾被任何会话绑定过的校准配置（配置不可变，
	// 保存引用即可），恢复后旧会话仍按原版本归一化。
	calibHistory map[uint64]*calibration.Config
}

// New 创建运行时。window 为乱序重排窗口大小（>=1）。
func New(calib *calibration.Store, policy health.Policy, window int) *Runtime {
	return NewWithClock(calib, policy, window, nil)
}

// NewWithClock 与 New 相同，但允许注入时钟（用于确定性测试）。
func NewWithClock(calib *calibration.Store, policy health.Policy, window int, now func() time.Time) *Runtime {
	if window < 1 {
		window = 1
	}
	r := &Runtime{
		calib:          calib,
		policy:         policy,
		window:         window,
		now:            now,
		devices:        make(map[string]*deviceState),
		acceptedMaxSeq: make(map[string]uint64),
		sessions:       make(map[uint64]*Session),
		nextSessionID:  1,
		calibHistory:   make(map[uint64]*calibration.Config),
	}
	if cur := calib.Snapshot(); cur != nil {
		r.calibHistory[cur.Version] = cur
	}
	return r
}

// NewSession 创建绑定当前校准快照的会话。
// 会话内的所有判定均使用该快照，结论可复现；
// 之后对 Store 的更新不影响已存在的会话。
// 会话在注册表中获得一个运行时内唯一、跨恢复延续的 ID。
func (r *Runtime) NewSession() *Session {
	cfg := r.calib.Snapshot()
	r.mu.Lock()
	defer r.mu.Unlock()
	id := r.nextSessionID
	r.nextSessionID++
	r.calibHistory[cfg.Version] = cfg
	s := &Session{id: id, rt: r, cfg: cfg}
	r.sessions[id] = s
	return s
}

// OpenSession 按 ID 打开会话：用于从快照恢复后继续使用重启前
// 已开始的会话。该会话仍绑定其原校准版本；不存在时返回 nil。
// 注意：调用方不得并发使用同一个 Session。
func (r *Runtime) OpenSession(id uint64) *Session {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.sessions[id]
}

// AcceptedMaxSeq 返回每台设备已纳入输出的最大序号快照。
func (r *Runtime) AcceptedMaxSeq() map[string]uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]uint64, len(r.acceptedMaxSeq))
	for k, v := range r.acceptedMaxSeq {
		out[k] = v
	}
	return out
}

// DeviceState 返回设备当前健康状态（未知设备视为 Healthy）。
func (r *Runtime) DeviceState(deviceID string) health.State {
	r.mu.Lock()
	defer r.mu.Unlock()
	if d, ok := r.devices[deviceID]; ok {
		return d.tracker.State()
	}
	return health.Healthy
}

// Session 是绑定固定校准快照的会话。
type Session struct {
	id  uint64
	rt  *Runtime
	cfg *calibration.Config
}

// ID 返回会话的唯一标识（跨恢复延续，供 OpenSession 重新打开）。
func (s *Session) ID() uint64 { return s.id }

// CalibVersion 返回会话绑定的校准版本。
func (s *Session) CalibVersion() uint64 { return s.cfg.Version }

// Ingest 接入一条测量数据。
//
// 处理顺序：校验 -> 健康门控 -> 去重/重启检测 -> 入重排缓冲。
// 返回值：(立即可输出的读数, 是否重启, 错误)。
// 乱序样本先进入缓冲，待窗口满或 Flush 时按序号有序输出，绝不丢失。
func (s *Session) Ingest(m telemetry.Measurement) (readings []telemetry.Reading, err error) {
	if err := m.Validate(); err != nil {
		s.rt.recordFailure(m.DeviceID)
		return nil, err
	}

	s.rt.mu.Lock()
	defer s.rt.mu.Unlock()

	dev := s.rt.deviceLocked(m.DeviceID)
	if !dev.tracker.Accepting() {
		return nil, fmt.Errorf("%w: %q", ErrDeviceIsolated, m.DeviceID)
	}

	ss := dev.sensorLocked(m.SensorID)
	var out []telemetry.Reading
	if ss.hasSeq && m.Seq < ss.maxSeq {
		if _, dup := ss.seen[m.Seq]; dup {
			return nil, ErrDuplicate
		}
		if m.Seq+uint64(s.rt.window) < ss.maxSeq {
			// 序号回退超出重排窗口：判定为设备重启。
			// 先吐出旧纪元的缓冲样本（不丢失），再重置序号空间。
			out = append(out, s.drainAllLocked(dev, ss)...)
			ss.seen = map[uint64]struct{}{}
			ss.pending = map[uint64]telemetry.Measurement{}
			ss.hasSeq = false
			ss.rebootPending = true
		}
		// 否则：窗口内的乱序迟到样本，正常进入缓冲。
	}
	if _, dup := ss.seen[m.Seq]; dup {
		return nil, ErrDuplicate
	}

	ss.seen[m.Seq] = struct{}{}
	if !ss.hasSeq || m.Seq > ss.maxSeq {
		ss.maxSeq = m.Seq
		ss.hasSeq = true
	}
	ss.pending[m.Seq] = m

	dev.tracker.RecordSuccess()
	return append(out, s.drainLocked(dev, ss)...), nil
}

// Flush 按序号顺序吐出该设备所有缓冲样本（如会话结束/重连后）。
func (s *Session) Flush(deviceID string) []telemetry.Reading {
	s.rt.mu.Lock()
	defer s.rt.mu.Unlock()
	dev, ok := s.rt.devices[deviceID]
	if !ok {
		return nil
	}
	var out []telemetry.Reading
	for _, ss := range dev.sensors {
		out = append(out, s.drainAllLocked(dev, ss)...)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Key.SensorID != out[j].Key.SensorID {
			return out[i].Key.SensorID < out[j].Key.SensorID
		}
		return out[i].Key.Seq < out[j].Key.Seq
	})
	return out
}

// drainLocked 在缓冲达到窗口大小时吐出最旧样本，保留 window-1 个在途样本。
func (s *Session) drainLocked(dev *deviceState, ss *sensorState) []telemetry.Reading {
	if len(ss.pending) < s.rt.window {
		return nil
	}
	seqs := sortedSeqs(ss.pending)
	emit := seqs[:len(seqs)-s.rt.window+1]
	return s.emitLocked(dev, ss, emit)
}

// drainAllLocked 吐出全部缓冲样本。
func (s *Session) drainAllLocked(dev *deviceState, ss *sensorState) []telemetry.Reading {
	return s.emitLocked(dev, ss, sortedSeqs(ss.pending))
}

// emitLocked 将指定序号的样本校准归一化后输出。
func (s *Session) emitLocked(dev *deviceState, ss *sensorState, seqs []uint64) []telemetry.Reading {
	out := make([]telemetry.Reading, 0, len(seqs))
	for _, seq := range seqs {
		m, ok := ss.pending[seq]
		if !ok {
			continue
		}
		delete(ss.pending, seq)
		p, err := s.cfg.ParamsFor(m.SensorID)
		if err != nil {
			// 配置缺少该传感器参数：记录失败但不阻塞其他样本。
			dev.tracker.RecordFailure()
			continue
		}
		rebooted := ss.rebootPending
		ss.rebootPending = false
		out = append(out, telemetry.Reading{
			Key:            m.Key(),
			Timestamp:      m.Timestamp,
			RawValue:       m.Value,
			Unit:           m.Unit,
			Value:          p.Apply(m.Value),
			CalibVersion:   s.cfg.Version,
			DeviceRebooted: rebooted,
		})
		if s.rt.acceptedMaxSeq[m.DeviceID] < seq {
			s.rt.acceptedMaxSeq[m.DeviceID] = seq
		}
	}
	return out
}

// deviceLocked 获取或创建设备状态。
func (r *Runtime) deviceLocked(id string) *deviceState {
	d, ok := r.devices[id]
	if !ok {
		d = &deviceState{
			tracker: health.NewTracker(r.policy, r.now),
			sensors: make(map[string]*sensorState),
		}
		r.devices[id] = d
	}
	return d
}

// sensorLocked 获取或创建传感器流状态。
func (d *deviceState) sensorLocked(id string) *sensorState {
	ss, ok := d.sensors[id]
	if !ok {
		ss = &sensorState{seen: make(map[uint64]struct{}), pending: make(map[uint64]telemetry.Measurement)}
		d.sensors[id] = ss
	}
	return ss
}

// recordFailure 记录一次设备级失败（如非法输入）。
func (r *Runtime) recordFailure(deviceID string) {
	if deviceID == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deviceLocked(deviceID).tracker.RecordFailure()
}

func sortedSeqs(m map[uint64]telemetry.Measurement) []uint64 {
	seqs := make([]uint64, 0, len(m))
	for s := range m {
		seqs = append(seqs, s)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	return seqs
}
