package telemetry

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Reading 是一条归一化后的可审计测量结果。
type Reading struct {
	DeviceID           string
	Metric             string
	Unit               Unit
	Raw                float64
	Normalized         float64
	CalibrationVersion uint64 // 结论可复现：记录所用校准版本
	SessionID          uint64 // 结论可复现：记录所属会话
	Seq                uint64
	DeviceEpoch        uint64
	Timestamp          time.Time
}

// IngestResult 描述一次接入的判定结果。
type IngestResult struct {
	Accepted  bool
	Duplicate bool // 重复上报，已去重
	Stale     bool // 乱序/旧纪元到达的数据，按确定规则处理
	Spike     bool // 瞬时尖峰，不计入健康评估
	Reading   *Reading
}

// epochState 记录设备某一重启纪元内的序号去重状态。
type epochState struct {
	maxSeq uint64
	seen   map[uint64]struct{}
}

// deviceState 是单台设备的运行时状态，方法在设备级互斥锁下串行执行。
type deviceState struct {
	mu       sync.Mutex
	tracker  *HealthTracker
	epoch    uint64
	hasEpoch bool
	cur      *epochState
	readings []Reading
}

// Runtime 是多设备遥测运行时入口。
// 并发语义：不同设备的 Ingest 互不阻塞（设备级锁），
// 校准读取无锁（原子快照），单台设备被隔离不影响其他设备吞吐。
type Runtime struct {
	store   *CalibrationStore
	policy  HealthPolicy
	session atomic.Uint64

	mu      sync.Mutex
	devices map[string]*deviceState
}

// NewRuntime 创建运行时。
func NewRuntime(store *CalibrationStore, policy HealthPolicy) *Runtime {
	return &Runtime{
		store:   store,
		policy:  policy,
		devices: make(map[string]*deviceState),
	}
}

func (r *Runtime) device(id string) *deviceState {
	r.mu.Lock()
	defer r.mu.Unlock()
	d, ok := r.devices[id]
	if !ok {
		d = &deviceState{tracker: NewHealthTracker(r.policy)}
		r.devices[id] = d
	}
	return d
}

// Ingest 接入一条样本：校验 → 去重/乱序处理 → 健康评估 → 校准归一化。
// 健康评估使用样本自带时间戳，保证判定可复现、测试无需真实时钟。
func (r *Runtime) Ingest(s Sample) (IngestResult, error) {
	d := r.device(s.DeviceID)
	d.mu.Lock()
	defer d.mu.Unlock()
	now := s.Timestamp

	// 1. 静态校验：失败计入设备健康（可能预示设备故障）。
	if err := Validate(s); err != nil {
		d.tracker.RecordFailure(now)
		return IngestResult{}, err
	}

	// 2. 纪元（设备重启）处理：新纪元到来时重置序号窗口，
	//    重启后从低序号重新开始的数据不会丢失；旧纪元的迟到数据按规则丢弃。
	if !d.hasEpoch || s.DeviceEpoch > d.epoch {
		d.epoch, d.hasEpoch = s.DeviceEpoch, true
		d.cur = &epochState{seen: make(map[uint64]struct{})}
	} else if s.DeviceEpoch < d.epoch {
		return IngestResult{Stale: true}, nil
	}

	// 3. 去重与乱序：同纪元内序号重复则去重；序号回退视为乱序，
	//    乱序数据仍被接受（不丢失），仅标记 Stale。
	res := IngestResult{}
	if _, dup := d.cur.seen[s.Seq]; dup {
		res.Duplicate = true
		return res, nil
	}
	d.cur.seen[s.Seq] = struct{}{}
	if s.Seq < d.cur.maxSeq {
		res.Stale = true
	} else {
		d.cur.maxSeq = s.Seq
	}

	// 4. 校准归一化：取当前配置的一致性快照，越界计入健康并拒绝。
	cal := r.store.Snapshot()
	norm, err := cal.Normalize(s.Value)
	if err != nil {
		d.tracker.RecordFailure(now)
		return IngestResult{}, err
	}

	// 5. 健康评估：尖峰不影响健康计数；隔离/恢复验证期间不纳入输出，
	//    但仍记录正常样本以便按确定规则恢复。
	spike := d.tracker.RecordOK(s.Value, now)
	res.Spike = spike
	if !d.tracker.CanOutput(now) {
		return res, &RejectError{
			Reason: ReasonDeviceIsolated,
			Detail: "device " + s.DeviceID + " is " + d.tracker.State().String(),
		}
	}

	rd := Reading{
		DeviceID:           s.DeviceID,
		Metric:             s.Metric,
		Unit:               s.Unit,
		Raw:                s.Value,
		Normalized:         norm,
		CalibrationVersion: cal.Version,
		SessionID:          r.session.Load(),
		Seq:                s.Seq,
		DeviceEpoch:        s.DeviceEpoch,
		Timestamp:          s.Timestamp,
	}
	d.readings = append(d.readings, rd)
	res.Accepted = true
	res.Reading = &rd
	return res, nil
}

// SwitchCalibration 切换校准配置并开启新会话；已开始的会话中
// 已产生的 Reading 保留其原校准版本与会话号，结论可复现。
// 配置不兼容既有数据时返回错误，当前配置与会话不变。
func (r *Runtime) SwitchCalibration(next Calibration, dataVersion uint64) error {
	if err := r.store.Apply(next, dataVersion); err != nil {
		return err
	}
	r.session.Add(1)
	return nil
}

// Health 返回设备当前健康状态；未知设备视为 Healthy。
func (r *Runtime) Health(deviceID string) HealthState {
	r.mu.Lock()
	d, ok := r.devices[deviceID]
	r.mu.Unlock()
	if !ok {
		return StateHealthy
	}
	return d.tracker.State()
}

// Readings 返回设备已接受的归一化结果，按 (DeviceEpoch, Seq) 排序。
func (r *Runtime) Readings(deviceID string) []Reading {
	r.mu.Lock()
	d, ok := r.devices[deviceID]
	r.mu.Unlock()
	if !ok {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]Reading, len(d.readings))
	copy(out, d.readings)
	sort.Slice(out, func(i, j int) bool {
		if out[i].DeviceEpoch != out[j].DeviceEpoch {
			return out[i].DeviceEpoch < out[j].DeviceEpoch
		}
		return out[i].Seq < out[j].Seq
	})
	return out
}
