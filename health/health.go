// Package health 实现确定性的设备健康状态机与故障隔离。
//
// 状态转换（全部规则确定、可复现）：
//
//	Healthy   --窗口内失败数 >= IsolateThreshold-->  Isolated
//	Healthy   --单次失败/断连--------------------->  Suspect
//	Suspect   --连续 RecoverAfter 次成功----------->  Healthy
//	Suspect   --窗口内失败数 >= IsolateThreshold-->  Isolated
//	Isolated  --静默 Quarantine 时长--------------->  Recovering
//	Recovering--连续 RecoverAfter 次成功----------->  Healthy
//	Recovering--任何一次失败----------------------->  Isolated（重新计时）
//
// 短暂尖峰（窗口内失败数未达阈值）或单次断连只会进入 Suspect，
// 不会被误判为永久故障。隔离期间该设备的样本被丢弃，
// 但不影响其他设备的处理。
package health

import "time"

// State 是设备健康状态。
type State int

const (
	Healthy State = iota
	Suspect
	Isolated
	Recovering
)

func (s State) String() string {
	switch s {
	case Healthy:
		return "Healthy"
	case Suspect:
		return "Suspect"
	case Isolated:
		return "Isolated"
	case Recovering:
		return "Recovering"
	}
	return "Unknown"
}

// Policy 是健康评估参数。
type Policy struct {
	Window           time.Duration // 失败统计窗口
	IsolateThreshold int           // 窗口内达到该失败数则隔离
	Quarantine       time.Duration // 隔离静默期，过后进入恢复观察
	RecoverAfter     int           // 恢复所需连续成功次数
}

// DefaultPolicy 返回默认策略。
func DefaultPolicy() Policy {
	return Policy{
		Window:           10 * time.Second,
		IsolateThreshold: 3,
		Quarantine:       5 * time.Second,
		RecoverAfter:     2,
	}
}

// Tracker 跟踪单台设备的健康状态。非并发安全，由上层串行化访问。
type Tracker struct {
	pol Policy
	now func() time.Time // 可注入时钟，便于确定性测试

	state      State
	failures   []time.Time // 窗口内的失败时刻
	isolatedAt time.Time
	successRun int // 连续成功计数
}

// NewTracker 创建跟踪器；now 为 nil 时使用真实时钟。
func NewTracker(pol Policy, now func() time.Time) *Tracker {
	if now == nil {
		now = time.Now
	}
	return &Tracker{pol: pol, now: now, state: Healthy}
}

// RestoreTracker 从快照字段重建跟踪器，使故障隔离与恢复进度跨重启延续。
//
// 参数语义与 Tracker 内部字段一一对应：
//   - state：冻结时的健康状态（调用方须保证在合法枚举范围内）；
//   - failures：统计窗口内的失败时刻（拷贝后使用，不持有入参切片）；
//   - isolatedAt：本次隔离的起始时刻（仅 state==Isolated 时有意义）；
//   - successRun：Suspect/Recovering 下已累计的连续成功次数。
//
// 重建后的状态机会立刻按 now 推进（例如隔离静默期已过则进入
// Recovering），与“从未重启、时钟一直连续走动”的行为一致。
// now 为 nil 时使用真实时钟；长时间无人值守场景下，墙钟单调时
// 隔离/恢复计时自然延续。
func RestoreTracker(pol Policy, now func() time.Time, state State, failures []time.Time, isolatedAt time.Time, successRun int) *Tracker {
	if now == nil {
		now = time.Now
	}
	t := &Tracker{
		pol:        pol,
		now:        now,
		state:      state,
		isolatedAt: isolatedAt,
		successRun: successRun,
	}
	t.failures = append(t.failures, failures...)
	return t
}

// StateAndProgress 返回重建快照所需的全部字段（供运行时冻结状态使用）。
func (t *Tracker) StateAndProgress() (state State, failures []time.Time, isolatedAt time.Time, successRun int) {
	t.advance()
	fs := make([]time.Time, len(t.failures))
	copy(fs, t.failures)
	return t.state, fs, t.isolatedAt, t.successRun
}

// State 返回当前状态（会按时间推进 Isolated -> Recovering）。
func (t *Tracker) State() State {
	t.advance()
	return t.state
}

// Accepting 报告当前状态下样本是否应纳入输出。
// Isolated 状态下样本被丢弃，其余状态均纳入。
func (t *Tracker) Accepting() bool { return t.State() != Isolated }

// RecordSuccess 记录一次有效样本。
func (t *Tracker) RecordSuccess() {
	t.advance()
	switch t.state {
	case Suspect, Recovering:
		t.successRun++
		if t.successRun >= t.pol.RecoverAfter {
			t.state = Healthy
			t.successRun = 0
			t.failures = t.failures[:0]
		}
	}
}

// RecordFailure 记录一次失败（坏样本或断连）。
func (t *Tracker) RecordFailure() {
	t.advance()
	t.successRun = 0
	switch t.state {
	case Recovering:
		// 恢复期内任何失败立即重新隔离。
		t.isolate()
	case Isolated:
		// 隔离期内的失败延长静默期。
		t.isolatedAt = t.now()
	default:
		t.failures = append(t.failures, t.now())
		t.prune()
		if len(t.failures) >= t.pol.IsolateThreshold {
			t.isolate()
		} else if t.state == Healthy {
			t.state = Suspect
		}
	}
}

// advance 按时间推进状态（隔离静默期满进入恢复观察）。
func (t *Tracker) advance() {
	if t.state == Isolated && t.now().Sub(t.isolatedAt) >= t.pol.Quarantine {
		t.state = Recovering
		t.successRun = 0
		t.failures = t.failures[:0]
	}
}

func (t *Tracker) isolate() {
	t.state = Isolated
	t.isolatedAt = t.now()
	t.successRun = 0
	t.failures = t.failures[:0]
}

// prune 移除窗口外的失败记录。
func (t *Tracker) prune() {
	cut := t.now().Add(-t.pol.Window)
	keep := t.failures[:0]
	for _, ts := range t.failures {
		if ts.After(cut) {
			keep = append(keep, ts)
		}
	}
	t.failures = keep
}
