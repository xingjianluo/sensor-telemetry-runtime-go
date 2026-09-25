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
