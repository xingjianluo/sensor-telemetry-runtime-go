package telemetry

import (
	"math"
	"sync"
	"time"
)

// HealthState 是设备健康状态机的状态。
type HealthState int

const (
	StateHealthy HealthState = iota
	StateSuspect             // 出现零星异常，观察中
	StateIsolated            // 判定故障，输出中隔离
	StateRecovering          // 故障后观察到恢复迹象，验证中
)

func (h HealthState) String() string {
	switch h {
	case StateHealthy:
		return "healthy"
	case StateSuspect:
		return "suspect"
	case StateIsolated:
		return "isolated"
	case StateRecovering:
		return "recovering"
	}
	return "unknown"
}

// HealthPolicy 定义状态转换的确定规则。
type HealthPolicy struct {
	SuspectAfter    int           // 连续异常多少次进入 Suspect
	IsolateAfter    int           // 连续异常多少次进入 Isolated
	RecoverAfter    int           // Recovering 下连续正常多少次回到 Healthy
	MinIsolation    time.Duration // 最短隔离时长，防止抖动
	SpikeFactor     float64       // 相对上次值的跳变倍数，超过记为尖峰（单次不判故障）
}

// HealthTracker 跟踪单台设备的健康状态。所有方法可并发调用。
//
// 状态转换（确定规则）：
//
//	Healthy    --连续异常>=SuspectAfter--> Suspect
//	Suspect    --连续异常>=IsolateAfter--> Isolated
//	Suspect    --一次正常-------------> Healthy
//	Isolated   --隔离满MinIsolation后一次正常--> Recovering
//	Recovering --连续正常>=RecoverAfter---> Healthy（重新纳入输出）
//	Recovering --一次异常-------------> Isolated（重新计时）
type HealthTracker struct {
	mu      sync.Mutex
	policy  HealthPolicy
	state   HealthState
	failRun int       // 连续异常计数
	okRun   int       // Recovering 下连续正常计数
	since   time.Time // 进入当前状态的时间
	lastVal float64
	hasLast bool
}

// NewHealthTracker 按策略创建跟踪器。
func NewHealthTracker(p HealthPolicy) *HealthTracker {
	return &HealthTracker{policy: p, state: StateHealthy}
}

// State 返回当前状态。
func (t *HealthTracker) State() HealthState {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.state
}

// RecordOK 记录一次正常样本。相对上次值的跳变超过 SpikeFactor 倍时
// 判定为瞬时尖峰：返回 isSpike=true，且不影响健康计数、不更新基准值，
// 因此单次尖峰不会被误判为故障或恢复。
func (t *HealthTracker) RecordOK(value float64, now time.Time) (isSpike bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.hasLast && t.policy.SpikeFactor > 0 {
		base := math.Abs(t.lastVal)
		if base < 1e-9 {
			base = 1e-9
		}
		if math.Abs(value-t.lastVal)/base > t.policy.SpikeFactor {
			return true // 尖峰：不更新基准，不改变状态计数
		}
	}
	t.lastVal, t.hasLast = value, true
	t.failRun = 0
	switch t.state {
	case StateSuspect:
		t.setState(StateHealthy, now)
	case StateIsolated:
		// 隔离满最短时长后出现的正常样本才视为恢复迹象。
		if now.Sub(t.since) >= t.policy.MinIsolation {
			t.setState(StateRecovering, now)
			t.okRun = 1
		}
	case StateRecovering:
		t.okRun++
		if t.okRun >= t.policy.RecoverAfter {
			t.setState(StateHealthy, now)
		}
	}
	return false
}

// RecordFailure 记录一次异常（断连、校验失败等）。
// 单次异常只会推进计数，达到阈值才转换状态，因此单次断连不会被判永久故障。
func (t *HealthTracker) RecordFailure(now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.failRun++
	t.okRun = 0
	switch t.state {
	case StateHealthy:
		if t.failRun >= t.policy.SuspectAfter {
			t.setState(StateSuspect, now)
		}
		fallthrough // Suspect 与 Healthy 共用隔离阈值判定
	case StateSuspect:
		if t.failRun >= t.policy.IsolateAfter {
			t.setState(StateIsolated, now)
		}
	case StateRecovering:
		// 恢复验证期间出现异常：回到隔离并重新计时。
		t.setState(StateIsolated, now)
	case StateIsolated:
		// 隔离期间持续异常：重新计时最短隔离时长。
		t.since = now
	}
}

// CanOutput 报告当前状态下该设备的测量是否应纳入输出。
// 隔离与恢复验证期间不纳入输出；恢复完成（回到 Healthy）后按确定规则重新纳入。
func (t *HealthTracker) CanOutput(now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.state == StateHealthy || t.state == StateSuspect
}

func (t *HealthTracker) setState(s HealthState, now time.Time) {
	t.state = s
	t.since = now
	if s == StateHealthy {
		t.failRun, t.okRun = 0, 0
	}
}
