package telemetry

import (
	"testing"
	"time"
)

func testPolicy() HealthPolicy {
	return HealthPolicy{
		SuspectAfter: 2,
		IsolateAfter: 4,
		RecoverAfter: 2,
		MinIsolation: 10 * time.Second,
		SpikeFactor:  3.0,
	}
}

func at(sec int) time.Time { return time.Unix(int64(sec), 0) }

func TestSingleDisconnectNotIsolated(t *testing.T) {
	tr := NewHealthTracker(testPolicy())
	tr.RecordFailure(at(0))
	t.Logf("input=1 failure -> state=%s (basis: failRun=1 < SuspectAfter=2)", tr.State())
	if tr.State() != StateHealthy {
		t.Fatalf("single failure changed state to %s", tr.State())
	}
	tr.RecordOK(10, at(1))
	tr.RecordFailure(at(2))
	t.Logf("input=ok then 1 failure -> state=%s (basis: counter reset by ok)", tr.State())
	if tr.State() != StateHealthy {
		t.Fatalf("state=%s, want healthy", tr.State())
	}
}

func TestSpikeNotMisjudged(t *testing.T) {
	tr := NewHealthTracker(testPolicy())
	tr.RecordOK(10, at(0))
	if !tr.RecordOK(100, at(1)) { // 跳变 9 倍 > SpikeFactor 3
		t.Fatal("expected spike detection")
	}
	t.Logf("input value=100 after baseline=10 -> spike=true, state=%s (basis: jump 9x > SpikeFactor 3)", tr.State())
	if tr.State() != StateHealthy {
		t.Fatalf("spike changed state to %s", tr.State())
	}
	// 尖峰不更新基准：下一个接近原基准的值不算尖峰
	if tr.RecordOK(11, at(2)) {
		t.Fatal("value near original baseline misjudged as spike")
	}
	t.Logf("input value=11 -> spike=false (basis: baseline still 10, spike did not update it)")
}

func TestFaultIsolationAndDeterministicRecovery(t *testing.T) {
	tr := NewHealthTracker(testPolicy())
	// 连续 4 次异常 -> Isolated
	for i := 1; i <= 4; i++ {
		tr.RecordFailure(at(i))
		t.Logf("input=failure #%d -> state=%s", i, tr.State())
	}
	if tr.State() != StateIsolated {
		t.Fatalf("state=%s, want isolated", tr.State())
	}
	if tr.CanOutput(at(4)) {
		t.Fatal("isolated device must not output")
	}

	// 隔离期内出现的正常样本不触发恢复（MinIsolation 未满）
	tr.RecordOK(10, at(5))
	t.Logf("input=ok at t=5 (isolation since t=4, MinIsolation=10s) -> state=%s", tr.State())
	if tr.State() != StateIsolated {
		t.Fatalf("early ok changed state to %s", tr.State())
	}

	// 隔离满后一次正常 -> Recovering，但仍不输出
	tr.RecordOK(10, at(15))
	t.Logf("input=ok at t=15 -> state=%s canOutput=%v", tr.State(), tr.CanOutput(at(15)))
	if tr.State() != StateRecovering || tr.CanOutput(at(15)) {
		t.Fatalf("state=%s canOutput=%v, want recovering/false", tr.State(), tr.CanOutput(at(15)))
	}

	// Recovering 期间一次异常 -> 回到 Isolated 并重新计时
	tr.RecordFailure(at(16))
	t.Logf("input=failure during recovering -> state=%s", tr.State())
	if tr.State() != StateIsolated {
		t.Fatalf("state=%s, want isolated", tr.State())
	}

	// 重新走恢复流程：连续 RecoverAfter 次正常 -> Healthy，重新纳入输出
	tr.RecordOK(10, at(30))
	tr.RecordOK(10, at(31))
	t.Logf("input=2 consecutive ok after t=30 -> state=%s canOutput=%v", tr.State(), tr.CanOutput(at(31)))
	if tr.State() != StateHealthy || !tr.CanOutput(at(31)) {
		t.Fatalf("state=%s canOutput=%v, want healthy/true", tr.State(), tr.CanOutput(at(31)))
	}
}

func TestSuspectRecoversOnSingleOK(t *testing.T) {
	tr := NewHealthTracker(testPolicy())
	tr.RecordFailure(at(0))
	tr.RecordFailure(at(1))
	t.Logf("input=2 failures -> state=%s (basis: failRun=2 >= SuspectAfter=2)", tr.State())
	if tr.State() != StateSuspect {
		t.Fatalf("state=%s, want suspect", tr.State())
	}
	if !tr.CanOutput(at(1)) {
		t.Fatal("suspect device should still output")
	}
	tr.RecordOK(5, at(2))
	t.Logf("input=ok -> state=%s", tr.State())
	if tr.State() != StateHealthy {
		t.Fatalf("state=%s, want healthy", tr.State())
	}
}
