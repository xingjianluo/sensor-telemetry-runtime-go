package health

import (
	"testing"
	"time"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestTracker(pol Policy) (*Tracker, *fakeClock) {
	c := &fakeClock{t: time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC)}
	return NewTracker(pol, c.now), c
}

func TestTransientSpikeNotIsolated(t *testing.T) {
	pol := DefaultPolicy()
	tr, c := newTestTracker(pol)
	// 单次失败 -> Suspect，不隔离。
	tr.RecordFailure()
	t.Logf("after 1 failure: state=%s (threshold=%d)", tr.State(), pol.IsolateThreshold)
	if tr.State() != Suspect {
		t.Fatalf("state=%s, want Suspect", tr.State())
	}
	// 窗口滑过后旧失败失效，再失败一次仍不构成阈值。
	c.advance(pol.Window + time.Second)
	tr.RecordFailure()
	if tr.State() != Suspect {
		t.Fatalf("transient spike misjudged: state=%s, want Suspect", tr.State())
	}
	// 连续成功恢复 Healthy。
	tr.RecordSuccess()
	tr.RecordSuccess()
	if tr.State() != Healthy {
		t.Fatalf("state=%s, want Healthy", tr.State())
	}
}

func TestSustainedFailureIsolatesThenRecovers(t *testing.T) {
	pol := DefaultPolicy()
	tr, c := newTestTracker(pol)
	for i := 0; i < pol.IsolateThreshold; i++ {
		tr.RecordFailure()
		t.Logf("failure #%d -> state=%s", i+1, tr.State())
	}
	if tr.State() != Isolated {
		t.Fatalf("state=%s, want Isolated", tr.State())
	}
	if tr.Accepting() {
		t.Fatal("isolated device must not accept samples")
	}
	// 静默期内失败延长隔离。
	c.advance(pol.Quarantine - time.Second)
	tr.RecordFailure()
	c.advance(2 * time.Second)
	if tr.State() != Isolated {
		t.Fatalf("quarantine not extended: state=%s", tr.State())
	}
	// 静默期满 -> Recovering。
	c.advance(pol.Quarantine)
	if tr.State() != Recovering {
		t.Fatalf("state=%s, want Recovering", tr.State())
	}
	// 恢复期内一次失败 -> 重新隔离。
	tr.RecordFailure()
	if tr.State() != Isolated {
		t.Fatalf("state=%s, want Isolated after relapse", tr.State())
	}
	// 再次静默期满，连续成功 -> Healthy。
	c.advance(pol.Quarantine)
	tr.RecordSuccess()
	tr.RecordSuccess()
	t.Logf("after recovery successes: state=%s", tr.State())
	if tr.State() != Healthy {
		t.Fatalf("state=%s, want Healthy", tr.State())
	}
	if !tr.Accepting() {
		t.Fatal("recovered device must accept samples")
	}
}

func TestSingleDisconnectIsTransient(t *testing.T) {
	pol := DefaultPolicy()
	tr, _ := newTestTracker(pol)
	tr.RecordFailure() // 单次断连
	if tr.State() == Isolated {
		t.Fatal("single disconnect must not isolate device")
	}
	tr.RecordSuccess()
	tr.RecordSuccess()
	if tr.State() != Healthy {
		t.Fatalf("state=%s, want Healthy after reconnect", tr.State())
	}
}
