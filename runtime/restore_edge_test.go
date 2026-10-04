package runtime

import (
	"testing"

	"github.com/xingjianluo/sensor-telemetry-runtime-go/health"
)

func TestRestoreEmptyRuntime(t *testing.T) {
	rt := New(newStore(), health.DefaultPolicy(), 3)
	data, err := rt.ExportSnapshotBytes()
	if err != nil {
		t.Fatal(err)
	}
	rt2, sessions, err := rt.RestoreBytes(data, health.DefaultPolicy(), nil)
	if err != nil {
		t.Fatalf("restore empty snapshot: %v", err)
	}
	if len(sessions) != 0 || len(rt2.devices) != 0 {
		t.Fatalf("restored runtime not empty: sessions=%d devices=%d", len(sessions), len(rt2.devices))
	}
	// 恢复出的空实例必须立即可用，且会话 ID 从 1 开始（发号器随快照延续）。
	s := rt2.NewSession()
	if s.ID() != 1 {
		t.Fatalf("first session id=%d, want 1", s.ID())
	}
	rs, err := s.Ingest(meas("dev-new", "temp-1", 1, 5))
	if err != nil {
		t.Fatal(err)
	}
	if len(rs) != 0 { // window=3：仍在缓冲
		t.Fatalf("empty-restored runtime ingest wrong: %d emitted", len(rs))
	}
	rs = s.Flush("dev-new")
	if len(rs) != 1 || rs[0].Value != 11 || rs[0].CalibVersion != 1 {
		t.Fatalf("empty-restored runtime output wrong: %+v", rs)
	}
	t.Log("fresh-start snapshot (no devices/sessions) restores into a usable runtime")
}

func TestMultipleSnapshotCycles(t *testing.T) {
	const dev = "dev-cycle"
	rt := New(newStore(), health.DefaultPolicy(), 4)
	s := rt.NewSession()
	s.Ingest(meas(dev, "temp-1", 1, 1))
	s.Ingest(meas(dev, "temp-1", 2, 2))

	// 连续经历两次“冻结 -> 恢复”，缓冲与去重始终延续。
	var cur *Runtime = rt
	var sess *Session = s
	for cycle := 1; cycle <= 2; cycle++ {
		data, _ := cur.ExportSnapshotBytes()
		next, m, err := cur.RestoreBytes(data, health.DefaultPolicy(), nil)
		if err != nil {
			t.Fatalf("cycle %d restore: %v", cycle, err)
		}
		cur, sess = next, m[sess.ID()]
		t.Logf("cycle %d restored, session id=%d still open", cycle, sess.ID())
	}
	if _, err := sess.Ingest(meas(dev, "temp-1", 1, 99)); err != ErrDuplicate {
		t.Fatalf("after 2 cycles old seq resurrected: %v", err)
	}
	rs := sess.Flush(dev)
	if len(rs) != 2 || rs[0].Key.Seq != 1 || rs[1].Key.Seq != 2 {
		t.Fatalf("buffer not preserved across cycles: %+v", rs)
	}
	logReadings(t, "after 2 snapshot cycles", rs)
}
