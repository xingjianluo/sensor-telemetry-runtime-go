package runtime

import (
	"testing"

	"github.com/xingjianluo/sensor-telemetry-runtime-go/health"
	"github.com/xingjianluo/sensor-telemetry-runtime-go/telemetry"
)

// readAll 对一个全新会话执行相同的接入+收尾序列，汇总全部输出。
// seqStream 中 seq 为 0 的位置表示对上一个非零 seq 做一次重复上报。
func runScenario(t *testing.T, s *Session, dev string, seqs []uint64) []telemetry.Reading {
	t.Helper()
	var all []telemetry.Reading
	var last uint64
	for _, seq := range seqs {
		emitSeq := seq
		duplicate := false
		if seq == 0 {
			emitSeq = last
			duplicate = true
		}
		last = emitSeq
		rs, err := s.Ingest(meas(dev, "temp-1", emitSeq, float64(emitSeq)))
		if duplicate {
			if err != ErrDuplicate {
				t.Fatalf("dup seq=%d: want ErrDuplicate, got %v", emitSeq, err)
			}
			t.Logf("  in: seq=%d (repeat) -> rejected duplicate", emitSeq)
			continue
		}
		if err != nil {
			t.Fatalf("seq=%d: %v", emitSeq, err)
		}
		t.Logf("  in: seq=%d -> emitted=%d", emitSeq, len(rs))
		logReadings(t, "emit", rs)
		all = append(all, rs...)
	}
	rest := s.Flush(dev)
	t.Logf("  flush -> emitted=%d", len(rest))
	logReadings(t, "flush", rest)
	return append(all, rest...)
}

// sameReadings 逐字段比较两条输出序列（含归一化值与校准版本、重启标记）。
func sameReadings(t *testing.T, got, want []telemetry.Reading, label string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: count %d != %d", label, len(got), len(want))
	}
	for i := range want {
		a, b := got[i], want[i]
		if a.Key != b.Key || a.Value != b.Value || a.CalibVersion != b.CalibVersion ||
			a.DeviceRebooted != b.DeviceRebooted || a.RawValue != b.RawValue ||
			a.Unit != b.Unit || !a.Timestamp.Equal(b.Timestamp) {
			t.Fatalf("%s: reading %d differs:\n got %+v\nwant %+v", label, i, a, b)
		}
	}
}

func TestRestoreEquivalentToContinuous_OutOfOrderDupPending(t *testing.T) {
	// 序列：含乱序、重复上报；快照点选在缓冲仍有未吐出样本时。
	seqs := []uint64{1, 4, 2, 0, 3, 5} // 0 = 重复上报 seq=2
	const window = 4
	const dev = "dev-1"
	splitAfter := 3 // 接入 1,4,2 后冻结：缓冲内 {1,2,4}，均未吐出

	// 基线：一直连续运行，不重启。
	t.Log("== baseline: continuous run ==")
	rtBase := New(newStore(), health.DefaultPolicy(), window)
	base := runScenario(t, rtBase.NewSession(), dev, seqs)

	// 恢复线：前半段 -> 冻结 -> 恢复 -> 后半段。
	t.Log("== restore run: freeze with pending buffer ==")
	rt := New(newStore(), health.DefaultPolicy(), window)
	s := rt.NewSession()
	var pre []telemetry.Reading
	for i := 0; i < splitAfter; i++ {
		rs, err := s.Ingest(meas(dev, "temp-1", seqs[i], float64(seqs[i])))
		if err != nil {
			t.Fatal(err)
		}
		pre = append(pre, rs...)
	}
	snap := rt.ExportSnapshot()
	pendingBefore := len(findSensor(t, snap, dev, "temp-1").Pending)
	t.Logf("frozen with %d pending (un-emitted) samples", pendingBefore)
	if pendingBefore != 3 {
		t.Fatalf("setup: want 3 pending at freeze, got %d", pendingBefore)
	}
	data, err := exportForTest(snap)
	if err != nil {
		t.Fatal(err)
	}
	rt2, sessions, err := rt.RestoreBytes(data, health.DefaultPolicy(), nil)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	s2 := sessions[s.ID()]
	t.Logf("restored session id=%d bound to calib v%d (was v%d)",
		s2.ID(), s2.CalibVersion(), s.CalibVersion())
	if s2 == nil || s2.CalibVersion() != s.CalibVersion() {
		t.Fatal("session calib version not continued")
	}

	// 恢复后重放后半段（重复上报 seq=2 与 seq=3,5）。
	var post []telemetry.Reading
	for i := splitAfter; i < len(seqs); i++ {
		emitSeq := seqs[i]
		dup := emitSeq == 0
		if dup {
			emitSeq = seqs[i-1]
		}
		rs, err := s2.Ingest(meas(dev, "temp-1", emitSeq, float64(emitSeq)))
		if dup {
			t.Logf("  in(post): seq=%d repeat -> %v", emitSeq, err)
			if err != ErrDuplicate {
				t.Fatalf("post-restore dup not recognized: %v", err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("post-restore seq=%d: %v", emitSeq, err)
		}
		post = append(post, rs...)
	}
	post = append(post, s2.Flush(dev)...)

	recovered := append(append([]telemetry.Reading{}, pre...), post...)
	t.Logf("baseline=%d readings, restored=%d readings", len(base), len(recovered))
	sameReadings(t, recovered, base, "continuous-vs-restored")

	// 恢复后再重复旧序号仍被拒绝（已接纳编号不复活）。
	if _, err := s2.Ingest(meas(dev, "temp-1", 1, 99)); err != ErrDuplicate {
		t.Fatalf("accepted seq replayed as new: %v", err)
	}
	t.Log("post-restore replay of old seq=1 still rejected as duplicate")

	// 恢复后新建会话不得复用旧 ID，且绑定当前校准版本。
	s3 := rt2.NewSession()
	if s3.ID() <= s2.ID() {
		t.Fatalf("new session id %d reused old id space", s3.ID())
	}
}
