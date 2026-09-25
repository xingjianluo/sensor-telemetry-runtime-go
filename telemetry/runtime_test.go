package telemetry

import (
	"errors"
	"testing"
	"time"
)

func newTestRuntime() *Runtime {
	store := NewCalibrationStore(Calibration{
		Version: 1, Scale: 0.5, Offset: 1, Min: -1000, Max: 1000,
	})
	return NewRuntime(store, testPolicy())
}

func sample(dev string, epoch, seq uint64, val float64, sec int) Sample {
	return Sample{
		DeviceID: dev, Metric: "temp", Unit: UnitCelsius, Value: val,
		Seq: seq, DeviceEpoch: epoch, Timestamp: at(sec),
		Source: Source{Kind: "gateway", ID: "gw-1"},
	}
}

func TestOutOfOrderAndDuplicate(t *testing.T) {
	rt := newTestRuntime()

	// 乱序到达：seq 3 先于 seq 2
	r3, err := rt.Ingest(sample("dev-a", 1, 3, 10, 3))
	if err != nil || !r3.Accepted {
		t.Fatalf("seq3: err=%v res=%+v", err, r3)
	}
	t.Logf("input seq=3 val=10 -> accepted normalized=%v (cal v%d)", r3.Reading.Normalized, r3.Reading.CalibrationVersion)

	r2, err := rt.Ingest(sample("dev-a", 1, 2, 8, 2))
	if err != nil || !r2.Accepted || !r2.Stale {
		t.Fatalf("seq2 out-of-order: err=%v res=%+v", err, r2)
	}
	t.Logf("input seq=2 val=8 (out-of-order) -> accepted stale=true normalized=%v (basis: seq 2 < maxSeq 3, data not lost)", r2.Reading.Normalized)

	// 重复上报：同纪元同序号
	dup, err := rt.Ingest(sample("dev-a", 1, 3, 10, 3))
	if err != nil || !dup.Duplicate || dup.Accepted {
		t.Fatalf("duplicate: err=%v res=%+v", err, dup)
	}
	t.Logf("input seq=3 again -> duplicate=true (basis: (epoch,seq) already seen)")

	rds := rt.Readings("dev-a")
	if len(rds) != 2 || rds[0].Seq != 2 || rds[1].Seq != 3 {
		t.Fatalf("readings=%+v, want 2 sorted by seq", rds)
	}
	t.Logf("readings sorted by seq: [%d, %d], count=%d (duplicate not stored twice)", rds[0].Seq, rds[1].Seq, len(rds))
}

func TestDeviceRebootEpoch(t *testing.T) {
	rt := newTestRuntime()
	mustAccept(t, rt, sample("dev-b", 1, 1, 10, 1))
	mustAccept(t, rt, sample("dev-b", 1, 2, 11, 2))

	// 设备重启：epoch 2，序号从 1 重新开始 —— 不得因序号回退而丢失
	r, err := rt.Ingest(sample("dev-b", 2, 1, 12, 3))
	if err != nil || !r.Accepted {
		t.Fatalf("post-reboot seq=1: err=%v res=%+v", err, r)
	}
	t.Logf("input epoch=2 seq=1 (device rebooted) -> accepted normalized=%v (basis: new epoch resets seq window)", r.Reading.Normalized)

	// 旧纪元的迟到数据按规则丢弃
	stale, err := rt.Ingest(sample("dev-b", 1, 5, 99, 4))
	if err != nil || !stale.Stale || stale.Accepted {
		t.Fatalf("old-epoch late arrival: err=%v res=%+v", err, stale)
	}
	t.Logf("input epoch=1 seq=5 after reboot -> stale dropped (basis: epoch 1 < current epoch 2)")

	if n := len(rt.Readings("dev-b")); n != 3 {
		t.Fatalf("readings count=%d, want 3", n)
	}
}

func TestCalibrationSwitchSessionsReproducible(t *testing.T) {
	rt := newTestRuntime()
	mustAccept(t, rt, sample("dev-c", 1, 1, 10, 1)) // v1: 10*0.5+1 = 6

	// 不兼容配置被拒绝，会话不变
	bad := Calibration{Version: 2, Scale: 2, Offset: 0, Min: -1000, Max: 1000, MinRawVersion: 5}
	if err := rt.SwitchCalibration(bad, 1); !errors.Is(err, ErrCalibrationIncompatible) {
		t.Fatalf("switch to incompatible config: %v", err)
	}
	t.Logf("switch v2 (requires data>=v5, data=v1) -> rejected, session unchanged")

	// 兼容切换成功，开启新会话
	good := Calibration{Version: 2, Scale: 2, Offset: 0, Min: -1000, Max: 1000, MinRawVersion: 1}
	if err := rt.SwitchCalibration(good, 1); err != nil {
		t.Fatalf("switch: %v", err)
	}
	mustAccept(t, rt, sample("dev-c", 1, 2, 10, 2)) // v2: 10*2+0 = 20

	rds := rt.Readings("dev-c")
	if len(rds) != 2 {
		t.Fatalf("readings=%d, want 2", len(rds))
	}
	t.Logf("session0 reading: raw=%v normalized=%v calVersion=%d session=%d",
		rds[0].Raw, rds[0].Normalized, rds[0].CalibrationVersion, rds[0].SessionID)
	t.Logf("session1 reading: raw=%v normalized=%v calVersion=%d session=%d",
		rds[1].Raw, rds[1].Normalized, rds[1].CalibrationVersion, rds[1].SessionID)
	// 切换前的结论保持旧版本可复现，切换后使用新版本
	if rds[0].Normalized != 6 || rds[0].CalibrationVersion != 1 || rds[0].SessionID != 0 {
		t.Fatalf("old session conclusion altered: %+v", rds[0])
	}
	if rds[1].Normalized != 20 || rds[1].CalibrationVersion != 2 || rds[1].SessionID != 1 {
		t.Fatalf("new session reading wrong: %+v", rds[1])
	}
}

func TestIsolationDoesNotBlockOtherDevices(t *testing.T) {
	rt := newTestRuntime()

	// dev-bad 连续 4 次非法输入 -> Isolated
	for i := 1; i <= 4; i++ {
		s := sample("dev-bad", 1, uint64(i), 10, i)
		s.Unit = "" // 非法：缺单位
		_, err := rt.Ingest(s)
		t.Logf("input dev-bad invalid #%d -> err=%v state=%s", i, err, rt.Health("dev-bad"))
	}
	if st := rt.Health("dev-bad"); st != StateIsolated {
		t.Fatalf("dev-bad state=%s, want isolated", st)
	}

	// 隔离期间合法样本被拒绝（不纳入输出），但错误可区分
	_, err := rt.Ingest(sample("dev-bad", 1, 5, 10, 5))
	var re *RejectError
	if !errors.As(err, &re) || re.Reason != ReasonDeviceIsolated {
		t.Fatalf("isolated ingest: err=%v", err)
	}
	t.Logf("input dev-bad valid sample while isolated -> reject reason=%s", re.Reason)

	// 其他设备不受影响，吞吐正常
	r, err := rt.Ingest(sample("dev-good", 1, 1, 10, 5))
	if err != nil || !r.Accepted {
		t.Fatalf("dev-good blocked by dev-bad isolation: err=%v", err)
	}
	t.Logf("input dev-good while dev-bad isolated -> accepted normalized=%v (isolation is per-device)", r.Reading.Normalized)

	// dev-bad 按确定规则恢复：MinIsolation 后连续 2 次正常 -> Healthy
	rt.Ingest(sample("dev-bad", 1, 6, 10, 20))
	r, err = rt.Ingest(sample("dev-bad", 1, 7, 10, 21))
	if err != nil || !r.Accepted {
		t.Fatalf("dev-bad recovery: err=%v res=%+v", err, r)
	}
	t.Logf("input dev-bad 2 consecutive ok after MinIsolation -> state=%s, re-included normalized=%v",
		rt.Health("dev-bad"), r.Reading.Normalized)
	if st := rt.Health("dev-bad"); st != StateHealthy {
		t.Fatalf("dev-bad state=%s, want healthy", st)
	}
}

func mustAccept(t *testing.T, rt *Runtime, s Sample) IngestResult {
	t.Helper()
	r, err := rt.Ingest(s)
	if err != nil || !r.Accepted {
		t.Fatalf("ingest %+v: err=%v res=%+v", s, err, r)
	}
	return r
}

var _ = time.Now // keep time import if unused later
