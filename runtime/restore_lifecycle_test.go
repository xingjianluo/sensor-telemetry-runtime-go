package runtime

import (
	"errors"
	"testing"
	"time"

	"github.com/xingjianluo/sensor-telemetry-runtime-go/calibration"
	"github.com/xingjianluo/sensor-telemetry-runtime-go/health"
	"github.com/xingjianluo/sensor-telemetry-runtime-go/snapshot"
	"github.com/xingjianluo/sensor-telemetry-runtime-go/telemetry"
)

func TestRestoreContinuesRebootBoundary(t *testing.T) {
	const window = 3
	const dev = "dev-reboot"
	// 冻结点：seq=1 触发序号回退超窗口（1+3<100）-> 判定重启：
	// 旧纪元缓冲 seq=100 先吐出，随后重置序号空间，seq=1 进入新
	// 重排缓冲且 rebootPending=true（尚未吐出）。
	freeze := func() (*Runtime, *Session, []telemetry.Reading, *snapshot.Snapshot) {
		rt := New(newStore(), health.DefaultPolicy(), window)
		s := rt.NewSession()
		var pre []telemetry.Reading
		if rs, err := s.Ingest(meas(dev, "temp-1", 100, 100)); err != nil {
			t.Fatal(err)
		} else {
			pre = append(pre, rs...)
		}
		rs, err := s.Ingest(meas(dev, "temp-1", 1, 1))
		if err != nil {
			t.Fatalf("reboot trigger: %v", err)
		}
		pre = append(pre, rs...) // 旧纪元 seq=100 在此吐出
		snap := rt.ExportSnapshot()
		ss := findSensor(t, snap, dev, "temp-1")
		if !ss.RebootPending || len(ss.Pending) != 1 || ss.Pending[0].Seq != 1 {
			t.Fatalf("freeze point wrong: reboot=%v pending=%+v", ss.RebootPending, ss.Pending)
		}
		t.Logf("frozen at reboot boundary: old epoch emitted=%d, pending seq=1 rebootPending=true", len(pre))
		return rt, s, pre, snap
	}

	// 基线：不重启，继续 2,3 后收尾。
	_, sb, basePre, _ := freeze()
	base := append([]telemetry.Reading{}, basePre...)
	sb.Ingest(meas(dev, "temp-1", 2, 2))
	rs, _ := sb.Ingest(meas(dev, "temp-1", 3, 3)) // 缓冲满 -> 吐出新纪元 seq=1
	base = append(base, rs...)
	base = append(base, sb.Flush(dev)...)
	if !base[1].DeviceRebooted || base[1].Key.Seq != 1 {
		t.Fatalf("baseline assumption wrong: %+v", base)
	}

	// 恢复线：重启边界必须逐字段一致。
	rt, s, pre, snap := freeze()
	data, _ := exportForTest(snap)
	rt2, _, err := rt.RestoreBytes(data, health.DefaultPolicy(), nil)
	if err != nil {
		t.Fatal(err)
	}
	s2 := rt2.OpenSession(s.ID())
	s2.Ingest(meas(dev, "temp-1", 2, 2))
	rs2, _ := s2.Ingest(meas(dev, "temp-1", 3, 3))
	got := append([]telemetry.Reading{}, pre...)
	got = append(got, rs2...)
	got = append(got, s2.Flush(dev)...)
	logReadings(t, "restored full output", got)
	sameReadings(t, got, base, "reboot-boundary")

	// 新纪元旧序号 100 可再次使用而不判重（序号空间已重置）。
	if _, err := s2.Ingest(meas(dev, "temp-1", 100, 7)); err != nil {
		t.Fatalf("post-reboot old seq space should be reusable: %v", err)
	}
	t.Log("post-restore: new epoch reuses seq space; reboot flag preserved on first output")
}

func TestRestoreContinuesIsolationAndRecovery(t *testing.T) {
	pol := health.DefaultPolicy()
	clk := &fakeClock{t: base}
	rt := NewWithClock(newStore(), pol, 1, clk.now)
	s := rt.NewSession()

	// 制造 2 次失败（Suspect），冻结恢复进度。
	bad := meas("dev-bad", "temp-1", 1, 1)
	bad.Unit = ""
	for i := 0; i < pol.IsolateThreshold-1; i++ {
		s.Ingest(bad)
	}
	snap1 := rt.ExportSnapshot()
	if h := findDevice(snap1, "dev-bad").Health; h.State != int(health.Suspect) || len(h.Failures) != 2 {
		t.Fatalf("freeze state wrong: %+v", h)
	}
	t.Logf("frozen at Suspect: failures=%d, window=%s", len(snap1.Devices[0].Health.Failures), pol.Window)

	data, _ := exportForTest(snap1)
	rt2, _, err := rt.RestoreBytes(data, pol, clk.now)
	if err != nil {
		t.Fatal(err)
	}
	s2 := rt2.NewSession()
	// 基线：连续运行下再来一次失败即隔离；恢复实例行为必须一致。
	s2.Ingest(bad)
	if st := rt2.DeviceState("dev-bad"); st != health.Isolated {
		t.Fatalf("restored state=%s, want Isolated after 3rd failure", st)
	}
	t.Logf("after restore + 1 more failure: state=%s (failure window carried over)", rt2.DeviceState("dev-bad"))

	// 隔离中样本被拒绝，其他设备不被阻塞。
	if _, err := s2.Ingest(meas("dev-bad", "temp-1", 2, 1)); !errors.Is(err, ErrDeviceIsolated) {
		t.Fatalf("want ErrDeviceIsolated, got %v", err)
	}
	if rs, err := s2.Ingest(meas("dev-good", "temp-1", 1, 20)); err != nil || len(rs) != 1 {
		t.Fatalf("other device blocked after restore: rs=%v err=%v", rs, err)
	}
	t.Log("isolation enforced after restore; other device still flowing")

	// 在 Recovering 中段（差 1 次成功）再冻结并恢复，恢复进度延续。
	clk.advance(pol.Quarantine + time.Second)
	if _, err := s2.Ingest(meas("dev-bad", "temp-1", 10, 1)); err != nil {
		t.Fatal(err)
	}
	if st := rt2.DeviceState("dev-bad"); st != health.Recovering {
		t.Fatalf("state=%s, want Recovering", st)
	}
	snap2 := rt2.ExportSnapshot()
	h2 := findDevice(snap2, "dev-bad").Health
	t.Logf("frozen mid-recovery: state=%s successRun=%d (need %d)",
		health.State(h2.State), h2.SuccessRun, pol.RecoverAfter)
	data2, _ := exportForTest(snap2)
	rt3, _, err := rt2.RestoreBytes(data2, pol, clk.now)
	if err != nil {
		t.Fatal(err)
	}
	s3 := rt3.NewSession()
	if _, err := s3.Ingest(meas("dev-bad", "temp-1", 11, 1)); err != nil {
		t.Fatal(err)
	}
	if st := rt3.DeviceState("dev-bad"); st != health.Healthy {
		t.Fatalf("state=%s, recovery progress reset on restore", st)
	}
	t.Log("recovery success-run carried across second restore -> Healthy")
}

func TestRestoreKeepsCalibVersionPerSession(t *testing.T) {
	store := newStore() // v1: raw*2+1
	rt := New(store, health.DefaultPolicy(), 1)
	old := rt.NewSession() // 绑定 v1

	if err := store.Update(&calibration.Config{
		Version: 2,
		Sensors: map[string]calibration.Params{"temp-1": {Scale: 10, Offset: 0}},
	}, rt.AcceptedMaxSeq()); err != nil {
		t.Fatal(err)
	}
	mid := rt.NewSession() // 绑定 v2
	// v1 旧会话处理一条（输出 calib v1）。
	if rs, err := old.Ingest(meas("dev-a", "temp-1", 1, 5)); err != nil || rs[0].Value != 11 {
		t.Fatalf("old session setup: %v", err)
	}
	// v2 会话只接入不吐出（window=1 实际立即吐出，故取其结论即可）。
	rs, err := mid.Ingest(meas("dev-b", "temp-1", 1, 5))
	if err != nil || rs[0].Value != 50 || rs[0].CalibVersion != 2 {
		t.Fatalf("mid session setup: %+v err=%v", rs, err)
	}
	snap := rt.ExportSnapshot()
	data, _ := exportForTest(snap)

	rt2, sessions, err := rt.RestoreBytes(data, health.DefaultPolicy(), nil)
	if err != nil {
		t.Fatal(err)
	}
	old2, mid2 := sessions[old.ID()], sessions[mid.ID()]
	t.Logf("restored: session#%d v%d, session#%d v%d; store current=v%d",
		old2.ID(), old2.CalibVersion(), mid2.ID(), mid2.CalibVersion(),
		rt2.calib.Snapshot().Version)
	if old2.CalibVersion() != 1 || mid2.CalibVersion() != 2 {
		t.Fatal("session calib versions not continued")
	}
	if rt2.calib.Snapshot().Version != 2 {
		t.Fatal("store current calib version not restored")
	}
	// 恢复后旧会话仍按 v1 归一化；当前配置会话按 v2。
	r1, _ := old2.Ingest(meas("dev-a", "temp-1", 2, 5))
	r2, _ := mid2.Ingest(meas("dev-b", "temp-1", 2, 5))
	logReadings(t, "restored v1 session", r1)
	logReadings(t, "restored v2 session", r2)
	if r1[0].Value != 11 || r1[0].CalibVersion != 1 {
		t.Fatalf("old session lost v1 binding: %+v", r1[0])
	}
	if r2[0].Value != 50 || r2[0].CalibVersion != 2 {
		t.Fatalf("mid session lost v2 binding: %+v", r2[0])
	}
	// 恢复后从 v2 升级 v3，兼容性校验基于已输出水位继续生效。
	if err := rt2.calib.Update(&calibration.Config{
		Version: 3,
		Sensors: map[string]calibration.Params{"temp-1": {Scale: 1, Offset: 0}},
	}, rt2.AcceptedMaxSeq()); err != nil {
		t.Fatalf("post-restore update v3: %v", err)
	}
	fresh := rt2.NewSession()
	if r3, _ := fresh.Ingest(meas("dev-c", "temp-1", 1, 7)); r3[0].Value != 7 || r3[0].CalibVersion != 3 {
		t.Fatalf("new session after restore should use v3: %+v", r3)
	}
	t.Log("post-restore calib upgrades still work; old sessions pinned to their versions")
}
