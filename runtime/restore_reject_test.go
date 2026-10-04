package runtime

import (
	"errors"
	"testing"
	"time"

	"github.com/xingjianluo/sensor-telemetry-runtime-go/health"
	"github.com/xingjianluo/sensor-telemetry-runtime-go/snapshot"
)

func TestRestoreRejectsIncompatibleData(t *testing.T) {
	rt := New(newStore(), health.DefaultPolicy(), 2)
	s := rt.NewSession()
	s.Ingest(meas("dev-1", "temp-1", 1, 1))
	good, err := rt.ExportSnapshotBytes()
	if err != nil {
		t.Fatal(err)
	}

	// 1) 非 JSON / 坏 magic / 未来版本。
	if _, _, gerr := rt.RestoreBytes([]byte("{nope"), health.Policy{}, nil); !errors.Is(gerr, snapshot.ErrMalformed) {
		t.Fatalf("garbage: want ErrMalformed, got %v", gerr)
	} else {
		t.Logf("garbage rejected: %v", gerr)
	}
	badMagic := append([]byte(nil), good...)
	badMagic[len(badMagic)-2] = 'x' // 大概率破坏 JSON；若恰好没破坏，下面用构造法兜底
	snap := rt.ExportSnapshot()
	snap.Magic = "wrong"
	b, _ := snapshot.Encode(snap)
	if _, _, err := rt.RestoreBytes(b, health.Policy{}, nil); !errors.Is(err, snapshot.ErrBadMagic) {
		t.Fatalf("bad magic: want ErrBadMagic, got %v", err)
	}
	t.Logf("bad magic rejected: %v", snapshot.ErrBadMagic)
	future := rt.ExportSnapshot()
	future.Format = snapshot.FormatVersion + 1
	b, _ = snapshot.Encode(future)
	if _, _, err := rt.RestoreBytes(b, health.Policy{}, nil); !errors.Is(err, snapshot.ErrUnsupportedVersion) {
		t.Fatalf("future version: want ErrUnsupportedVersion, got %v", err)
	}
	t.Logf("future format v%d rejected: %v", snapshot.FormatVersion+1, snapshot.ErrUnsupportedVersion)

	// 2) 语义不兼容：会话 ID 占用了发号器未来空间（恢复后会复用旧 ID）。
	snap2 := rt.ExportSnapshot()
	snap2.NextSessionID = snap2.Sessions[0].ID // id >= next -> 冲突
	b, _ = snapshot.Encode(snap2)
	if _, _, err := rt.RestoreBytes(b, health.DefaultPolicy(), nil); !errors.Is(err, ErrRestoreIncompatible) {
		t.Fatalf("session-id lease: want ErrRestoreIncompatible, got %v", err)
	}
	t.Logf("session id lease conflict rejected: %v", err)

	// 3) 在途样本不在 seen 集合中（无歧义区分被破坏）。
	snap3 := rt.ExportSnapshot()
	for i := range snap3.Devices[0].Sensors {
		snap3.Devices[0].Sensors[i].Seen = nil
	}
	b, _ = snapshot.Encode(snap3)
	if _, _, err := rt.RestoreBytes(b, health.DefaultPolicy(), nil); !errors.Is(err, ErrRestoreIncompatible) {
		t.Fatalf("pending-not-in-seen: want ErrRestoreIncompatible, got %v", err)
	}
	t.Logf("pending/seen inconsistency rejected: %v", err)

	// 4) 在途样本本身非法（单位不合法）：恢复必须拒绝而不是带病运行。
	snap4 := rt.ExportSnapshot()
	snap4.Devices[0].Sensors[0].Pending[0].Unit = "bogus-unit"
	b, _ = snapshot.Encode(snap4)
	if _, _, err := rt.RestoreBytes(b, health.DefaultPolicy(), nil); !errors.Is(err, ErrRestoreIncompatible) {
		t.Fatalf("invalid pending measurement: want ErrRestoreIncompatible, got %v", err)
	}
	t.Logf("invalid pending measurement rejected: %v", err)

	// 5) 健康策略不一致：恢复语义会改变，必须拒绝。
	other := health.DefaultPolicy()
	other.Quarantine += time.Second
	if _, _, err := rt.RestoreBytes(good, other, nil); !errors.Is(err, ErrRestoreIncompatible) {
		t.Fatalf("policy mismatch: want ErrRestoreIncompatible, got %v", err)
	}
	t.Logf("health policy mismatch rejected: %v", err)

	// 6) nil 快照。
	if _, _, err := rt.Restore(nil, health.Policy{}, nil); !errors.Is(err, ErrRestoreIncompatible) {
		t.Fatalf("nil snapshot: want ErrRestoreIncompatible, got %v", err)
	}
}

func TestFailedRestoreDoesNotAffectCurrentInstance(t *testing.T) {
	store := newStore()
	rt := New(store, health.DefaultPolicy(), 2)
	s := rt.NewSession()
	// 只接入 seq=1（在途未吐出），随后制造失败恢复尝试。
	s.Ingest(meas("dev-1", "temp-1", 1, 5))
	snap := rt.ExportSnapshot()
	corrupt := *snap
	corrupt.Sessions[0].CalibVersion = 42
	b, _ := snapshot.Encode(&corrupt)
	if _, _, err := rt.RestoreBytes(b, health.DefaultPolicy(), nil); err == nil {
		t.Fatal("expected restore to fail")
	} else {
		t.Logf("restore failed as expected: %v", err)
	}

	// 当前实例完全不受影响：会话仍在、绑定版本不变、状态延续。
	if s.CalibVersion() != 1 {
		t.Fatalf("current session calib changed to v%d", s.CalibVersion())
	}
	if rt.DeviceState("dev-1") != health.Healthy {
		t.Fatal("current instance device state altered by failed restore")
	}
	// seq=1 仍在缓冲：seq=2 使缓冲满 -> 按序吐出 seq=1。
	rs, err := s.Ingest(meas("dev-1", "temp-1", 2, 6))
	if err != nil || len(rs) != 1 || rs[0].Key.Seq != 1 {
		t.Fatalf("current session disrupted: rs=%v err=%v", rs, err)
	}
	logReadings(t, "current instance still works after failed restore", rs)
	// 重复仍被识别。
	if _, err := s.Ingest(meas("dev-1", "temp-1", 2, 6)); err != ErrDuplicate {
		t.Fatalf("want ErrDuplicate, got %v", err)
	}
	// 坏字节输入同样不影响实例。
	if _, _, err := rt.RestoreBytes([]byte("not-a-snapshot"), health.Policy{}, nil); !errors.Is(err, snapshot.ErrMalformed) {
		t.Fatalf("want ErrMalformed, got %v", err)
	}
	t.Log("current instance and its sessions unaffected by every failed restore")
}
