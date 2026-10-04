package runtime

import (
	"testing"

	"github.com/xingjianluo/sensor-telemetry-runtime-go/health"
	"github.com/xingjianluo/sensor-telemetry-runtime-go/snapshot"
)

// findSensor 从快照中定位 (设备,传感器) 状态。
func findSensor(t *testing.T, s *snapshot.Snapshot, dev, sensor string) snapshot.SensorState {
	t.Helper()
	for _, d := range s.Devices {
		if d.DeviceID != dev {
			continue
		}
		for _, ss := range d.Sensors {
			if ss.SensorID == sensor {
				return ss
			}
		}
	}
	t.Fatalf("sensor %s/%s not in snapshot", dev, sensor)
	return snapshot.SensorState{}
}

func findDevice(s *snapshot.Snapshot, dev string) snapshot.DeviceState {
	for _, d := range s.Devices {
		if d.DeviceID == dev {
			return d
		}
	}
	return snapshot.DeviceState{}
}

func TestExportSnapshotFreezesState(t *testing.T) {
	store := newStore()
	clk := &fakeClock{t: base}
	rt := NewWithClock(store, health.DefaultPolicy(), 3, clk.now)
	s := rt.NewSession()
	// window=3：seq 1,3,2 后缓冲长度 3 才吐出 seq=1；
	// 只接入两条使 seq=1 已输出、seq=3/2 仍在途。
	if rs, err := s.Ingest(meas("dev-1", "temp-1", 1, 1)); err != nil || len(rs) != 0 {
		t.Fatalf("seq1: rs=%d err=%v", len(rs), err)
	}
	if rs, err := s.Ingest(meas("dev-1", "temp-1", 3, 3)); err != nil || len(rs) != 0 {
		t.Fatalf("seq3: rs=%d err=%v", len(rs), err)
	}
	snap := rt.ExportSnapshot()
	d := findDevice(snap, "dev-1")
	ss := findSensor(t, snap, "dev-1", "temp-1")
	t.Logf("freeze: seen=%v maxSeq=%d pending=%d emittedMaxSeq=%d reboot=%v",
		ss.Seen, ss.MaxSeq, len(ss.Pending), d.EmittedMaxSeq, ss.RebootPending)
	if len(ss.Seen) != 2 || ss.MaxSeq != 3 || !ss.HasSeq {
		t.Fatalf("seen/maxSeq wrong: %+v", ss)
	}
	if len(ss.Pending) != 2 {
		t.Fatalf("pending=%d, want 2 in-flight samples", len(ss.Pending))
	}
	if d.EmittedMaxSeq != 0 {
		t.Fatalf("nothing emitted yet, got emittedMaxSeq=%d", d.EmittedMaxSeq)
	}
	if len(snap.Sessions) != 1 || snap.Sessions[0].CalibVersion != 1 || snap.Sessions[0].ID != s.ID() {
		t.Fatalf("session not frozen: %+v", snap.Sessions)
	}
	if snap.NextSessionID != s.ID()+1 {
		t.Fatalf("nextSessionID=%d, want %d", snap.NextSessionID, s.ID()+1)
	}

	// 再来一条 seq=2：缓冲达窗口 -> seq=1 被吐出；快照不受影响。
	rs, err := s.Ingest(meas("dev-1", "temp-1", 2, 2))
	if err != nil || len(rs) != 1 || rs[0].Key.Seq != 1 {
		t.Fatalf("expect emit seq=1, got rs=%v err=%v", rs, err)
	}
	logReadings(t, "emitted after freeze", rs)
	ss2 := findSensor(t, snap, "dev-1", "temp-1")
	if len(ss2.Pending) != 2 {
		t.Fatal("frozen snapshot mutated by later ingest: mixed-view snapshot")
	}
	if findDevice(rt.ExportSnapshot(), "dev-1").EmittedMaxSeq != 1 {
		t.Fatal("fresh snapshot should show emittedMaxSeq=1")
	}

	// 字节往返必须成功且自描述。
	data, err := snapshot.Encode(snap)
	if err != nil {
		t.Fatal(err)
	}
	back, err := snapshot.Decode(data)
	if err != nil {
		t.Fatalf("round-trip decode: %v", err)
	}
	if back.Window != 3 || back.TakenAt.IsZero() {
		t.Fatalf("header lost: %+v", back)
	}
}

func TestExportSnapshotKeepsHealthAndReboot(t *testing.T) {
	pol := health.DefaultPolicy()
	clk := &fakeClock{t: base}
	rt := NewWithClock(newStore(), pol, 1, clk.now)
	s := rt.NewSession()
	// 制造 Suspect（单次非法输入）。
	bad := meas("dev-1", "temp-1", 1, 1)
	bad.Source = "unknown"
	s.Ingest(bad)
	snap := rt.ExportSnapshot()
	d := findDevice(snap, "dev-1")
	t.Logf("health frozen: state=%s failures=%d successRun=%d",
		health.State(d.Health.State), len(d.Health.Failures), d.Health.SuccessRun)
	if health.State(d.Health.State) != health.Suspect || len(d.Health.Failures) != 1 {
		t.Fatalf("health progress not frozen: %+v", d.Health)
	}

	// 设备重启边界也要冻结。
	rt2 := NewWithClock(newStore(), pol, 1, clk.now)
	s2 := rt2.NewSession()
	s2.Ingest(meas("dev-2", "temp-1", 100, 1))
	// seq=1 回退超窗口 -> 重启：旧缓冲吐出、rebootPending 随首条输出消费。
	rs, _ := s2.Ingest(meas("dev-2", "temp-1", 1, 2))
	if len(rs) != 1 || !rs[0].DeviceRebooted || rs[0].Key.Seq != 1 {
		t.Fatalf("reboot setup wrong: %+v", rs)
	}
	// 制造一个已判定重启但尚未吐出新样本的状态较难（window=1 立即吐出），
	// 因此验证重启后 seen 已重置：旧 seq 不在去重集合。
	snap2 := rt2.ExportSnapshot()
	ss := findSensor(t, snap2, "dev-2", "temp-1")
	t.Logf("post-reboot seen=%v maxSeq=%d hasSeq=%v", ss.Seen, ss.MaxSeq, ss.HasSeq)
	if len(ss.Seen) != 1 || ss.MaxSeq != 1 || !ss.HasSeq {
		t.Fatalf("post-reboot seq space not reflected: seen=%v max=%d", ss.Seen, ss.MaxSeq)
	}
}

// exportForTest 是测试内使用的字节导出辅助。
func exportForTest(s *snapshot.Snapshot) ([]byte, error) { return snapshot.Encode(s) }
