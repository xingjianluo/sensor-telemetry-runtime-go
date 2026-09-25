package runtime

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/sensor-telemetry-runtime-go/calibration"
	"github.com/xingjianluo/sensor-telemetry-runtime-go/health"
	"github.com/xingjianluo/sensor-telemetry-runtime-go/telemetry"
)

var base = time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

func meas(dev, sensor string, seq uint64, v float64) telemetry.Measurement {
	return telemetry.Measurement{
		DeviceID:  dev,
		SensorID:  sensor,
		Seq:       seq,
		Timestamp: base.Add(time.Duration(seq) * time.Second),
		Value:     v,
		Unit:      telemetry.UnitCelsius,
		Source:    telemetry.SourceDevice,
	}
}

func newStore() *calibration.Store {
	s, err := calibration.NewStore(&calibration.Config{
		Version: 1,
		Sensors: map[string]calibration.Params{"temp-1": {Scale: 2, Offset: 1}},
	})
	if err != nil {
		panic(err)
	}
	return s
}

func logReadings(t *testing.T, why string, rs []telemetry.Reading) {
	t.Helper()
	for _, r := range rs {
		t.Logf("[%s] out: dev=%s sensor=%s seq=%d raw=%v -> normalized=%v (calib v%d, reboot=%v)",
			why, r.Key.DeviceID, r.Key.SensorID, r.Key.Seq, r.RawValue, r.Value, r.CalibVersion, r.DeviceRebooted)
	}
}

func TestOutOfOrderReordering(t *testing.T) {
	rt := New(newStore(), health.DefaultPolicy(), 2)
	s := rt.NewSession()
	var all []telemetry.Reading
	for _, seq := range []uint64{1, 3, 2, 5, 4} {
		m := meas("dev-1", "temp-1", seq, float64(seq))
		rs, err := s.Ingest(m)
		t.Logf("in: seq=%d -> emitted=%d err=%v", seq, len(rs), err)
		if err != nil {
			t.Fatalf("seq=%d rejected: %v", seq, err)
		}
		all = append(all, rs...)
	}
	all = append(all, s.Flush("dev-1")...)
	logReadings(t, "reordered", all)
	if len(all) != 5 {
		t.Fatalf("lost data: got %d readings, want 5", len(all))
	}
	if !sort.SliceIsSorted(all, func(i, j int) bool { return all[i].Key.Seq < all[j].Key.Seq }) {
		t.Fatal("output not in seq order")
	}
	// 归一化值 = raw*2+1。
	for _, r := range all {
		if want := r.RawValue*2 + 1; r.Value != want {
			t.Fatalf("seq=%d normalized=%v, want %v", r.Key.Seq, r.Value, want)
		}
	}
}

func TestDuplicatesRejected(t *testing.T) {
	rt := New(newStore(), health.DefaultPolicy(), 1)
	s := rt.NewSession()
	if _, err := s.Ingest(meas("dev-1", "temp-1", 7, 10)); err != nil {
		t.Fatal(err)
	}
	_, err := s.Ingest(meas("dev-1", "temp-1", 7, 99)) // 同 seq 不同值：重复上报
	t.Logf("duplicate seq=7 -> err=%v", err)
	if !errors.Is(err, ErrDuplicate) {
		t.Fatalf("want ErrDuplicate, got %v", err)
	}
	// 重复不得覆盖已接受数据。
	rs := s.Flush("dev-1")
	for _, r := range rs {
		if r.Key.Seq == 7 && r.RawValue != 10 {
			t.Fatal("duplicate overwrote accepted data")
		}
	}
}

func TestDeviceRebootDetected(t *testing.T) {
	rt := New(newStore(), health.DefaultPolicy(), 1)
	s := rt.NewSession()
	s.Ingest(meas("dev-1", "temp-1", 100, 1))
	s.Ingest(meas("dev-1", "temp-1", 101, 2))
	// 序号回退且非重复 -> 设备重启。
	rs, err := s.Ingest(meas("dev-1", "temp-1", 1, 3))
	t.Logf("reboot sample seq=1 after maxSeq=101 -> reboot=%v err=%v", len(rs) > 0 && rs[0].DeviceRebooted, err)
	if err != nil {
		t.Fatal(err)
	}
	if len(rs) != 1 || !rs[0].DeviceRebooted {
		t.Fatal("reboot not flagged")
	}
	// 重启后旧序号空间可重新使用，不判重、不丢失。
	if _, err := s.Ingest(meas("dev-1", "temp-1", 2, 4)); err != nil {
		t.Fatalf("post-reboot seq rejected: %v", err)
	}
}

func TestSessionCalibIsolation(t *testing.T) {
	store := newStore()
	rt := New(store, health.DefaultPolicy(), 1)
	s1 := rt.NewSession() // 绑定 v1: value = raw*2+1

	if err := store.Update(&calibration.Config{
		Version: 2,
		Sensors: map[string]calibration.Params{"temp-1": {Scale: 10, Offset: 0}},
	}, rt.AcceptedMaxSeq()); err != nil {
		t.Fatal(err)
	}
	s2 := rt.NewSession() // 绑定 v2: value = raw*10

	m1 := meas("dev-1", "temp-1", 1, 5)
	m2 := meas("dev-2", "temp-1", 1, 5)
	r1, _ := s1.Ingest(m1)
	r2, _ := s2.Ingest(m2)
	t.Logf("same raw=5: session v1 -> %v; session v2 -> %v", r1[0].Value, r2[0].Value)
	if r1[0].Value != 11 || r1[0].CalibVersion != 1 {
		t.Fatalf("session v1 conclusion not reproducible: %+v", r1[0])
	}
	if r2[0].Value != 50 || r2[0].CalibVersion != 2 {
		t.Fatalf("session v2 conclusion wrong: %+v", r2[0])
	}
	// 切换后旧会话仍按旧配置判定（可复现）。
	r1b, _ := s1.Ingest(meas("dev-1", "temp-1", 2, 5))
	if r1b[0].Value != 11 || r1b[0].CalibVersion != 1 {
		t.Fatal("old session affected by config switch")
	}
}

func TestIsolationDoesNotBlockOthers(t *testing.T) {
	clk := &fakeClock{t: base}
	pol := health.DefaultPolicy()
	rt := NewWithClock(newStore(), pol, 1, clk.now)
	s := rt.NewSession()

	// 用非法输入制造 dev-bad 的失败，达到阈值后隔离。
	bad := meas("dev-bad", "temp-1", 1, 1)
	bad.Unit = "" // 触发 ErrMissingUnit
	for i := 0; i < pol.IsolateThreshold; i++ {
		s.Ingest(bad)
		t.Logf("dev-bad invalid input #%d -> state=%s", i+1, rt.DeviceState("dev-bad"))
	}
	if rt.DeviceState("dev-bad") != health.Isolated {
		t.Fatalf("dev-bad state=%s, want Isolated", rt.DeviceState("dev-bad"))
	}
	// 隔离期间样本被丢弃，但给出可区分原因。
	_, err := s.Ingest(meas("dev-bad", "temp-1", 2, 1))
	if !errors.Is(err, ErrDeviceIsolated) {
		t.Fatalf("want ErrDeviceIsolated, got %v", err)
	}
	// 其他设备不受影响。
	rs, err := s.Ingest(meas("dev-good", "temp-1", 1, 20))
	if err != nil || len(rs) != 1 {
		t.Fatalf("healthy device blocked: rs=%v err=%v", rs, err)
	}
	logReadings(t, "dev-good unaffected", rs)

	// 静默期满后，真实故障恢复 -> 按规则重新纳入。
	clk.advance(pol.Quarantine + time.Second)
	for i := 0; i < pol.RecoverAfter; i++ {
		if _, err := s.Ingest(meas("dev-bad", "temp-1", uint64(10+i), 1)); err != nil {
			t.Fatalf("recovery sample #%d rejected: %v", i, err)
		}
		t.Logf("dev-bad recovery sample #%d -> state=%s", i+1, rt.DeviceState("dev-bad"))
	}
	if rt.DeviceState("dev-bad") != health.Healthy {
		t.Fatalf("state=%s, want Healthy after recovery", rt.DeviceState("dev-bad"))
	}
}

func TestTransientFaultNotIsolated(t *testing.T) {
	rt := New(newStore(), health.DefaultPolicy(), 1)
	s := rt.NewSession()
	bad := meas("dev-1", "temp-1", 1, 1)
	bad.Source = "unknown" // 单次非法输入 = 瞬时故障
	s.Ingest(bad)
	t.Logf("after single bad input: state=%s", rt.DeviceState("dev-1"))
	if rt.DeviceState("dev-1") == health.Isolated {
		t.Fatal("transient fault misjudged as permanent")
	}
	// 设备继续正常输出。
	if _, err := s.Ingest(meas("dev-1", "temp-1", 2, 5)); err != nil {
		t.Fatalf("device blocked after transient fault: %v", err)
	}
}

func TestConcurrentIngestConsistency(t *testing.T) {
	store := newStore()
	rt := New(store, health.DefaultPolicy(), 4)
	const devices = 8
	const perDevice = 200

	var wg sync.WaitGroup
	// 配置写者：持续升级版本。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for v := uint64(2); v <= 50; v++ {
			store.Update(&calibration.Config{
				Version: v,
				Sensors: map[string]calibration.Params{"temp-1": {Scale: 2, Offset: 1}},
			}, rt.AcceptedMaxSeq())
		}
	}()

	// 每设备一个写者：乱序 + 10% 重复上报。
	sessions := make([]*Session, devices)
	emitted := make([][]telemetry.Reading, devices) // 每写者独占，无需加锁
	for d := 0; d < devices; d++ {
		sessions[d] = rt.NewSession()
	}
	for d := 0; d < devices; d++ {
		wg.Add(1)
		go func(d int) {
			defer wg.Done()
			dev := fmt.Sprintf("dev-%d", d)
			seqs := make([]uint64, 0, perDevice)
			for i := 1; i <= perDevice; i++ {
				seqs = append(seqs, uint64(i))
			}
			// 确定性乱序：逆序对交换。
			for i := 0; i+1 < len(seqs); i += 2 {
				seqs[i], seqs[i+1] = seqs[i+1], seqs[i]
			}
			for _, seq := range seqs {
				rs, err := sessions[d].Ingest(meas(dev, "temp-1", seq, float64(seq)))
				if err != nil {
					t.Errorf("dev=%s seq=%d: %v", dev, seq, err)
				}
				emitted[d] = append(emitted[d], rs...)
				if seq%10 == 0 { // 重复上报
					if _, err := sessions[d].Ingest(meas(dev, "temp-1", seq, 0)); !errors.Is(err, ErrDuplicate) {
						t.Errorf("dev=%s seq=%d: want ErrDuplicate, got %v", dev, seq, err)
					}
				}
			}
		}(d)
	}
	wg.Wait()

	// 验证：每设备恰好 perDevice 条唯一读数，seq 覆盖 1..perDevice。
	for d := 0; d < devices; d++ {
		dev := fmt.Sprintf("dev-%d", d)
		rs := append(emitted[d], sessions[d].Flush(dev)...)
		seen := map[uint64]bool{}
		for _, r := range rs {
			if seen[r.Key.Seq] {
				t.Fatalf("%s: duplicate output seq=%d", dev, r.Key.Seq)
			}
			seen[r.Key.Seq] = true
		}
		if len(seen) != perDevice {
			t.Fatalf("%s: got %d unique readings, want %d", dev, len(seen), perDevice)
		}
		t.Logf("%s: %d unique readings, maxSeq accepted=%d", dev, len(seen), rt.AcceptedMaxSeq()[dev])
	}
}

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }
