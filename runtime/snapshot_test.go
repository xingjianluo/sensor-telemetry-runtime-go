package runtime

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/sensor-telemetry-runtime-go/calibration"
	"github.com/xingjianluo/sensor-telemetry-runtime-go/health"
	"github.com/xingjianluo/sensor-telemetry-runtime-go/telemetry"
)

// ingestLogged 接入一条样本并打印输入、判定依据与归一化结果。
func ingestLogged(t *testing.T, s *Session, m telemetry.Measurement) []telemetry.Reading {
	t.Helper()
	rs, err := s.Ingest(m)
	t.Logf("in: dev=%s sensor=%s seq=%d raw=%v -> emitted=%d err=%v",
		m.DeviceID, m.SensorID, m.Seq, m.Value, len(rs), err)
	logReadings(t, "emit", rs)
	return rs
}

// roundTrip 模拟“导出 -> 落盘 -> 恢复”的完整过程。
func roundTrip(t *testing.T, s *Session, store *calibration.Store, pol health.Policy, now func() time.Time) (*Runtime, *Session) {
	t.Helper()
	snap := s.Snapshot()
	t.Logf("snapshot: devices=%d acceptedMaxSeq=%v calibVersion=%d window=%d",
		len(snap.Devices), snap.AcceptedMaxSeq, snap.Calib.Version, snap.Window)
	data, err := snap.Encode()
	if err != nil {
		t.Fatalf("encode snapshot: %v", err)
	}
	t.Logf("snapshot encoded: %d bytes", len(data))
	decoded, err := DecodeSnapshot(data)
	if err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	rt, rs, err := Restore(decoded, store, pol, now)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	return rt, rs
}

// TestSnapshotRestoreEquivalence 验证核心不变式：相同接入与收尾序列下，
// “中途快照+恢复”与“一直连续运行”的最终输出完全一致
// （样本集合、流内顺序、归一化值、校准版本、重启标记）。
func TestSnapshotRestoreEquivalence(t *testing.T) {
	const window = 2
	// 完整接入序列：乱序 -> 重复 -> 设备重启 -> 重启后继续乱序。
	type step struct {
		m       telemetry.Measurement
		wantErr error
	}
	steps := []step{
		{meas("dev-1", "temp-1", 10, 10), nil},
		{meas("dev-1", "temp-1", 12, 12), nil},
		{meas("dev-1", "temp-1", 11, 11), nil},
		// ---- 快照点（以上为先导段） ----
		{meas("dev-1", "temp-1", 11, 99), ErrDuplicate}, // 恢复前已接纳的序号不得再输出
		{meas("dev-1", "temp-1", 1, 1), nil},            // 序号回退超窗：设备重启
		{meas("dev-1", "temp-1", 2, 2), nil},
		{meas("dev-1", "temp-1", 4, 4), nil},
		{meas("dev-1", "temp-1", 3, 3), nil},
	}
	const split = 3

	run := func(restart bool) []telemetry.Reading {
		store := newStore()
		rt := New(store, health.DefaultPolicy(), window)
		s := rt.NewSession()
		var out []telemetry.Reading
		for i, st := range steps {
			if restart && i == split {
				t.Logf("--- restart: snapshot + restore at step %d ---", i)
				_, s = roundTrip(t, s, store, health.DefaultPolicy(), nil)
			}
			rs, err := s.Ingest(st.m)
			if !errors.Is(err, st.wantErr) {
				t.Fatalf("step %d seq=%d: want err=%v, got %v", i, st.m.Seq, st.wantErr, err)
			}
			out = append(out, rs...)
		}
		out = append(out, s.Flush("dev-1")...)
		return out
	}

	continuous := run(false)
	restored := run(true)
	logReadings(t, "continuous", continuous)
	logReadings(t, "restored", restored)

	if !reflect.DeepEqual(continuous, restored) {
		t.Fatalf("restored output differs from continuous run:\ncontinuous=%+v\nrestored=%+v", continuous, restored)
	}
	// 判定依据：重启标记只出现在新纪元首条（seq=1），归一化 = raw*2+1，校准版本 = 1。
	var reboots int
	for _, r := range restored {
		if r.DeviceRebooted {
			reboots++
			if r.Key.Seq != 1 {
				t.Fatalf("reboot flag on wrong seq: %+v", r)
			}
		}
		if want := r.RawValue*2 + 1; r.Value != want {
			t.Fatalf("seq=%d normalized=%v, want %v", r.Key.Seq, r.Value, want)
		}
		if r.CalibVersion != 1 {
			t.Fatalf("seq=%d calibVersion=%d, want 1", r.Key.Seq, r.CalibVersion)
		}
	}
	if reboots != 1 {
		t.Fatalf("got %d reboot flags, want 1", reboots)
	}
	t.Logf("equivalence verified: %d readings identical, reboots=%d", len(restored), reboots)
}

// TestSnapshotWithPendingBuffer 导出时仍有缓冲未吐出：
// 快照必须区分“已输出”与“等待处理”，恢复后缓冲样本按原顺序吐出。
func TestSnapshotWithPendingBuffer(t *testing.T) {
	store := newStore()
	rt := New(store, health.DefaultPolicy(), 3) // window=3：两条样本不足以触发吐出
	s := rt.NewSession()

	var early []telemetry.Reading
	for _, seq := range []uint64{2, 1} { // 乱序进入缓冲，均未输出
		early = append(early, ingestLogged(t, s, meas("dev-1", "temp-1", seq, float64(seq)))...)
	}
	if len(early) != 0 {
		t.Fatalf("expected nothing emitted before snapshot, got %d", len(early))
	}

	snap := s.Snapshot()
	pending := snap.Devices["dev-1"].Sensors["temp-1"].Pending
	t.Logf("snapshot pending buffer: %d measurements (seen=%v)", len(pending),
		snap.Devices["dev-1"].Sensors["temp-1"].Seen)
	if len(pending) != 2 {
		t.Fatalf("snapshot lost buffered data: pending=%d, want 2", len(pending))
	}

	_, s2 := roundTrip(t, s, store, health.DefaultPolicy(), nil)
	// 恢复后补一条触发窗口吐出，再 Flush 收尾：顺序必须为 1,2,3。
	out := ingestLogged(t, s2, meas("dev-1", "temp-1", 3, 3))
	out = append(out, s2.Flush("dev-1")...)
	logReadings(t, "after-restore", out)
	if len(out) != 3 {
		t.Fatalf("got %d readings, want 3", len(out))
	}
	for i, want := range []uint64{1, 2, 3} {
		if out[i].Key.Seq != want {
			t.Fatalf("out[%d].seq=%d, want %d (buffered order not preserved)", i, out[i].Key.Seq, want)
		}
		if v := out[i].Value; v != float64(want)*2+1 {
			t.Fatalf("seq=%d normalized=%v, want %v", want, v, float64(want)*2+1)
		}
	}
}

// TestSnapshotDuringConcurrentIngest 并发接入期间导出：
// 接入不被拒绝/丢失/错序，快照一致且可恢复出完整状态。
func TestSnapshotDuringConcurrentIngest(t *testing.T) {
	store := newStore()
	rt := New(store, health.DefaultPolicy(), 4)
	const devices = 6
	const perDevice = 150

	sessions := make([]*Session, devices)
	emitted := make([][]telemetry.Reading, devices)
	for d := 0; d < devices; d++ {
		sessions[d] = rt.NewSession()
	}

	var wg sync.WaitGroup
	stopSnap := make(chan struct{})
	var snapErr error
	var snapCount int
	var snapMu sync.Mutex
	// 快照者：接入进行期间持续导出并编码/解码校验。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopSnap:
				return
			default:
			}
			snap := sessions[0].Snapshot()
			data, err := snap.Encode()
			if err != nil {
				snapMu.Lock()
				snapErr = err
				snapMu.Unlock()
				return
			}
			if _, err := DecodeSnapshot(data); err != nil {
				snapMu.Lock()
				snapErr = err
				snapMu.Unlock()
				return
			}
			snapMu.Lock()
			snapCount++
			snapMu.Unlock()
		}
	}()

	// 每设备一个写者：确定性乱序 + 周期性重复上报。
	for d := 0; d < devices; d++ {
		wg.Add(1)
		go func(d int) {
			defer wg.Done()
			dev := fmt.Sprintf("dev-%d", d)
			seqs := make([]uint64, 0, perDevice)
			for i := 1; i <= perDevice; i++ {
				seqs = append(seqs, uint64(i))
			}
			for i := 0; i+1 < len(seqs); i += 2 {
				seqs[i], seqs[i+1] = seqs[i+1], seqs[i]
			}
			for _, seq := range seqs {
				rs, err := sessions[d].Ingest(meas(dev, "temp-1", seq, float64(seq)))
				if err != nil { // 导出期间接入不得被拒绝
					t.Errorf("dev=%s seq=%d rejected during snapshot: %v", dev, seq, err)
				}
				emitted[d] = append(emitted[d], rs...)
				if seq%10 == 0 {
					if _, err := sessions[d].Ingest(meas(dev, "temp-1", seq, 0)); !errors.Is(err, ErrDuplicate) {
						t.Errorf("dev=%s seq=%d: want ErrDuplicate, got %v", dev, seq, err)
					}
				}
			}
		}(d)
	}
	// 写者结束后停止快照者。
	close(stopSnap)
	wg.Wait()

	snapMu.Lock()
	if snapErr != nil {
		t.Fatalf("snapshot during concurrent ingest failed: %v", snapErr)
	}
	t.Logf("snapshots taken during concurrent ingest: %d", snapCount)
	snapMu.Unlock()

	// 最终快照 -> 恢复 -> Flush：与已输出部分合并后，
	// 每设备必须恰好覆盖 1..perDevice，无重复、无遗漏。
	_, rs := roundTrip(t, sessions[0], store, health.DefaultPolicy(), nil)
	for d := 0; d < devices; d++ {
		dev := fmt.Sprintf("dev-%d", d)
		all := append(emitted[d], rs.Flush(dev)...)
		seen := map[uint64]bool{}
		for _, r := range all {
			if seen[r.Key.Seq] {
				t.Fatalf("%s: duplicate output seq=%d after restore", dev, r.Key.Seq)
			}
			seen[r.Key.Seq] = true
		}
		if len(seen) != perDevice {
			t.Fatalf("%s: got %d unique readings, want %d (lost data across restore)", dev, len(seen), perDevice)
		}
		t.Logf("%s: %d unique readings after restore, acceptedMaxSeq=%d",
			dev, len(seen), rs.rt.AcceptedMaxSeq()[dev])
	}
}

// TestRestorePreservesRebootBoundary 设备重启边界延续：
// 快照时重启标记尚未输出，恢复后新纪元首条仍须带重启标记，
// 旧纪元序号不得重复输出。
func TestRestorePreservesRebootBoundary(t *testing.T) {
	store := newStore()
	rt := New(store, health.DefaultPolicy(), 2)
	s := rt.NewSession()

	ingestLogged(t, s, meas("dev-1", "temp-1", 100, 1))
	ingestLogged(t, s, meas("dev-1", "temp-1", 101, 2))      // 触发吐出 seq=100
	out := ingestLogged(t, s, meas("dev-1", "temp-1", 1, 3)) // 回退超窗：重启，吐出旧纪元 seq=101
	if len(out) != 1 || out[0].Key.Seq != 101 || out[0].DeviceRebooted {
		t.Fatalf("reboot drain wrong: %+v", out)
	}
	t.Logf("reboot detected; rebootPending held in buffer, taking snapshot")

	_, s2 := roundTrip(t, s, store, health.DefaultPolicy(), nil)

	// 新纪元首条输出必须带重启标记。
	out = ingestLogged(t, s2, meas("dev-1", "temp-1", 2, 4))
	if len(out) != 1 || out[0].Key.Seq != 1 || !out[0].DeviceRebooted {
		t.Fatalf("reboot flag lost across restore: %+v", out)
	}
	// 新纪元已接纳的序号仍判重，不得当成新样本重复输出。
	if _, err := s2.Ingest(meas("dev-1", "temp-1", 1, 99)); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("new-epoch dup after restore: want ErrDuplicate, got %v", err)
	}
	// 旧纪元缓冲在重启时已吐出，恢复后 Flush 不得再次输出。
	if rest := s2.Flush("dev-1"); len(rest) != 1 || rest[0].Key.Seq != 2 {
		t.Fatalf("old-epoch data re-emitted after restore: %+v", rest)
	}
	t.Logf("reboot boundary preserved: seq=1 emitted with DeviceRebooted=true, no old-epoch replay")
}

// TestRestorePreservesIsolationAndRecovery 故障隔离与恢复进度延续：
// 恢复后健康状态不重置，恢复期连续成功计数不清零。
func TestRestorePreservesIsolationAndRecovery(t *testing.T) {
	clk := &fakeClock{t: base}
	pol := health.DefaultPolicy()
	store := newStore()
	rt := NewWithClock(store, pol, 1, clk.now)
	s := rt.NewSession()

	// 制造失败达到阈值，隔离 dev-bad。
	bad := meas("dev-bad", "temp-1", 1, 1)
	bad.Unit = ""
	for i := 0; i < pol.IsolateThreshold; i++ {
		s.Ingest(bad)
	}
	t.Logf("dev-bad state before snapshot: %s", rt.DeviceState("dev-bad"))
	if rt.DeviceState("dev-bad") != health.Isolated {
		t.Fatalf("state=%s, want Isolated", rt.DeviceState("dev-bad"))
	}

	rt2, s2 := roundTrip(t, s, store, pol, clk.now)
	if st := rt2.DeviceState("dev-bad"); st != health.Isolated {
		t.Fatalf("isolation reset by restore: state=%s, want Isolated", st)
	}
	// 恢复后的实例在静默期内仍拒绝该设备。
	if _, err := s2.Ingest(meas("dev-bad", "temp-1", 2, 1)); !errors.Is(err, ErrDeviceIsolated) {
		t.Fatalf("want ErrDeviceIsolated after restore, got %v", err)
	}

	// 静默期满，先积累 1 次恢复进度（RecoverAfter=2），再快照恢复。
	clk.advance(pol.Quarantine + time.Second)
	if _, err := s2.Ingest(meas("dev-bad", "temp-1", 10, 1)); err != nil {
		t.Fatalf("recovery sample rejected: %v", err)
	}
	t.Logf("recovery progress 1/%d, state=%s; snapshot again", pol.RecoverAfter, rt2.DeviceState("dev-bad"))
	if rt2.DeviceState("dev-bad") != health.Recovering {
		t.Fatalf("state=%s, want Recovering", rt2.DeviceState("dev-bad"))
	}

	rt3, s3 := roundTrip(t, s2, store, pol, clk.now)
	if st := rt3.DeviceState("dev-bad"); st != health.Recovering {
		t.Fatalf("recovery progress reset by restore: state=%s, want Recovering", st)
	}
	// 再 1 次成功即应恢复健康（进度延续，未清零）。
	if _, err := s3.Ingest(meas("dev-bad", "temp-1", 11, 1)); err != nil {
		t.Fatalf("recovery sample rejected: %v", err)
	}
	if st := rt3.DeviceState("dev-bad"); st != health.Healthy {
		t.Fatalf("state=%s, want Healthy (recovery progress not preserved)", st)
	}
	t.Logf("isolation & recovery progress preserved across two restores")
}

// TestRestorePreservesCalibVersion 校准版本延续：
// 会话绑定的校准快照随状态快照一起恢复，
// 即使 Store 已升级，恢复出的会话仍按原版本归一化。
func TestRestorePreservesCalibVersion(t *testing.T) {
	store := newStore() // v1: value = raw*2+1
	rt := New(store, health.DefaultPolicy(), 1)
	s := rt.NewSession() // 绑定 v1

	if err := store.Update(&calibration.Config{
		Version: 2,
		Sensors: map[string]calibration.Params{"temp-1": {Scale: 10, Offset: 0}},
	}, rt.AcceptedMaxSeq()); err != nil {
		t.Fatal(err)
	}
	t.Logf("store upgraded to v2 (raw*10); session still bound to v1")

	_, s2 := roundTrip(t, s, store, health.DefaultPolicy(), nil)
	if v := s2.CalibVersion(); v != 1 {
		t.Fatalf("restored session calib version=%d, want 1", v)
	}
	out := ingestLogged(t, s2, meas("dev-1", "temp-1", 1, 5))
	if len(out) != 1 || out[0].Value != 11 || out[0].CalibVersion != 1 {
		t.Fatalf("restored session used wrong calibration: %+v", out)
	}
	t.Logf("restored session: raw=5 -> normalized=%v (calib v%d), calibration continued",
		out[0].Value, out[0].CalibVersion)
}

// TestRestoreRejectsIncompatible 格式或版本不兼容明确拒绝，
// 且拒绝失败不影响当前实例与已开始的会话。
func TestRestoreRejectsIncompatible(t *testing.T) {
	store := newStore()
	rt := New(store, health.DefaultPolicy(), 2)
	s := rt.NewSession()
	ingestLogged(t, s, meas("dev-1", "temp-1", 1, 5))
	good := s.Snapshot()

	cases := map[string]*Snapshot{
		"nil snapshot": nil,
		"bad format version": func() *Snapshot {
			c := *good
			c.FormatVersion = SnapshotFormatVersion + 1
			return &c
		}(),
		"zero window": func() *Snapshot {
			c := *good
			c.Window = 0
			return &c
		}(),
		"missing calibration": func() *Snapshot {
			c := *good
			c.Calib = nil
			return &c
		}(),
		"pending not in seen": func() *Snapshot {
			c := *good
			d := c.Devices["dev-1"]
			ss := d.Sensors["temp-1"]
			ss.Pending = []telemetry.Measurement{meas("dev-1", "temp-1", 999, 1)}
			d.Sensors["temp-1"] = ss
			c.Devices["dev-1"] = d
			return &c
		}(),
	}
	for name, snap := range cases {
		_, _, err := Restore(snap, store, health.DefaultPolicy(), nil)
		t.Logf("restore case %q -> err=%v", name, err)
		if !errors.Is(err, ErrSnapshotIncompatible) {
			t.Fatalf("case %q: want ErrSnapshotIncompatible, got %v", name, err)
		}
	}
	// 损坏的字节串也要明确拒绝。
	if _, err := DecodeSnapshot([]byte("{not json")); !errors.Is(err, ErrSnapshotIncompatible) {
		t.Fatalf("corrupt bytes: want ErrSnapshotIncompatible, got %v", err)
	}

	// 失败的恢复不影响当前实例：原会话继续正常接入与判定。
	// window=2：seq=2 到达后缓冲满，吐出 seq=1（raw=5 -> 11）。
	out := ingestLogged(t, s, meas("dev-1", "temp-1", 2, 6))
	if len(out) != 1 || out[0].Key.Seq != 1 || out[0].Value != 11 {
		t.Fatalf("running instance affected by failed restore: %+v", out)
	}
	if rest := s.Flush("dev-1"); len(rest) != 1 || rest[0].Key.Seq != 2 || rest[0].Value != 13 {
		t.Fatalf("flush after failed restores wrong: %+v", rest)
	}
	if _, err := s.Ingest(meas("dev-1", "temp-1", 1, 99)); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("dedup broken after failed restore: %v", err)
	}
	t.Logf("running instance unaffected by %d failed restore attempts", len(cases)+1)
}
