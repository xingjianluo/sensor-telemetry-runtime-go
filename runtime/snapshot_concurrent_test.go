package runtime

import (
	"fmt"
	"sync"
	"testing"

	"github.com/xingjianluo/sensor-telemetry-runtime-go/health"
	"github.com/xingjianluo/sensor-telemetry-runtime-go/snapshot"
)

type telemetryReadingAlias struct{ seq uint64 }

// consistentSnapshot 校验快照内部自洽（不存在混合视图的判据）：
//   - 每流 pending 序号必须是 seen 的子集（在途样本必然已被接纳）；
//   - hasSeq 时 maxSeq 必须在 seen 内；
//   - 每设备 emittedMaxSeq 不超过该设备任一流出现过的最大已输出水位；
//   - 重新 Decode 后仍能通过格式校验。
func consistentSnapshot(t *testing.T, s *snapshot.Snapshot, raw []byte) {
	t.Helper()
	back, err := snapshot.Decode(raw)
	if err != nil {
		t.Fatalf("exported bytes must decode: %v", err)
	}
	for _, d := range back.Devices {
		for _, ss := range d.Sensors {
			seen := map[uint64]bool{}
			for _, seq := range ss.Seen {
				if seen[seq] {
					t.Fatalf("%s/%s: duplicated seen seq %d in snapshot", d.DeviceID, ss.SensorID, seq)
				}
				seen[seq] = true
			}
			for _, m := range ss.Pending {
				if !seen[m.Seq] {
					t.Fatalf("%s/%s: pending seq %d not in seen (mixed view)", d.DeviceID, ss.SensorID, m.Seq)
				}
			}
			if ss.HasSeq && !seen[ss.MaxSeq] {
				t.Fatalf("%s/%s: maxSeq %d not in seen", d.DeviceID, ss.SensorID, ss.MaxSeq)
			}
		}
	}
}

func TestConcurrentIngestAndExport(t *testing.T) {
	store := newStore()
	rt := New(store, health.DefaultPolicy(), 5)
	const devices = 6
	const perDevice = 150
	const exporters = 3
	const exportsEach = 200

	sessions := make([]*Session, devices)
	for d := 0; d < devices; d++ {
		sessions[d] = rt.NewSession()
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	var exportCount int64
	var cMu sync.Mutex

	// 导出者：在接入高峰期反复冻结+编码+解码+一致性校验。
	for e := 0; e < exporters; e++ {
		wg.Add(1)
		go func(e int) {
			defer wg.Done()
			for i := 0; i < exportsEach; i++ {
				data, err := rt.ExportSnapshotBytes()
				if err != nil {
					t.Errorf("export failed: %v", err)
					return
				}
				snap, err := snapshot.Decode(data)
				if err != nil {
					t.Errorf("exported snapshot undecodable: %v", err)
					return
				}
				consistentSnapshot(t, snap, data)
				cMu.Lock()
				exportCount++
				cMu.Unlock()
			}
		}(e)
	}

	// 接入者：确定性乱序 + 周期性重复上报。
	emitted := make([][]telemetryReadingAlias, devices)
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
				if err != nil {
					t.Errorf("ingest rejected during export: dev=%s seq=%d: %v", dev, seq, err)
					return
				}
				for _, r := range rs {
					emitted[d] = append(emitted[d], telemetryReadingAlias{seq: r.Key.Seq})
				}
				if seq%15 == 0 {
					if _, err := sessions[d].Ingest(meas(dev, "temp-1", seq, 0)); err != ErrDuplicate {
						t.Errorf("repeat dev=%s seq=%d: want ErrDuplicate, got %v", dev, seq, err)
					}
				}
			}
		}(d)
	}

	// 等接入结束后再停止导出统计（导出者跑到固定次数即止）。
	wg.Wait()
	close(stop)
	_ = stop

	t.Logf("verified %d consistent snapshots under concurrent ingest", exportCount)

	// 最终：每设备输出集合恰好 1..perDevice，无重复无遗漏。
	for d := 0; d < devices; d++ {
		dev := fmt.Sprintf("dev-%d", d)
		rs := sessions[d].Flush(dev)
		seenOut := map[uint64]bool{}
		for _, a := range emitted[d] {
			if seenOut[a.seq] {
				t.Fatalf("%s: duplicate emitted seq %d", dev, a.seq)
			}
			seenOut[a.seq] = true
		}
		for _, r := range rs {
			if seenOut[r.Key.Seq] {
				t.Fatalf("%s: flush duplicate seq %d", dev, r.Key.Seq)
			}
			seenOut[r.Key.Seq] = true
		}
		if len(seenOut) != perDevice {
			t.Fatalf("%s: %d unique outputs, want %d (lost during export?)", dev, len(seenOut), perDevice)
		}
		t.Logf("%s: %d unique ordered outputs, exports caused no loss/reject", dev, len(seenOut))
	}

	// 从“最终时刻”快照恢复一个实例，恢复后状态必须完整且可继续接入。
	data, _ := rt.ExportSnapshotBytes()
	rt2, sess2, err := rt.RestoreBytes(data, health.DefaultPolicy(), nil)
	if err != nil {
		t.Fatalf("restore after concurrent run: %v", err)
	}
	if rt2.DeviceState("dev-0") != health.Healthy {
		t.Fatal("restored device health unexpected")
	}
	for d := 0; d < devices; d++ {
		dev := fmt.Sprintf("dev-%d", d)
		s2 := sess2[sessions[d].ID()]
		// 所有旧序号仍被识别为重复。
		if _, err := s2.Ingest(meas(dev, "temp-1", 1, 1)); err != ErrDuplicate {
			t.Fatalf("%s: old seq accepted after restore: %v", dev, err)
		}
		// 新序号继续进入重排缓冲；填满 window 后最旧样本有序吐出。
		var newOut []uint64
		for k := 1; k <= 5; k++ {
			rs, err := s2.Ingest(meas(dev, "temp-1", uint64(perDevice+k), float64(perDevice+k)))
			if err != nil {
				t.Fatalf("%s: post-restore ingest seq=%d: %v", dev, perDevice+k, err)
			}
			for _, r := range rs {
				newOut = append(newOut, r.Key.Seq)
			}
		}
		if len(newOut) != 1 || newOut[0] != perDevice+1 {
			t.Fatalf("%s: post-restore new stream out-of-order: %v", dev, newOut)
		}
	}
	t.Log("restored instance continues ingest for all devices with dedup intact")
}
