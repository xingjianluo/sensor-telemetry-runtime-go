package telemetry

import (
	"fmt"
	"sync"
	"testing"
)

// 多设备并发接入 + 并发校准切换：验证数据不丢不重、状态一致。
func TestConcurrentIngestConsistency(t *testing.T) {
	store := NewCalibrationStore(Calibration{Version: 1, Scale: 1, Offset: 0, Min: -1e9, Max: 1e9})
	rt := NewRuntime(store, testPolicy())

	const devices = 8
	const perDevice = 500

	var wg sync.WaitGroup
	// 每个设备一个写者，顺序发送 seq 1..N，并夹带重复与乱序
	for d := 0; d < devices; d++ {
		wg.Add(1)
		go func(d int) {
			defer wg.Done()
			dev := fmt.Sprintf("dev-%d", d)
			for i := 1; i <= perDevice; i++ {
				rt.Ingest(sample(dev, 1, uint64(i), float64(i), i))
				if i%10 == 0 {
					rt.Ingest(sample(dev, 1, uint64(i), float64(i), i)) // 重复
				}
				if i%25 == 0 && i > 1 {
					rt.Ingest(sample(dev, 1, uint64(i-1), float64(i-1), i-1)) // 乱序（已见，去重）
				}
			}
		}(d)
	}
	// 并发校准升级（兼容），切换不应造成撕裂或 panic
	wg.Add(1)
	go func() {
		defer wg.Done()
		for v := uint64(2); v <= 20; v++ {
			_ = rt.SwitchCalibration(Calibration{
				Version: v, Scale: 1, Offset: 0, Min: -1e9, Max: 1e9,
			}, 0)
		}
	}()
	// 并发读取健康与结果
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 2000; i++ {
			rt.Health(fmt.Sprintf("dev-%d", i%devices))
			rt.Readings(fmt.Sprintf("dev-%d", i%devices))
		}
	}()
	wg.Wait()

	// 一致性断言：每台设备恰好 perDevice 条、seq 1..N 各一次
	for d := 0; d < devices; d++ {
		dev := fmt.Sprintf("dev-%d", d)
		rds := rt.Readings(dev)
		if len(rds) != perDevice {
			t.Fatalf("%s: got %d readings, want %d (data lost or duplicated)", dev, len(rds), perDevice)
		}
		for i, rd := range rds {
			if rd.Seq != uint64(i+1) {
				t.Fatalf("%s: readings[%d].Seq=%d, want %d", dev, i, rd.Seq, i+1)
			}
			if rd.Normalized != rd.Raw { // Scale=1 Offset=0
				t.Fatalf("%s: seq=%d normalized=%v raw=%v inconsistent", dev, rd.Seq, rd.Normalized, rd.Raw)
			}
		}
	}
	t.Logf("verified %d devices x %d readings: no loss, no duplication, normalization consistent", devices, perDevice)
}
