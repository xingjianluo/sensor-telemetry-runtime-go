package calibration

import (
	"errors"
	"sync"
	"testing"
)

func cfg(version uint64, scale float64) *Config {
	return &Config{
		Version: version,
		Sensors: map[string]Params{"temp-1": {Scale: scale, Offset: 0.5}},
	}
}

func TestSnapshotImmutabilityAndAtomicUpdate(t *testing.T) {
	s, err := NewStore(cfg(1, 1.0))
	if err != nil {
		t.Fatal(err)
	}
	before := s.Snapshot()
	if err := s.Update(cfg(2, 2.0), nil); err != nil {
		t.Fatal(err)
	}
	after := s.Snapshot()
	t.Logf("before=%+v after=%+v", before.Sensors["temp-1"], after.Sensors["temp-1"])
	if before.Sensors["temp-1"].Scale != 1.0 {
		t.Fatal("old snapshot mutated by update: readers saw half-updated config")
	}
	if after.Sensors["temp-1"].Scale != 2.0 {
		t.Fatal("new snapshot not installed")
	}
}

func TestUpdateRejectsNonIncreasingVersion(t *testing.T) {
	s, _ := NewStore(cfg(2, 1.0))
	if err := s.Update(cfg(2, 9.0), nil); err == nil {
		t.Fatal("expected rejection of same version")
	}
	if err := s.Update(cfg(1, 9.0), nil); err == nil {
		t.Fatal("expected rejection of older version")
	}
}

func TestUpdateRejectsIncompatibleConfig(t *testing.T) {
	s, _ := NewStore(cfg(1, 1.0))
	accepted := map[string]uint64{"dev-1": 100}
	bad := cfg(2, 1.0)
	bad.MinSeq = map[string]uint64{"dev-1": 200} // 高于已接受的最大序号
	err := s.Update(bad, accepted)
	t.Logf("update incompatible cfg -> err=%v", err)
	if !errors.Is(err, ErrIncompatible) {
		t.Fatalf("expected ErrIncompatible, got %v", err)
	}
	if s.Snapshot().Version != 1 {
		t.Fatal("incompatible config must not be activated")
	}
	// 兼容配置（MinSeq <= acceptedMax+1）应允许启用。
	good := cfg(2, 1.0)
	good.MinSeq = map[string]uint64{"dev-1": 101}
	if err := s.Update(good, accepted); err != nil {
		t.Fatalf("compatible config rejected: %v", err)
	}
}

func TestConcurrentReadWriteConsistency(t *testing.T) {
	s, _ := NewStore(cfg(1, 1.0))
	var wg sync.WaitGroup
	stop := make(chan struct{})
	// 读者：任何时刻读到的快照必须内部一致（Version 与参数匹配）。
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				c := s.Snapshot()
				p := c.Sensors["temp-1"]
				if p.Scale != float64(c.Version) {
					t.Errorf("half-updated config observed: version=%d scale=%v", c.Version, p.Scale)
					return
				}
			}
		}()
	}
	// 写者：单调递增版本，Scale 与 Version 保持一致。
	for v := uint64(2); v <= 200; v++ {
		if err := s.Update(cfg(v, float64(v)), nil); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
	if got := s.Snapshot().Version; got != 200 {
		t.Fatalf("final version = %d, want 200", got)
	}
}
