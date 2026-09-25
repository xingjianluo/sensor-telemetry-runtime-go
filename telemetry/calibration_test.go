package telemetry

import (
	"errors"
	"sync"
	"testing"
)

func baseCal() Calibration {
	return Calibration{Version: 1, Scale: 1, Offset: 0, Min: -100, Max: 100, MinRawVersion: 0}
}

func TestNormalizeAndRange(t *testing.T) {
	c := Calibration{Version: 1, Scale: 0.1, Offset: 2, Min: 0, Max: 10}
	v, err := c.Normalize(50)
	t.Logf("input raw=50 cal=%+v -> normalized=%v err=%v", c, v, err)
	if err != nil || v != 7 {
		t.Fatalf("normalized=%v err=%v, want 7", v, err)
	}
	if _, err := c.Normalize(500); err == nil {
		t.Fatal("expected out-of-range reject")
	} else {
		re := err.(*RejectError)
		t.Logf("input raw=500 -> reject reason=%s detail=%s", re.Reason, re.Detail)
		if re.Reason != ReasonOutOfRange {
			t.Fatalf("reason=%s, want %s", re.Reason, ReasonOutOfRange)
		}
	}
}

func TestApplyVersioningAndCompatibility(t *testing.T) {
	s := NewCalibrationStore(baseCal())

	// 版本不回退
	stale := baseCal()
	if err := s.Apply(stale, 0); !errors.Is(err, ErrCalibrationVersionStale) {
		t.Fatalf("expected stale version error, got %v", err)
	}
	t.Logf("apply same version -> rejected: %v", ErrCalibrationVersionStale)

	// 新配置不兼容既有数据（数据版本 1 < MinRawVersion 2）必须拒绝启用
	incompat := Calibration{Version: 2, Scale: 2, Offset: 0, Min: -100, Max: 100, MinRawVersion: 2}
	if err := s.Apply(incompat, 1); !errors.Is(err, ErrCalibrationIncompatible) {
		t.Fatalf("expected incompatible error, got %v", err)
	}
	t.Logf("apply v2 requiring data>=v2 with data=v1 -> rejected: %v", ErrCalibrationIncompatible)
	if got := s.Snapshot().Version; got != 1 {
		t.Fatalf("config changed after rejection: version=%d", got)
	}

	// 兼容升级成功
	ok := Calibration{Version: 2, Scale: 2, Offset: 0, Min: -100, Max: 100, MinRawVersion: 1}
	if err := s.Apply(ok, 1); err != nil {
		t.Fatalf("apply compatible v2: %v", err)
	}
	t.Logf("apply v2 compatible with data=v1 -> accepted, snapshot=%+v", s.Snapshot())
	if got := s.Snapshot().Version; got != 2 {
		t.Fatalf("version=%d, want 2", got)
	}
}

// 并发读写：读者只能看到完整旧配置或完整新配置，绝不看到半更新状态。
func TestConcurrentSnapshotConsistency(t *testing.T) {
	s := NewCalibrationStore(baseCal())
	var wg sync.WaitGroup
	var writerWG sync.WaitGroup
	stop := make(chan struct{})

	// 写者：不断切换到新版本（Scale 与 Version 同步增长）
	writerWG.Add(1)
	go func() {
		defer writerWG.Done()
		for v := uint64(2); ; v++ {
			select {
			case <-stop:
				return
			default:
			}
			f := float64(v)
			_ = s.Apply(Calibration{Version: v, Scale: f, Offset: f, Min: -1e9, Max: 1e9}, 0)
		}
	}()

	// 读者：校验快照内部一致性（Scale==Offset==float64(Version)）
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20000; j++ {
				c := s.Snapshot()
				if c.Version > 1 && (c.Scale != float64(c.Version) || c.Offset != float64(c.Version)) {
					t.Errorf("torn snapshot: %+v", c)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(stop)
	writerWG.Wait()
	t.Logf("concurrent read/write finished, final snapshot=%+v", s.Snapshot())
}
