package telemetry

import (
	"errors"
	"fmt"
	"math"
	"sync/atomic"
)

// Calibration 是一份版本化的校准配置（不可变）。
// 归一化公式：normalized = raw*Scale + Offset，结果须落在 [Min, Max]。
type Calibration struct {
	Version uint64
	Scale   float64
	Offset  float64
	Min     float64 // 归一化结果合法下界
	Max     float64 // 归一化结果合法上界
	// MinRawVersion 表示本配置可解释的最早数据版本；
	// 用于“新配置不兼容旧数据时拒绝启用”的判定。
	MinRawVersion uint64
}

// 校准相关错误，供上层用 errors.Is 区分。
var (
	ErrCalibrationInvalid      = errors.New("calibration config invalid")
	ErrCalibrationVersionStale = errors.New("calibration version not increasing")
	ErrCalibrationIncompatible = errors.New("calibration incompatible with existing data")
)

// validate 校验配置自身合法性。
func (c Calibration) validate() error {
	if math.IsNaN(c.Scale) || math.IsInf(c.Scale, 0) ||
		math.IsNaN(c.Offset) || math.IsInf(c.Offset, 0) {
		return fmt.Errorf("%w: scale/offset must be finite", ErrCalibrationInvalid)
	}
	if c.Scale == 0 {
		return fmt.Errorf("%w: scale must be non-zero", ErrCalibrationInvalid)
	}
	if !(c.Min < c.Max) {
		return fmt.Errorf("%w: require Min < Max, got [%v, %v]", ErrCalibrationInvalid, c.Min, c.Max)
	}
	return nil
}

// Normalize 应用校准公式，结果越界返回 ReasonOutOfRange。
func (c Calibration) Normalize(raw float64) (float64, error) {
	v := raw*c.Scale + c.Offset
	if v < c.Min || v > c.Max {
		return 0, &RejectError{
			Reason: ReasonOutOfRange,
			Detail: fmt.Sprintf("normalized value %v outside [%v, %v] (calibration v%d)", v, c.Min, c.Max, c.Version),
		}
	}
	return v, nil
}

// CalibrationStore 支持并发读取与原子切换。
// 配置通过 atomic.Pointer 整体发布，运行中的采样要么读到旧版本、
// 要么读到新版本，绝不会读到半更新配置。
type CalibrationStore struct {
	cur atomic.Pointer[Calibration]
}

// NewCalibrationStore 以初始配置创建存储；初始配置非法时返回 nil。
func NewCalibrationStore(initial Calibration) *CalibrationStore {
	if initial.validate() != nil {
		return nil
	}
	s := &CalibrationStore{}
	s.cur.Store(&initial)
	return s
}

// Snapshot 返回当前生效配置的一致性快照（值拷贝，可安全长期使用）。
func (s *CalibrationStore) Snapshot() Calibration {
	return *s.cur.Load()
}

// Apply 校验并原子切换到新配置；不兼容时返回错误且不改变当前配置。
// dataVersion 是系统中已存在数据的版本：若 next.MinRawVersion > dataVersion，
// 说明新配置无法解释既有数据，拒绝启用。
func (s *CalibrationStore) Apply(next Calibration, dataVersion uint64) error {
	if err := next.validate(); err != nil {
		return err
	}
	cur := s.cur.Load()
	if next.Version <= cur.Version {
		return fmt.Errorf("%w: current v%d, got v%d", ErrCalibrationVersionStale, cur.Version, next.Version)
	}
	if next.MinRawVersion > dataVersion {
		return fmt.Errorf("%w: config v%d requires data >= v%d, existing data is v%d",
			ErrCalibrationIncompatible, next.Version, next.MinRawVersion, dataVersion)
	}
	// 配置对象不可变，整体替换指针即完成原子发布。
	s.cur.Store(&next)
	return nil
}
