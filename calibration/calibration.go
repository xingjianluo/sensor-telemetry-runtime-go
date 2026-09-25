// Package calibration 提供版本化校准参数的并发安全存取。
//
// 并发语义：Store 内部使用 atomic.Pointer 保存当前配置快照，
// 读者通过 Snapshot() 无锁获取完整快照，写者通过 Update() 整体替换，
// 因此运行中的采样绝不会读到半更新配置。
package calibration

import (
	"errors"
	"fmt"
	"sync/atomic"
)

// ErrIncompatible 表示新配置与已接受的数据不兼容，拒绝启用。
var ErrIncompatible = errors.New("calibration: new config incompatible with accepted data")

// ErrNoSensor 表示配置中缺少某传感器的校准参数。
var ErrNoSensor = errors.New("calibration: no calibration for sensor")

// Params 是单个传感器的线性校准参数：value = raw*Scale + Offset。
type Params struct {
	Scale  float64
	Offset float64
}

// Apply 对原始值应用校准。
func (p Params) Apply(raw float64) float64 { return raw*p.Scale + p.Offset }

// Config 是一份不可变的校准配置快照。
type Config struct {
	Version uint64
	// Sensors 以 sensorID 为键的校准参数。
	Sensors map[string]Params
	// MinSeq 是该配置版本允许接受的最低设备序号（每设备）。
	// 用于表达“新配置不兼容旧数据”：若新配置 MinSeq 高于
	// 已接受数据的最大序号，则该配置拒绝启用。
	MinSeq map[string]uint64
}

// ParamsFor 返回指定传感器的校准参数。
func (c *Config) ParamsFor(sensorID string) (Params, error) {
	p, ok := c.Sensors[sensorID]
	if !ok {
		return Params{}, fmt.Errorf("%w: %q", ErrNoSensor, sensorID)
	}
	return p, nil
}

// Store 是校准配置的并发安全仓库。
type Store struct {
	cur atomic.Pointer[Config]
}

// NewStore 创建仓库并安装初始配置（Version 必须 >= 1）。
func NewStore(initial *Config) (*Store, error) {
	if initial == nil || initial.Version == 0 {
		return nil, errors.New("calibration: initial config must have version >= 1")
	}
	s := &Store{}
	s.cur.Store(initial)
	return s, nil
}

// Snapshot 返回当前配置快照；快照不可变，调用方可安全长期持有。
func (s *Store) Snapshot() *Config { return s.cur.Load() }

// Update 校验兼容性后原子切换到新配置。
//
// 兼容规则：新配置版本必须严格递增；且对每台设备，
// 新配置的 MinSeq 不得高于已接受数据的最大序号（acceptedMaxSeq），
// 否则新配置会拒绝已入库的数据，视为不兼容，返回 ErrIncompatible。
func (s *Store) Update(next *Config, acceptedMaxSeq map[string]uint64) error {
	if next == nil {
		return errors.New("calibration: nil config")
	}
	cur := s.cur.Load()
	if next.Version <= cur.Version {
		return fmt.Errorf("calibration: version must increase: %d <= %d",
			next.Version, cur.Version)
	}
	for dev, minSeq := range next.MinSeq {
		if maxSeq, ok := acceptedMaxSeq[dev]; ok && minSeq > maxSeq+1 {
			return fmt.Errorf("%w: device %q requires seq >= %d but accepted max is %d",
				ErrIncompatible, dev, minSeq, maxSeq)
		}
	}
	s.cur.Store(next)
	return nil
}
