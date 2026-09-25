// Package telemetry 定义多传感器测量数据的统一表示、校验与归一化。
package telemetry

import (
	"errors"
	"fmt"
	"time"
)

// 可区分的输入拒绝原因。
var (
	ErrMissingDeviceID = errors.New("telemetry: missing device id")
	ErrMissingSensorID = errors.New("telemetry: missing sensor id")
	ErrMissingUnit     = errors.New("telemetry: missing unit")
	ErrUnknownUnit     = errors.New("telemetry: unknown unit")
	ErrUnknownSource   = errors.New("telemetry: unknown source")
	ErrOutOfRange      = errors.New("telemetry: value out of range")
	ErrInvalidTime     = errors.New("telemetry: invalid timestamp")
)

// Source 表示数据来源。
type Source string

const (
	SourceDevice   Source = "device"   // 设备直报
	SourceGateway  Source = "gateway"  // 网关转发
	SourceReplayer Source = "replayer" // 离线回放
)

// Valid 判断来源是否明确。
func (s Source) Valid() bool {
	switch s {
	case SourceDevice, SourceGateway, SourceReplayer:
		return true
	}
	return false
}

// Unit 表示物理单位（仅支持已注册单位，缺失或未知一律拒绝）。
type Unit string

const (
	UnitCelsius   Unit = "C"
	UnitVolt      Unit = "V"
	UnitAmpere    Unit = "A"
	UnitPascal    Unit = "Pa"
	UnitPercent   Unit = "%"
	UnitRevPerMin Unit = "rpm"
)

// validUnits 是已注册单位及其合法取值范围 [min, max]。
var validUnits = map[Unit][2]float64{
	UnitCelsius:   {-273.15, 10000},
	UnitVolt:      {-1e6, 1e6},
	UnitAmpere:    {-1e6, 1e6},
	UnitPascal:    {0, 1e9},
	UnitPercent:   {0, 100},
	UnitRevPerMin: {0, 1e7},
}

// Measurement 是一条接入的原始测量数据。
type Measurement struct {
	DeviceID  string
	SensorID  string
	Seq       uint64    // 设备侧单调序号，用于去重与乱序检测
	Timestamp time.Time // 采样时刻（设备时钟）
	Value     float64
	Unit      Unit
	Source    Source
}

// Validate 校验测量数据，返回可区分的拒绝原因；通过时返回 nil。
func (m Measurement) Validate() error {
	if m.DeviceID == "" {
		return ErrMissingDeviceID
	}
	if m.SensorID == "" {
		return ErrMissingSensorID
	}
	if m.Unit == "" {
		return ErrMissingUnit
	}
	r, ok := validUnits[m.Unit]
	if !ok {
		return fmt.Errorf("%w: %q", ErrUnknownUnit, m.Unit)
	}
	if !m.Source.Valid() {
		return fmt.Errorf("%w: %q", ErrUnknownSource, m.Source)
	}
	if m.Timestamp.IsZero() {
		return ErrInvalidTime
	}
	if m.Value < r[0] || m.Value > r[1] {
		return fmt.Errorf("%w: %v not in [%v, %v] for unit %q",
			ErrOutOfRange, m.Value, r[0], r[1], m.Unit)
	}
	return nil
}

// Key 返回用于去重的键（设备 + 传感器 + 序号）。
func (m Measurement) Key() Key {
	return Key{DeviceID: m.DeviceID, SensorID: m.SensorID, Seq: m.Seq}
}

// Key 是测量去重键。
type Key struct {
	DeviceID string
	SensorID string
	Seq      uint64
}

// Reading 是校准归一化后的输出结果。
type Reading struct {
	Key            Key
	Timestamp      time.Time
	RawValue       float64
	Unit           Unit
	Value          float64 // 校准后的归一化值
	CalibVersion   uint64  // 产生该结果所用的校准配置版本
	DeviceRebooted bool    // 该样本前是否检测到设备重启
}
