// Package telemetry 提供多传感器遥测运行时：统一测量表示、
// 乱序/重复处理、版本化校准与设备健康评估。
package telemetry

import (
	"fmt"
	"math"
	"time"
)

// Unit 表示测量物理单位。
type Unit string

const (
	UnitCelsius Unit = "celsius"
	UnitVolt    Unit = "volt"
	UnitAmpere  Unit = "ampere"
	UnitPercent Unit = "percent"
	UnitPascal  Unit = "pascal"
)

// validUnits 是运行时可接受的单位集合。
var validUnits = map[Unit]bool{
	UnitCelsius: true,
	UnitVolt:    true,
	UnitAmpere:  true,
	UnitPercent: true,
	UnitPascal:  true,
}

// Source 描述数据来源，来源不明确的输入必须被拒绝。
type Source struct {
	Kind string // 例如 "gateway"、"edge-agent"
	ID   string // 来源实例标识
}

// Sample 是设备上报的一条原始测量。
type Sample struct {
	DeviceID    string
	Metric      string
	Unit        Unit
	Value       float64
	Seq         uint64    // 设备侧单调序号，用于乱序/重复判定
	DeviceEpoch uint64    // 设备重启纪元，重启后递增
	Timestamp   time.Time // 设备侧采样时间
	Source      Source
}

// RejectReason 是可区分的输入拒绝原因。
type RejectReason string

const (
	ReasonMissingUnit    RejectReason = "missing_unit"
	ReasonUnknownUnit    RejectReason = "unknown_unit"
	ReasonOutOfRange     RejectReason = "out_of_range"
	ReasonUnknownSource  RejectReason = "unknown_source"
	ReasonMissingDevice  RejectReason = "missing_device"
	ReasonMissingMetric  RejectReason = "missing_metric"
	ReasonInvalidReading RejectReason = "invalid_reading"
	ReasonDeviceIsolated RejectReason = "device_isolated"
)

// RejectError 表示输入被拒绝，Reason 可供上层区分处理。
type RejectError struct {
	Reason RejectReason
	Detail string
}

func (e *RejectError) Error() string {
	return fmt.Sprintf("sample rejected (%s): %s", e.Reason, e.Detail)
}

// Validate 对单条样本做静态校验，不依赖运行时状态。
// 单位缺失、单位未知、范围非法、来源不明确等均返回可区分的 RejectError。
func Validate(s Sample) error {
	if s.DeviceID == "" {
		return &RejectError{Reason: ReasonMissingDevice, Detail: "device id is empty"}
	}
	if s.Metric == "" {
		return &RejectError{Reason: ReasonMissingMetric, Detail: "metric name is empty"}
	}
	if s.Unit == "" {
		return &RejectError{Reason: ReasonMissingUnit, Detail: "unit is empty"}
	}
	if !validUnits[s.Unit] {
		return &RejectError{Reason: ReasonUnknownUnit, Detail: "unit " + string(s.Unit) + " is not supported"}
	}
	if s.Source.Kind == "" || s.Source.ID == "" {
		return &RejectError{Reason: ReasonUnknownSource, Detail: "source kind/id must both be set"}
	}
	if math.IsNaN(s.Value) || math.IsInf(s.Value, 0) {
		return &RejectError{Reason: ReasonInvalidReading, Detail: "value is NaN or Inf"}
	}
	if s.Timestamp.IsZero() {
		return &RejectError{Reason: ReasonInvalidReading, Detail: "timestamp is zero"}
	}
	return nil
}
