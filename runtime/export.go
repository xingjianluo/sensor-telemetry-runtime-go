package runtime

import (
	"sort"

	"github.com/xingjianluo/sensor-telemetry-runtime-go/calibration"
	"github.com/xingjianluo/sensor-telemetry-runtime-go/snapshot"
	"github.com/xingjianluo/sensor-telemetry-runtime-go/telemetry"
	"time"
)

// ExportSnapshot 在运行时互斥锁保护下冻结当前全部运行状态，
// 生成一份一致快照供上层保存。
//
// 一致性保证：导出全程持有 Runtime 的互斥锁（与 Ingest/Flush/
// 配置切换所用的是同一把锁），因此并发接入不会看到“部分旧、部分新”
// 的混合视图；接入请求只会在锁外排队等待，不会被拒绝、丢失或错序。
//
// 已纳入输出与仍在等待处理的数据通过两个无歧义的集合区分：
//   - 已输出：每设备 EmittedMaxSeq（== acceptedMaxSeq）；
//   - 在途：每传感器 Pending（已接纳但尚未吐出的样本）。
//
// 快照持有的是深拷贝，返回后上层对字节的任何处理都不会影响运行时。
func (r *Runtime) ExportSnapshot() *snapshot.Snapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.exportLocked()
}

// ExportSnapshotBytes 是 Encode(ExportSnapshot()) 的便捷形式。
func (r *Runtime) ExportSnapshotBytes() ([]byte, error) {
	return snapshot.Encode(r.ExportSnapshot())
}

func (r *Runtime) exportLocked() *snapshot.Snapshot {
	now := time.Now
	if r.now != nil {
		now = r.now
	}
	s := &snapshot.Snapshot{
		Magic:   snapshot.FormatMagic,
		Format:  snapshot.FormatVersion,
		TakenAt: now(),
		Window:  r.window,
		Policy: snapshot.Policy{
			WindowNS:         int64(r.policy.Window),
			IsolateThreshold: r.policy.IsolateThreshold,
			QuarantineNS:     int64(r.policy.Quarantine),
			RecoverAfter:     r.policy.RecoverAfter,
		},
		CurrentCalibVersion: r.calib.Snapshot().Version,
		NextSessionID:       r.nextSessionID,
	}

	// 校准历史按版本排序输出（确定性）。
	versions := make([]uint64, 0, len(r.calibHistory))
	for v := range r.calibHistory {
		versions = append(versions, v)
	}
	sort.Slice(versions, func(i, j int) bool { return versions[i] < versions[j] })
	s.Calibs = make([]snapshot.CalibConfig, 0, len(versions))
	for _, v := range versions {
		s.Calibs = append(s.Calibs, exportConfig(r.calibHistory[v]))
	}

	// 会话注册表按 ID 排序。
	ids := make([]uint64, 0, len(r.sessions))
	for id := range r.sessions {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	s.Sessions = make([]snapshot.Session, 0, len(ids))
	for _, id := range ids {
		s.Sessions = append(s.Sessions, snapshot.Session{
			ID:           id,
			CalibVersion: r.sessions[id].cfg.Version,
		})
	}

	// 设备按 ID 排序，保证同状态下字节确定。
	devIDs := make([]string, 0, len(r.devices))
	for id := range r.devices {
		devIDs = append(devIDs, id)
	}
	sort.Strings(devIDs)
	s.Devices = make([]snapshot.DeviceState, 0, len(devIDs))
	for _, id := range devIDs {
		d := r.devices[id]
		state, failures, isolatedAt, successRun := d.tracker.StateAndProgress()
		ds := snapshot.DeviceState{
			DeviceID:      id,
			EmittedMaxSeq: r.acceptedMaxSeq[id],
			Health: snapshot.HealthState{
				State:      int(state),
				Failures:   failures,
				IsolatedAt: isolatedAt,
				SuccessRun: successRun,
			},
		}
		sensorIDs := make([]string, 0, len(d.sensors))
		for sid := range d.sensors {
			sensorIDs = append(sensorIDs, sid)
		}
		sort.Strings(sensorIDs)
		for _, sid := range sensorIDs {
			ss := d.sensors[sid]
			ds.Sensors = append(ds.Sensors, exportSensor(sid, ss))
		}
		s.Devices = append(s.Devices, ds)
	}
	return s
}

func exportSensor(sid string, ss *sensorState) snapshot.SensorState {
	seen := make([]uint64, 0, len(ss.seen))
	for seq := range ss.seen {
		seen = append(seen, seq)
	}
	sort.Slice(seen, func(i, j int) bool { return seen[i] < seen[j] })

	seqs := make([]uint64, 0, len(ss.pending))
	for seq := range ss.pending {
		seqs = append(seqs, seq)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	pending := make([]snapshot.Measurement, 0, len(seqs))
	for _, seq := range seqs {
		pending = append(pending, exportMeasurement(ss.pending[seq]))
	}
	return snapshot.SensorState{
		SensorID:      sid,
		Seen:          seen,
		MaxSeq:        ss.maxSeq,
		HasSeq:        ss.hasSeq,
		Pending:       pending,
		RebootPending: ss.rebootPending,
	}
}

func exportMeasurement(m telemetry.Measurement) snapshot.Measurement {
	return snapshot.Measurement{
		DeviceID:  m.DeviceID,
		SensorID:  m.SensorID,
		Seq:       m.Seq,
		Timestamp: m.Timestamp,
		Value:     m.Value,
		Unit:      string(m.Unit),
		Source:    string(m.Source),
	}
}

func exportConfig(c *calibration.Config) snapshot.CalibConfig {
	sensorIDs := make([]string, 0, len(c.Sensors))
	for sid := range c.Sensors {
		sensorIDs = append(sensorIDs, sid)
	}
	sort.Strings(sensorIDs)
	params := make([]snapshot.CalibSensorParam, 0, len(sensorIDs))
	for _, sid := range sensorIDs {
		p := c.Sensors[sid]
		params = append(params, snapshot.CalibSensorParam{
			SensorID: sid, Scale: p.Scale, Offset: p.Offset,
		})
	}
	devIDs := make([]string, 0, len(c.MinSeq))
	for id := range c.MinSeq {
		devIDs = append(devIDs, id)
	}
	sort.Strings(devIDs)
	minSeq := make([]snapshot.DeviceSeq, 0, len(devIDs))
	for _, id := range devIDs {
		minSeq = append(minSeq, snapshot.DeviceSeq{DeviceID: id, Seq: c.MinSeq[id]})
	}
	return snapshot.CalibConfig{Version: c.Version, Sensors: params, MinSeq: minSeq}
}
