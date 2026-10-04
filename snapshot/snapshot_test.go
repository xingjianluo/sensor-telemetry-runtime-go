package snapshot

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func validSnapshot() *Snapshot {
	return &Snapshot{
		Magic:   FormatMagic,
		Format:  FormatVersion,
		TakenAt: time.Date(2026, 10, 4, 8, 0, 0, 0, time.UTC),
		Window:  2,
		Policy: Policy{
			WindowNS:         int64(10 * time.Second),
			IsolateThreshold: 3,
			QuarantineNS:     int64(5 * time.Second),
			RecoverAfter:     2,
		},
		CurrentCalibVersion: 1,
		NextSessionID:       2,
		Calibs: []CalibConfig{{
			Version: 1,
			Sensors: []CalibSensorParam{{SensorID: "temp-1", Scale: 2, Offset: 1}},
		}},
		Sessions: []Session{{ID: 1, CalibVersion: 1}},
		Devices: []DeviceState{{
			DeviceID:      "dev-1",
			Health:        HealthState{State: 0},
			EmittedMaxSeq: 1,
			Sensors: []SensorState{{
				SensorID: "temp-1",
				Seen:     []uint64{1, 2},
				MaxSeq:   2,
				HasSeq:   true,
				Pending: []Measurement{{
					DeviceID: "dev-1", SensorID: "temp-1", Seq: 2,
					Timestamp: time.Date(2026, 9, 26, 12, 0, 2, 0, time.UTC),
					Value:     2, Unit: "C", Source: "device",
				}},
			}},
		}},
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	data, err := Encode(validSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("encoded snapshot:\n%s", string(data))
	got, err := Decode(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Window != 2 || got.CurrentCalibVersion != 1 || got.NextSessionID != 2 {
		t.Fatalf("header mismatch: %+v", got)
	}
	if len(got.Devices) != 1 || len(got.Devices[0].Sensors[0].Pending) != 1 {
		t.Fatalf("body mismatch: %+v", got.Devices)
	}
	m := got.Devices[0].Sensors[0].Pending[0]
	if m.Value != 2 || m.Unit != "C" || m.Timestamp.IsZero() {
		t.Fatalf("pending measurement mismatch: %+v", m)
	}
}

func TestDecodeRejectsBadMagicAndVersion(t *testing.T) {
	s := validSnapshot()
	s.Magic = "something-else"
	data, _ := Encode(s)
	if _, err := Decode(data); !errors.Is(err, ErrBadMagic) {
		t.Fatalf("want ErrBadMagic, got %v", err)
	}
	t.Logf("bad magic correctly rejected: %v", ErrBadMagic)

	s = validSnapshot()
	s.Format = FormatVersion + 1
	data, _ = Encode(s)
	_, err := Decode(data)
	if !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("want ErrUnsupportedVersion, got %v", err)
	}
	if !strings.Contains(err.Error(), "unsupported format version") {
		t.Fatalf("error not explainable: %v", err)
	}
	t.Logf("future version correctly rejected: %v", err)

	if _, err := Decode([]byte("{not json")); !errors.Is(err, ErrMalformed) {
		t.Fatalf("want ErrMalformed on garbage, got %v", err)
	}
}

func TestDecodeRejectsStructuralProblems(t *testing.T) {
	cases := map[string]func(*Snapshot){
		"window<1":                      func(s *Snapshot) { s.Window = 0 },
		"empty calibs":                  func(s *Snapshot) { s.Calibs = nil },
		"current version missing":       func(s *Snapshot) { s.CurrentCalibVersion = 9 },
		"session binds unknown version": func(s *Snapshot) { s.Sessions[0].CalibVersion = 9 },
		"duplicate device":              func(s *Snapshot) { s.Devices = append(s.Devices, s.Devices[0]) },
		"bad health state":              func(s *Snapshot) { s.Devices[0].Health.State = 7 },
		"duplicate pending seq": func(s *Snapshot) {
			dup := s.Devices[0].Sensors[0].Pending[0]
			s.Devices[0].Sensors[0].Pending = append(s.Devices[0].Sensors[0].Pending, dup)
		},
		"pending key mismatch": func(s *Snapshot) {
			s.Devices[0].Sensors[0].Pending[0].DeviceID = "dev-other"
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			s := validSnapshot()
			mutate(s)
			data, _ := Encode(s)
			_, err := Decode(data)
			if !errors.Is(err, ErrMalformed) {
				t.Fatalf("want ErrMalformed, got %v", err)
			}
			t.Logf("%-28s -> %v", name, err)
		})
	}
}
