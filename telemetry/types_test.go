package telemetry

import (
	"errors"
	"testing"
	"time"
)

func TestValidateRejections(t *testing.T) {
	base := Measurement{
		DeviceID:  "dev-1",
		SensorID:  "temp-1",
		Seq:       1,
		Timestamp: time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC),
		Value:     25,
		Unit:      UnitCelsius,
		Source:    SourceDevice,
	}
	cases := []struct {
		name   string
		mutate func(m *Measurement)
		want   error
	}{
		{"ok", func(m *Measurement) {}, nil},
		{"missing device", func(m *Measurement) { m.DeviceID = "" }, ErrMissingDeviceID},
		{"missing sensor", func(m *Measurement) { m.SensorID = "" }, ErrMissingSensorID},
		{"missing unit", func(m *Measurement) { m.Unit = "" }, ErrMissingUnit},
		{"unknown unit", func(m *Measurement) { m.Unit = "furlong" }, ErrUnknownUnit},
		{"unknown source", func(m *Measurement) { m.Source = "carrier-pigeon" }, ErrUnknownSource},
		{"zero time", func(m *Measurement) { m.Timestamp = time.Time{} }, ErrInvalidTime},
		{"out of range high", func(m *Measurement) { m.Value = 20000 }, ErrOutOfRange},
		{"out of range low", func(m *Measurement) { m.Unit = UnitPercent; m.Value = -1 }, ErrOutOfRange},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := base
			tc.mutate(&m)
			err := m.Validate()
			t.Logf("input=%+v -> err=%v", m, err)
			if tc.want == nil {
				if err != nil {
					t.Fatalf("expected accept, got %v", err)
				}
				return
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("expected reason %v, got %v", tc.want, err)
			}
		})
	}
}
