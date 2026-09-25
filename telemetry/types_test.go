package telemetry

import (
	"errors"
	"testing"
	"time"
)

func validSample() Sample {
	return Sample{
		DeviceID:    "dev-1",
		Metric:      "temperature",
		Unit:        UnitCelsius,
		Value:       21.5,
		Seq:         1,
		DeviceEpoch: 1,
		Timestamp:   time.Unix(1000, 0),
		Source:      Source{Kind: "gateway", ID: "gw-1"},
	}
}

func TestValidateRejectReasons(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Sample)
		want   RejectReason
	}{
		{"missing device", func(s *Sample) { s.DeviceID = "" }, ReasonMissingDevice},
		{"missing metric", func(s *Sample) { s.Metric = "" }, ReasonMissingMetric},
		{"missing unit", func(s *Sample) { s.Unit = "" }, ReasonMissingUnit},
		{"unknown unit", func(s *Sample) { s.Unit = "furlong" }, ReasonUnknownUnit},
		{"missing source kind", func(s *Sample) { s.Source.Kind = "" }, ReasonUnknownSource},
		{"missing source id", func(s *Sample) { s.Source.ID = "" }, ReasonUnknownSource},
		{"NaN value", func(s *Sample) { s.Value = nan() }, ReasonInvalidReading},
		{"zero timestamp", func(s *Sample) { s.Timestamp = time.Time{} }, ReasonInvalidReading},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := validSample()
			tc.mutate(&s)
			err := Validate(s)
			var re *RejectError
			if !errors.As(err, &re) {
				t.Fatalf("input=%+v: expected RejectError, got %v", s, err)
			}
			t.Logf("input=%+v -> reject reason=%s detail=%s", s, re.Reason, re.Detail)
			if re.Reason != tc.want {
				t.Fatalf("reason=%s, want %s", re.Reason, tc.want)
			}
		})
	}
}

func TestValidateAcceptsValidSample(t *testing.T) {
	s := validSample()
	if err := Validate(s); err != nil {
		t.Fatalf("input=%+v: unexpected reject: %v", s, err)
	}
	t.Logf("input=%+v -> accepted", s)
}

func nan() float64 {
	z := 0.0
	return z / z
}
