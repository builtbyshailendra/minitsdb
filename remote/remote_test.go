package remote

import (
	"math"
	"testing"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
)

// Encoded with Prometheus's own generated types (prompb), decoded with this
// package's hand-written decoder.
func TestDecodesPrometheusWriteRequests(t *testing.T) {
	req := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels:    []prompb.Label{{Name: "__name__", Value: "http_requests_total"}, {Name: "code", Value: "200"}, {Name: "job", Value: "api"}},
				Samples:   []prompb.Sample{{Value: 1, Timestamp: 1_700_000_000_000}, {Value: 2.5, Timestamp: 1_700_000_015_000}},
				Exemplars: []prompb.Exemplar{{Labels: []prompb.Label{{Name: "trace_id", Value: "abc"}}, Value: 1, Timestamp: 1_700_000_000_000}},
			},
			{
				Labels:     []prompb.Label{{Name: "__name__", Value: "temperature"}, {Name: "empty", Value: ""}},
				Samples:    []prompb.Sample{{Value: math.Inf(-1), Timestamp: -5}, {Value: math.Copysign(0, -1), Timestamp: 0}},
				Histograms: []prompb.Histogram{{Count: &prompb.Histogram_CountInt{CountInt: 3}, Timestamp: 1}},
			},
		},
		Metadata: []prompb.MetricMetadata{{MetricFamilyName: "http_requests_total", Help: "requests", Type: prompb.MetricMetadata_COUNTER}},
	}
	raw, err := req.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decode(snappy.Encode(nil, raw))
	if err != nil {
		t.Fatal(err)
	}
	if got.Series != 2 || len(got.Samples) != 4 || got.Histograms != 1 {
		t.Fatalf("%+v", got)
	}
	a := got.Samples[1]
	if a.Labels.String() != `http_requests_total{code="200", job="api"}` || a.T != 1_700_000_015_000 || a.V != 2.5 {
		t.Fatalf("sample %+v", a)
	}
	// -0 doesn't survive: proto3 doesn't write a field equal to its zero
	// value, and -0 == 0, so Prometheus's encoder leaves the value out and
	// the receiver sees the default, +0.
	b := got.Samples[3]
	if b.Labels.String() != `temperature{}` || b.T != 0 || math.Float64bits(b.V) != 0 {
		t.Fatalf("sample %+v (labels %s)", b, b.Labels)
	}
	if got.Samples[2].T != -5 || !math.IsInf(got.Samples[2].V, -1) {
		t.Fatalf("negative timestamp / -Inf: %+v", got.Samples[2])
	}
}

func TestRejectsBadInput(t *testing.T) {
	for name, body := range map[string][]byte{
		"not snappy":      []byte("hello"),
		"bad protobuf":    snappy.Encode(nil, []byte{0x0a, 0xff, 0x01}),
		"duplicate label": snappy.Encode(nil, mustMarshal(t, &prompb.WriteRequest{Timeseries: []prompb.TimeSeries{{Labels: []prompb.Label{{Name: "a", Value: "1"}, {Name: "a", Value: "2"}}}}})),
	} {
		if _, err := Decode(body); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func mustMarshal(t *testing.T, r *prompb.WriteRequest) []byte {
	b, err := r.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// FuzzParse: any bytes either parse or return an error; never a panic.
func FuzzParse(f *testing.F) {
	ok, _ := (&prompb.WriteRequest{Timeseries: []prompb.TimeSeries{{
		Labels: []prompb.Label{{Name: "__name__", Value: "x"}}, Samples: []prompb.Sample{{Value: 1, Timestamp: 2}},
	}}}).Marshal()
	f.Add(ok)
	f.Add([]byte{0x0a, 0x00})
	f.Fuzz(func(t *testing.T, b []byte) {
		_, _ = Parse(b)
	})
}
