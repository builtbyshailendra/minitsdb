package promql

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/builtbyshailendra/minitsdb/tsdb"
)

func TestParseErrors(t *testing.T) {
	for q, want := range map[string]string{
		`rate(x)`:             "needs a range vector",
		`x[5q]`:               "expected a duration",
		`sum by (job x`:       "expected",
		`foo(x[5m])`:          "unknown function",
		`x{job="a"`:           "expected",
		`{}`:                  "needs a metric name",
		`x{job=~"("}`:         "missing closing",
		`x + `:                "unexpected",
		`1 2`:                 "unexpected",
		`x{job="unterminated`: "unterminated string",
	} {
		_, err := Parse(q)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: got %v, want an error containing %q", q, err, want)
		}
	}
}

func TestDurations(t *testing.T) {
	for s, want := range map[string]time.Duration{"30s": 30 * time.Second, "5m": 5 * time.Minute, "1h30m": 90 * time.Minute, "250ms": 250 * time.Millisecond, "2d": 48 * time.Hour} {
		if got, err := ParseDuration(s); err != nil || got != want {
			t.Errorf("%s: %v %v", s, got, err)
		}
	}
}

// A worked example of rate, by hand. Window (0 s, 60 s], samples at 10, 20,
// 30, 40, 50 s with values 100, 110, 5 (a reset), 15, 25:
//   - increase = 25 - 100 + 110 (the value before the reset) = 35
//   - sampled interval 40 s, average gap 10 s, threshold 11 s
//   - 10 s to the start and 10 s to the end: both under 11 s, so extend fully
//   - the zero point: 40 * 100 / 35 = 114 s away, further than 10 s: no limit
//   - increase over the window = 35 * (40 + 10 + 10) / 40 = 52.5; rate = 52.5 / 60
func TestRateByHand(t *testing.T) {
	w := []tsdb.Point{{T: 10000, V: 100}, {T: 20000, V: 110}, {T: 30000, V: 5}, {T: 40000, V: 15}, {T: 50000, V: 25}}
	inc, ok := extrapolatedRate(w, 0, 60000, false)
	if !ok || math.Abs(inc-52.5) > 1e-12 {
		t.Fatalf("increase %v", inc)
	}
	r, _ := extrapolatedRate(w, 0, 60000, true)
	if math.Abs(r-52.5/60) > 1e-12 {
		t.Fatalf("rate %v", r)
	}
	// A series that starts in the middle of the window: 35 s to the start is
	// more than 1.1 gaps, so it's extended by only half a gap (5 s).
	late := []tsdb.Point{{T: 35000, V: 0}, {T: 45000, V: 10}, {T: 55000, V: 20}}
	inc, _ = extrapolatedRate(late, 0, 60000, false)
	// increase 20 over 20 s; first value 0 means the zero point is right at
	// the first sample, so nothing is added at the start; 5 s to the end.
	if math.Abs(inc-20*(20+0+5)/20.0) > 1e-12 {
		t.Fatalf("late series increase %v", inc)
	}
}
