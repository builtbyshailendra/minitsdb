package promql

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	promlabels "github.com/prometheus/prometheus/model/labels"
	prom "github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/util/teststorage"

	"github.com/builtbyshailendra/minitsdb/labels"
	"github.com/builtbyshailendra/minitsdb/tsdb"
)

// The oracle test loads the same samples into minitsdb and into
// Prometheus's own storage, runs the same queries through both engines, and
// compares every point. Prometheus's engine is the specification.

const t0 = int64(1_700_000_000_000) // a round time in milliseconds

type genSeries struct {
	lset labels.Labels
	pts  []tsdb.Point
}

// generate makes 2 hours of awkward data: scrape jitter, missed scrapes,
// counter resets, series that start late or stop early, and float gauges.
func generate(seed uint64) []genSeries {
	rng := rand.New(rand.NewPCG(seed, 99))
	var out []genSeries
	for _, job := range []string{"api", "web", "batch"} {
		for _, inst := range []string{"a", "b", "c"} {
			for _, code := range []string{"200", "500"} {
				s := genSeries{lset: labels.FromStrings("__name__", "requests_total", "job", job, "instance", inst, "code", code)}
				start, end := int64(0), int64(2*3600*1000)
				if rng.IntN(4) == 0 {
					start = int64(rng.IntN(3600)) * 1000 // starts late
				}
				if rng.IntN(4) == 0 {
					end = start + int64(600+rng.IntN(3000))*1000 // stops early
				}
				jitter := rng.IntN(2) == 0
				v := float64(rng.IntN(1000))
				for ts := start; ts < end; ts += 15000 {
					if rng.Float64() < 0.05 {
						continue // missed scrape
					}
					t := ts
					if jitter {
						t += int64(rng.IntN(1000)) - 500
					}
					if rng.Float64() < 0.01 {
						v = float64(rng.IntN(10)) // counter reset (process restarted)
					} else {
						v += float64(rng.IntN(100))
					}
					// Stored timestamps are absolute: compare t0+t, not t
					// (comparing t kept only the first sample of each series,
					// and the first version of this test passed on nearly
					// empty data).
					if len(s.pts) > 0 && t0+t <= s.pts[len(s.pts)-1].T {
						continue
					}
					s.pts = append(s.pts, tsdb.Point{T: t0 + t, V: v})
				}
				out = append(out, s)
			}
		}
	}
	for _, room := range []string{"x", "y", "z"} {
		s := genSeries{lset: labels.FromStrings("__name__", "temperature_celsius", "room", room)}
		v := 20.0
		for ts := int64(0); ts < 2*3600*1000; ts += 30000 {
			v += rng.NormFloat64() * 0.3
			s.pts = append(s.pts, tsdb.Point{T: t0 + ts, V: v})
		}
		out = append(out, s)
	}
	return out
}

func load(t *testing.T, data []genSeries) (*tsdb.DB, *teststorage.TestStorage) {
	t.Helper()
	db, err := tsdb.Open(tsdb.Options{Dir: t.TempDir(), NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	st := teststorage.New(t)
	t.Cleanup(func() { st.Close() })
	app := st.Appender(context.Background())
	for _, s := range data {
		var batch []tsdb.Sample
		pl := promlabels.FromMap(labelMap(s.lset))
		for _, p := range s.pts {
			batch = append(batch, tsdb.Sample{Labels: s.lset, T: p.T, V: p.V})
			if _, err := app.Append(0, pl, p.T, p.V); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := db.Append(batch); err != nil {
			t.Fatal(err)
		}
	}
	if err := app.Commit(); err != nil {
		t.Fatal(err)
	}
	return db, st
}

func labelMap(ls labels.Labels) map[string]string {
	m := map[string]string{}
	for _, l := range ls {
		m[l.Name] = l.Value
	}
	return m
}

func fromProm(pl promlabels.Labels) labels.Labels {
	return labels.FromMap(pl.Map())
}

var oracleQueries = []string{
	`requests_total`,
	`requests_total{code="500", job=~"api|web"}`,
	`requests_total{instance!="a", job!~"b.*"}`,
	`rate(requests_total[1m])`,
	`rate(requests_total[5m])`,
	`rate(requests_total[15m])`,
	`increase(requests_total[10m])`,
	`sum by (job) (rate(requests_total[5m]))`,
	`sum(rate(requests_total[5m])) by (code, job)`,
	`sum without (instance) (rate(requests_total[5m]))`,
	`sum(rate(requests_total{code="500"}[5m])) / sum(rate(requests_total[5m]))`,
	`sum by (job) (rate(requests_total{code="500"}[5m])) / sum by (job) (rate(requests_total[5m]))`,
	`rate(requests_total[5m]) * 60`,
	`avg(temperature_celsius)`,
	`max by (room) (temperature_celsius)`,
	`min(temperature_celsius) - 1`,
	`count(requests_total)`,
	`count by (job) (rate(requests_total[5m]))`,
	`2 * temperature_celsius + 1`,
	`avg_over_time(temperature_celsius[10m])`,
	`max_over_time(temperature_celsius[5m])`,
	`min_over_time(temperature_celsius[5m])`,
	`sum_over_time(requests_total[2m])`,
	`count_over_time(requests_total[5m])`,
	`-temperature_celsius`,
	`(1 + 2) * 3`,
}

func close(a, b float64) bool {
	if math.IsNaN(a) || math.IsNaN(b) {
		return math.IsNaN(a) && math.IsNaN(b)
	}
	if math.IsInf(a, 0) || math.IsInf(b, 0) {
		return a == b
	}
	return math.Abs(a-b) <= 1e-9*math.Max(1, math.Max(math.Abs(a), math.Abs(b)))
}

func TestMatchesPrometheus(t *testing.T) {
	ctx := context.Background()
	ng := prom.NewEngine(prom.EngineOpts{MaxSamples: 50_000_000, Timeout: time.Minute, LookbackDelta: 5 * time.Minute})
	queries, points, mismatches := 0, 0, 0
	perShape := map[string]int{} // points compared per query shape
	var first []string
	for seed := uint64(1); seed <= 5; seed++ {
		data := generate(seed)
		db, st := load(t, data)
		ours := &Engine{Q: db}
		rng := rand.New(rand.NewPCG(seed, 7))
		for _, q := range oracleQueries {
			for range 4 {
				// A random window inside the data, with an odd step so
				// evaluation times don't line up with scrapes.
				start := t0 + int64(rng.IntN(5400))*1000 + int64(rng.IntN(1000))
				end := start + int64(600+rng.IntN(1800))*1000
				step := int64(1+rng.IntN(60)) * 1000
				pq, err := ng.NewRangeQuery(ctx, st, nil, q, time.UnixMilli(start), time.UnixMilli(end), time.Duration(step)*time.Millisecond)
				if err != nil {
					t.Fatalf("%s: %v", q, err)
				}
				res := pq.Exec(ctx)
				if res.Err != nil {
					t.Fatalf("prometheus %s: %v", q, res.Err)
				}
				want := map[string]map[int64]float64{}
				switch v := res.Value.(type) {
				case prom.Matrix:
					for _, s := range v {
						m := map[int64]float64{}
						for _, p := range s.Floats {
							m[p.T] = p.F
						}
						want[fromProm(s.Metric).String()] = m
					}
				default:
					t.Fatalf("%s: result type %T", q, v)
				}
				got, err := ours.Range(q, start, end, step)
				if err != nil {
					t.Fatalf("minitsdb %s: %v", q, err)
				}
				queries++
				gotKeys := map[string]bool{}
				for _, s := range got {
					k := s.Labels.String()
					gotKeys[k] = true
					w, ok := want[k]
					if !ok {
						mismatches++
						first = append(first, fmt.Sprintf("%s: extra series %s", q, k))
						continue
					}
					if len(w) != len(s.Points) {
						mismatches++
						first = append(first, fmt.Sprintf("%s %s: %d points, Prometheus %d", q, k, len(s.Points), len(w)))
					}
					for _, p := range s.Points {
						points++
						perShape[q]++
						pv, ok := w[p.T]
						if !ok || !close(pv, p.V) {
							mismatches++
							first = append(first, fmt.Sprintf("%s %s at %d: %v, Prometheus %v (present %v)", q, k, p.T-t0, p.V, pv, ok))
						}
					}
				}
				for k := range want {
					if !gotKeys[k] {
						mismatches++
						first = append(first, fmt.Sprintf("%s: missing series %s", q, k))
					}
				}
			}
		}
	}
	t.Logf("%d range queries (%d query shapes x 4 windows x 5 datasets), %d points compared, %d mismatches",
		queries, len(oracleQueries), points, mismatches)
	// Guard against agreeing on nothing: every query shape must have
	// produced a real number of points.
	for _, q := range oracleQueries {
		if perShape[q] < 200 {
			t.Errorf("%s: only %d points compared", q, perShape[q])
		}
	}
	if mismatches > 0 {
		if len(first) > 15 {
			first = first[:15]
		}
		t.Fatalf("differences from Prometheus:\n%s", strings.Join(first, "\n"))
	}
}
