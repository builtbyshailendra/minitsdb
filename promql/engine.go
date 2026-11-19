package promql

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/builtbyshailendra/minitsdb/labels"
	"github.com/builtbyshailendra/minitsdb/tsdb"
)

// Querier is what the engine reads from (tsdb.DB satisfies it).
type Querier interface {
	Select(mint, maxt int64, ms ...*labels.Matcher) []tsdb.Series
}

// Engine evaluates queries.
type Engine struct {
	Q        Querier
	Lookback time.Duration // how far back an instant selector looks, default 5m
}

// Sample is one element of an instant vector.
type Sample struct {
	Labels labels.Labels
	V      float64
}

// Vector is the result of an expression at one instant.
type Vector []Sample

// Scalar is a plain number.
type Scalar float64

type value interface{ isValue() }

func (Vector) isValue() {}
func (Scalar) isValue() {}

// ErrManyToMany: a binary operation found more than one series with the
// same labels on one side.
var ErrManyToMany = errors.New("many-to-many matching not allowed")

// Range evaluates q at start, start+step, ... end (milliseconds) and returns
// one series per label set, as Prometheus's query_range does.
func (e *Engine) Range(q string, start, end, step int64) ([]tsdb.Series, error) {
	if step <= 0 {
		return nil, fmt.Errorf("step must be positive")
	}
	ex, err := Parse(q)
	if err != nil {
		return nil, err
	}
	ev := e.prepare(ex, start, end)
	out := map[string]*tsdb.Series{}
	for t := start; t <= end; t += step {
		v, err := ev.eval(ex, t)
		if err != nil {
			return nil, err
		}
		switch v := v.(type) {
		case Scalar:
			s := out[""]
			if s == nil {
				s = &tsdb.Series{Labels: labels.Labels{}}
				out[""] = s
			}
			s.Points = append(s.Points, tsdb.Point{T: t, V: float64(v)})
		case Vector:
			for _, smp := range v {
				k := smp.Labels.String()
				s := out[k]
				if s == nil {
					s = &tsdb.Series{Labels: smp.Labels}
					out[k] = s
				}
				s.Points = append(s.Points, tsdb.Point{T: t, V: smp.V})
			}
		}
	}
	res := make([]tsdb.Series, 0, len(out))
	for _, s := range out {
		res = append(res, *s)
	}
	sort.Slice(res, func(i, j int) bool { return labels.Compare(res[i].Labels, res[j].Labels) < 0 })
	return res, nil
}

// Instant evaluates q at time t. The result is a Vector or a Scalar.
func (e *Engine) Instant(q string, t int64) (any, error) {
	ex, err := Parse(q)
	if err != nil {
		return nil, err
	}
	v, err := e.prepare(ex, t, t).eval(ex, t)
	if vec, ok := v.(Vector); ok {
		sort.Slice(vec, func(i, j int) bool { return labels.Compare(vec[i].Labels, vec[j].Labels) < 0 })
	}
	return v, err
}

type evaluator struct {
	lookback int64
	data     map[*VectorSelector][]tsdb.Series
}

// prepare fetches every selector's data once, for the whole query range.
func (e *Engine) prepare(ex Expr, start, end int64) *evaluator {
	lb := e.Lookback
	if lb == 0 {
		lb = 5 * time.Minute
	}
	ev := &evaluator{lookback: lb.Milliseconds(), data: map[*VectorSelector][]tsdb.Series{}}
	var walk func(Expr)
	walk = func(x Expr) {
		switch x := x.(type) {
		case *VectorSelector:
			ev.data[x] = e.Q.Select(start-ev.lookback, end, x.Matchers...)
		case *MatrixSelector:
			ev.data[x.VS] = e.Q.Select(start-x.Range.Milliseconds(), end, x.VS.Matchers...)
		case *Call:
			for _, a := range x.Args {
				walk(a)
			}
		case *AggregateExpr:
			walk(x.Expr)
		case *BinaryExpr:
			walk(x.LHS)
			walk(x.RHS)
		}
	}
	walk(ex)
	return ev
}

// window returns the points with T in (from, to].
func window(pts []tsdb.Point, from, to int64) []tsdb.Point {
	lo := sort.Search(len(pts), func(i int) bool { return pts[i].T > from })
	hi := sort.Search(len(pts), func(i int) bool { return pts[i].T > to })
	return pts[lo:hi]
}

func (ev *evaluator) eval(x Expr, t int64) (value, error) {
	switch x := x.(type) {
	case *NumberLiteral:
		return Scalar(x.Val), nil
	case *VectorSelector:
		var out Vector
		for _, s := range ev.data[x] {
			if w := window(s.Points, t-ev.lookback, t); len(w) > 0 {
				out = append(out, Sample{Labels: s.Labels, V: w[len(w)-1].V})
			}
		}
		return out, nil
	case *MatrixSelector:
		return nil, fmt.Errorf("a range vector can only be a function argument")
	case *Call:
		return ev.call(x, t)
	case *AggregateExpr:
		return ev.aggregate(x, t)
	case *BinaryExpr:
		return ev.binary(x, t)
	}
	return nil, fmt.Errorf("unknown expression %T", x)
}

func (ev *evaluator) call(c *Call, t int64) (value, error) {
	ms := c.Args[0].(*MatrixSelector)
	rng := ms.Range.Milliseconds()
	var out Vector
	for _, s := range ev.data[ms.VS] {
		w := window(s.Points, t-rng, t)
		var v float64
		switch c.Func {
		case "rate", "increase":
			r, ok := extrapolatedRate(w, t-rng, t, c.Func == "rate")
			if !ok {
				continue
			}
			v = r
		default:
			if len(w) == 0 {
				continue
			}
			v = overTime(c.Func, w)
		}
		out = append(out, Sample{Labels: s.Labels.Without(), V: v})
	}
	return out, nil
}

// extrapolatedRate is how Prometheus turns the samples in a window into an
// increase or a per-second rate:
//   - the raw increase is last - first, plus the value before every counter
//     reset (a drop means the counter restarted from zero);
//   - the samples rarely sit exactly on the window's edges, so the increase
//     is extrapolated towards each edge, but only if the gap to the edge is
//     less than 1.1x the average spacing of the samples (otherwise the
//     series probably started or stopped inside the window, and it is
//     extended by only half a spacing);
//   - a counter can't go below zero, so the extrapolation towards the start
//     stops where the line would cross zero.
func extrapolatedRate(w []tsdb.Point, rangeStart, rangeEnd int64, isRate bool) (float64, bool) {
	if len(w) < 2 {
		return 0, false
	}
	first, last := w[0], w[len(w)-1]
	increase := last.V - first.V
	prev := first.V
	for _, p := range w[1:] {
		if p.V < prev { // reset
			increase += prev
		}
		prev = p.V
	}
	sampled := float64(last.T-first.T) / 1000
	avgGap := sampled / float64(len(w)-1)
	toStart := float64(first.T-rangeStart) / 1000
	toEnd := float64(rangeEnd-last.T) / 1000
	threshold := avgGap * 1.1
	if toStart >= threshold {
		toStart = avgGap / 2
	}
	if increase > 0 && first.V >= 0 {
		if toZero := sampled * (first.V / increase); toZero < toStart {
			toStart = toZero
		}
	}
	if toEnd >= threshold {
		toEnd = avgGap / 2
	}
	factor := (sampled + toStart + toEnd) / sampled
	if isRate {
		factor /= float64(rangeEnd-rangeStart) / 1000
	}
	return increase * factor, true
}

func overTime(fn string, w []tsdb.Point) float64 {
	switch fn {
	case "count_over_time":
		return float64(len(w))
	case "min_over_time":
		m := w[0].V
		for _, p := range w[1:] {
			m = math.Min(m, p.V)
		}
		return m
	case "max_over_time":
		m := w[0].V
		for _, p := range w[1:] {
			m = math.Max(m, p.V)
		}
		return m
	}
	sum := 0.0
	for _, p := range w {
		sum += p.V
	}
	if fn == "avg_over_time" {
		return sum / float64(len(w))
	}
	return sum
}

func (ev *evaluator) aggregate(a *AggregateExpr, t int64) (value, error) {
	v, err := ev.eval(a.Expr, t)
	if err != nil {
		return nil, err
	}
	vec, ok := v.(Vector)
	if !ok {
		return nil, fmt.Errorf("%s needs a vector", a.Op)
	}
	type group struct {
		lset          labels.Labels
		sum, min, max float64
		count         int
	}
	groups := map[string]*group{}
	var order []string
	for _, s := range vec {
		var key labels.Labels
		if a.Without {
			key = s.Labels.Without(a.Grouping...)
		} else {
			key = s.Labels.Only(a.Grouping...)
		}
		k := key.String()
		g := groups[k]
		if g == nil {
			g = &group{lset: key, min: s.V, max: s.V}
			groups[k] = g
			order = append(order, k)
		}
		g.sum += s.V
		g.count++
		g.min = math.Min(g.min, s.V)
		g.max = math.Max(g.max, s.V)
	}
	out := make(Vector, 0, len(groups))
	for _, k := range order {
		g := groups[k]
		var v float64
		switch a.Op {
		case "sum":
			v = g.sum
		case "avg":
			v = g.sum / float64(g.count)
		case "min":
			v = g.min
		case "max":
			v = g.max
		case "count":
			v = float64(g.count)
		}
		out = append(out, Sample{Labels: g.lset, V: v})
	}
	return out, nil
}

func arith(op string, a, b float64) float64 {
	switch op {
	case "+":
		return a + b
	case "-":
		return a - b
	case "*":
		return a * b
	}
	return a / b // division by zero gives ±Inf or NaN, as in Prometheus
}

func (ev *evaluator) binary(b *BinaryExpr, t int64) (value, error) {
	l, err := ev.eval(b.LHS, t)
	if err != nil {
		return nil, err
	}
	r, err := ev.eval(b.RHS, t)
	if err != nil {
		return nil, err
	}
	switch l := l.(type) {
	case Scalar:
		switch r := r.(type) {
		case Scalar:
			return Scalar(arith(b.Op, float64(l), float64(r))), nil
		case Vector:
			out := make(Vector, len(r))
			for i, s := range r {
				out[i] = Sample{Labels: s.Labels.Without(), V: arith(b.Op, float64(l), s.V)}
			}
			return out, nil
		}
	case Vector:
		switch r := r.(type) {
		case Scalar:
			out := make(Vector, len(l))
			for i, s := range l {
				out[i] = Sample{Labels: s.Labels.Without(), V: arith(b.Op, s.V, float64(r))}
			}
			return out, nil
		case Vector:
			// One-to-one matching on all labels except the metric name.
			right := map[string]Sample{}
			for _, s := range r {
				k := s.Labels.Without().String()
				if _, dup := right[k]; dup {
					return nil, ErrManyToMany
				}
				right[k] = s
			}
			seen := map[string]bool{}
			var out Vector
			for _, s := range l {
				key := s.Labels.Without()
				k := key.String()
				if seen[k] {
					return nil, ErrManyToMany
				}
				seen[k] = true
				if rs, ok := right[k]; ok {
					out = append(out, Sample{Labels: key, V: arith(b.Op, s.V, rs.V)})
				}
			}
			return out, nil
		}
	}
	return nil, fmt.Errorf("unsupported operands for %s", b.Op)
}
