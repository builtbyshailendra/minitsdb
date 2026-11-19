// Package server is the HTTP interface: Prometheus remote write in, the
// Prometheus query API out, and the server's own metrics.
package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/builtbyshailendra/minitsdb/labels"
	"github.com/builtbyshailendra/minitsdb/promql"
	"github.com/builtbyshailendra/minitsdb/remote"
	"github.com/builtbyshailendra/minitsdb/tsdb"
)

// Server serves one database.
type Server struct {
	DB     *tsdb.DB
	Engine *promql.Engine
	mux    *http.ServeMux

	requests, samples, rejected, ignored, queries atomic.Int64
}

// New builds the handler.
func New(db *tsdb.DB) *Server {
	s := &Server{DB: db, Engine: &promql.Engine{Q: db}, mux: http.NewServeMux()}
	s.mux.HandleFunc("POST /api/v1/write", s.write)
	s.mux.HandleFunc("/api/v1/query", s.query)
	s.mux.HandleFunc("/api/v1/query_range", s.queryRange)
	s.mux.HandleFunc("GET /-/healthy", func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, "OK\n") })
	s.mux.HandleFunc("GET /metrics", s.metrics)
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

// write is the remote-write receiver. Status codes follow what Prometheus's
// sender expects: 204 when stored; 400 when the request can never succeed
// (bad encoding, out-of-order or duplicate samples), which the sender
// drops; 500 when retrying might help.
func (s *Server) write(w http.ResponseWriter, r *http.Request) {
	s.requests.Add(1)
	body, err := io.ReadAll(io.LimitReader(r.Body, remote.MaxDecodedSize))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	req, err := remote.Decode(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	res, err := s.DB.Append(req.Samples)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.samples.Add(int64(res.Accepted))
	s.ignored.Add(int64(res.Ignored))
	s.rejected.Add(int64(res.Rejected))
	if res.Rejected > 0 {
		// The accepted samples are stored; the rest never will be.
		http.Error(w, fmt.Sprintf("%d of %d samples rejected: %v", res.Rejected, len(req.Samples), res.FirstErr), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type apiResponse struct {
	Status    string `json:"status"`
	Data      any    `json:"data,omitempty"`
	ErrorType string `json:"errorType,omitempty"`
	Error     string `json:"error,omitempty"`
}

func reply(w http.ResponseWriter, code int, v apiResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func badData(w http.ResponseWriter, err error) {
	reply(w, http.StatusBadRequest, apiResponse{Status: "error", ErrorType: "bad_data", Error: err.Error()})
}

// parseTime accepts Unix seconds (with fractions) or RFC 3339.
func parseTime(s string, def time.Time) (int64, error) {
	if s == "" {
		return def.UnixMilli(), nil
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return int64(math.Round(f * 1000)), nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return 0, fmt.Errorf("cannot parse %q as a time", s)
	}
	return t.UnixMilli(), nil
}

func parseStep(s string) (int64, error) {
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return int64(f * 1000), nil
	}
	d, err := promql.ParseDuration(s)
	return d.Milliseconds(), err
}

// The API writes timestamps as Unix seconds and values as strings.
type point struct {
	t int64
	v float64
}

func (p point) MarshalJSON() ([]byte, error) {
	return fmt.Appendf(nil, "[%s,%q]", strconv.FormatFloat(float64(p.t)/1000, 'f', -1, 64), formatValue(p.v)), nil
}

func formatValue(v float64) string {
	switch {
	case math.IsNaN(v):
		return "NaN"
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	}
	return strconv.FormatFloat(v, 'f', -1, 64)
}

func labelMap(ls labels.Labels) map[string]string {
	m := map[string]string{}
	for _, l := range ls {
		m[l.Name] = l.Value
	}
	return m
}

func (s *Server) query(w http.ResponseWriter, r *http.Request) {
	s.queries.Add(1)
	t, err := parseTime(r.FormValue("time"), time.Now())
	if err != nil {
		badData(w, err)
		return
	}
	v, err := s.Engine.Instant(r.FormValue("query"), t)
	if err != nil {
		badData(w, err)
		return
	}
	switch v := v.(type) {
	case promql.Scalar:
		reply(w, 200, apiResponse{Status: "success", Data: map[string]any{"resultType": "scalar", "result": point{t, float64(v)}}})
	case promql.Vector:
		res := make([]map[string]any, 0, len(v))
		for _, smp := range v {
			res = append(res, map[string]any{"metric": labelMap(smp.Labels), "value": point{t, smp.V}})
		}
		reply(w, 200, apiResponse{Status: "success", Data: map[string]any{"resultType": "vector", "result": res}})
	}
}

func (s *Server) queryRange(w http.ResponseWriter, r *http.Request) {
	s.queries.Add(1)
	start, err1 := parseTime(r.FormValue("start"), time.Now())
	end, err2 := parseTime(r.FormValue("end"), time.Now())
	step, err3 := parseStep(r.FormValue("step"))
	if err := errors.Join(err1, err2, err3); err != nil {
		badData(w, err)
		return
	}
	if step <= 0 || (end-start)/step > 11000 { // Prometheus's limit on points per series
		badData(w, fmt.Errorf("step must be positive and give at most 11,000 points"))
		return
	}
	series, err := s.Engine.Range(r.FormValue("query"), start, end, step)
	if err != nil {
		badData(w, err)
		return
	}
	res := make([]map[string]any, 0, len(series))
	for _, ser := range series {
		pts := make([]point, len(ser.Points))
		for i, p := range ser.Points {
			pts[i] = point{p.T, p.V}
		}
		res = append(res, map[string]any{"metric": labelMap(ser.Labels), "values": pts})
	}
	reply(w, 200, apiResponse{Status: "success", Data: map[string]any{"resultType": "matrix", "result": res}})
}

// metrics exposes the server's own counters in the Prometheus text format,
// so a Prometheus could scrape it.
func (s *Server) metrics(w http.ResponseWriter, _ *http.Request) {
	st := s.DB.Stats()
	type m struct {
		name, help, typ string
		v               float64
	}
	ms := []m{
		{"minitsdb_remote_write_requests_total", "Remote-write requests received.", "counter", float64(s.requests.Load())},
		{"minitsdb_samples_appended_total", "Samples stored.", "counter", float64(s.samples.Load())},
		{"minitsdb_samples_rejected_total", "Samples rejected (out of order, duplicate, too old).", "counter", float64(s.rejected.Load())},
		{"minitsdb_samples_ignored_total", "Exact repeats of stored samples (retries).", "counter", float64(s.ignored.Load())},
		{"minitsdb_queries_total", "Queries evaluated.", "counter", float64(s.queries.Load())},
		{"minitsdb_head_series", "Series in the head block.", "gauge", float64(st.HeadSeries)},
		{"minitsdb_head_samples", "Samples in the head block.", "gauge", float64(st.HeadSamples)},
		{"minitsdb_blocks", "Blocks on disk.", "gauge", float64(len(st.Blocks))},
		{"minitsdb_block_bytes", "Bytes of block chunks and indexes on disk.", "gauge", float64(st.ChunkBytes + st.IndexBytes)},
	}
	sort.Slice(ms, func(i, j int) bool { return ms[i].name < ms[j].name })
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	var b strings.Builder
	for _, x := range ms {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s %s\n%s %s\n", x.name, x.help, x.name, x.typ, x.name, formatValue(x.v))
	}
	io.WriteString(w, b.String())
}
