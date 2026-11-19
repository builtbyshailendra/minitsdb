package e2e

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
)

type serverProc struct {
	cmd  *exec.Cmd
	addr string
}

func freeAddr(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

func startServer(t *testing.T, dir, addr string) *serverProc {
	t.Helper()
	cmd := exec.Command(binary, "serve", "-dir", dir, "-listen", addr, "-block-range", "30m", "-compact-interval", "300ms")
	out, _ := cmd.StdoutPipe()
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go io.Copy(io.Discard, bufio.NewReader(out))
	s := &serverProc{cmd: cmd, addr: addr}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if r, err := http.Get("http://" + addr + "/-/healthy"); err == nil {
			r.Body.Close()
			return s
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("server didn't start")
	return nil
}

const (
	base  = int64(1_700_000_000_000)
	hosts = 20
)

var cpuRate = map[string]float64{"user": 0.25, "system": 0.1, "idle": 0.6, "iowait": 0.05}

// scrape builds what a node-exporter-like scrape of every host at step i
// would send: CPU counters growing at a fixed rate per mode, a memory
// gauge and up.
func scrape(i int) *prompb.WriteRequest {
	ts := base + int64(i)*15000
	req := &prompb.WriteRequest{}
	for h := range hosts {
		host := fmt.Sprintf("host-%02d", h)
		for _, mode := range []string{"user", "system", "idle", "iowait"} {
			req.Timeseries = append(req.Timeseries, prompb.TimeSeries{
				Labels:  []prompb.Label{{Name: "__name__", Value: "cpu_seconds_total"}, {Name: "instance", Value: host}, {Name: "mode", Value: mode}},
				Samples: []prompb.Sample{{Value: float64(i) * 15 * cpuRate[mode], Timestamp: ts}},
			})
		}
		req.Timeseries = append(req.Timeseries,
			prompb.TimeSeries{
				Labels:  []prompb.Label{{Name: "__name__", Value: "memory_bytes"}, {Name: "instance", Value: host}},
				Samples: []prompb.Sample{{Value: memValue(h, i), Timestamp: ts}},
			},
			prompb.TimeSeries{
				Labels:  []prompb.Label{{Name: "__name__", Value: "up"}, {Name: "instance", Value: host}},
				Samples: []prompb.Sample{{Value: 1, Timestamp: ts}},
			})
	}
	return req
}

func memValue(h, i int) float64 { return float64(1e9 + h*1e6 + (i%10)*4096) }

// send posts a request like Prometheus's remote-write sender does.
func send(addr string, req *prompb.WriteRequest) (int, string, error) {
	raw, err := req.Marshal()
	if err != nil {
		return 0, "", err
	}
	hr, _ := http.NewRequest("POST", "http://"+addr+"/api/v1/write", bytes.NewReader(snappy.Encode(nil, raw)))
	hr.Header.Set("Content-Encoding", "snappy")
	hr.Header.Set("Content-Type", "application/x-protobuf")
	hr.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
	resp, err := http.DefaultClient.Do(hr)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body), nil
}

type apiResult struct {
	Status string `json:"status"`
	Error  string `json:"error"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			Value  []any             `json:"value"`
			Values [][]any           `json:"values"`
		} `json:"result"`
	} `json:"data"`
}

func query(t *testing.T, addr, path string, params url.Values) apiResult {
	t.Helper()
	resp, err := http.PostForm("http://"+addr+path, params)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var r apiResult
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		t.Fatal(err)
	}
	if r.Status != "success" {
		t.Fatalf("%s %v: %s", path, params, r.Error)
	}
	return r
}

func secs(ms int64) string { return strconv.FormatFloat(float64(ms)/1000, 'f', -1, 64) }

func val(x any) float64 {
	f, _ := strconv.ParseFloat(x.(string), 64)
	return f
}

// TestRemoteWriteAndQueryAPI sends 2 hours of scrapes (480 x 120 samples)
// the way Prometheus would, kills the server partway (the sender retries
// until it's back), then checks the query API's answers exactly.
func TestRemoteWriteAndQueryAPI(t *testing.T) {
	dir := t.TempDir()
	addr := freeAddr(t)
	srv := startServer(t, dir, addr)
	const scrapes, killAt = 480, 200
	retries := 0
	start := time.Now()
	for i := range scrapes {
		if i == killAt {
			_ = srv.cmd.Process.Kill()
			_ = srv.cmd.Wait()
			go func() { time.Sleep(500 * time.Millisecond); srv = startServer(t, dir, addr) }()
		}
		backoff := 30 * time.Millisecond
		for {
			code, body, err := send(addr, scrape(i))
			if err == nil && code == http.StatusNoContent {
				break
			}
			if err == nil && code >= 400 && code < 500 {
				t.Fatalf("scrape %d refused: %d %s", i, code, body)
			}
			retries++ // connection refused or 5xx: try again, like Prometheus
			time.Sleep(backoff)
			backoff = min(backoff*2, time.Second)
		}
	}
	t.Logf("sent %d scrapes (%d samples) in %v, server killed once at scrape %d, %d retries",
		scrapes, scrapes*hosts*6, time.Since(start).Round(time.Millisecond), killAt, retries)
	time.Sleep(time.Second) // let the background compaction run

	// Every up sample is there, at every step.
	end := base + (scrapes-1)*15000
	r := query(t, addr, "/api/v1/query_range", url.Values{"query": {"up"}, "start": {secs(base)}, "end": {secs(end)}, "step": {"15s"}})
	if len(r.Data.Result) != hosts {
		t.Fatalf("up: %d series", len(r.Data.Result))
	}
	for _, s := range r.Data.Result {
		if len(s.Values) != scrapes {
			t.Fatalf("up%v: %d points, want %d", s.Metric, len(s.Values), scrapes)
		}
	}

	// A counter growing linearly has an exact rate: every point must equal it.
	r = query(t, addr, "/api/v1/query_range", url.Values{"query": {`sum by (mode) (rate(cpu_seconds_total[5m]))`},
		"start": {secs(base + 600_000)}, "end": {secs(end)}, "step": {"60s"}})
	if len(r.Data.Result) != 4 {
		t.Fatalf("%d modes", len(r.Data.Result))
	}
	points := 0
	for _, s := range r.Data.Result {
		want := hosts * cpuRate[s.Metric["mode"]]
		for _, p := range s.Values {
			if math.Abs(val(p[1])-want) > 1e-9 {
				t.Fatalf("mode %s at %v: %v, want %v", s.Metric["mode"], p[0], p[1], want)
			}
			points++
		}
	}

	// Gauges read back exactly, from blocks and head alike.
	for _, i := range []int{0, 7, 199, 200, 201, 479} {
		r = query(t, addr, "/api/v1/query", url.Values{"query": {`memory_bytes{instance="host-07"}`}, "time": {secs(base + int64(i)*15000)}})
		if len(r.Data.Result) != 1 || val(r.Data.Result[0].Value[1]) != memValue(7, i) {
			t.Fatalf("memory at scrape %d: %+v", i, r.Data.Result)
		}
	}
	r = query(t, addr, "/api/v1/query", url.Values{"query": {"count(up)"}, "time": {secs(end)}})
	if val(r.Data.Result[0].Value[1]) != hosts {
		t.Fatalf("count(up) = %v", r.Data.Result[0].Value)
	}

	// An old sample is refused with 400, so Prometheus would drop it rather than retry.
	code, body, err := send(addr, &prompb.WriteRequest{Timeseries: []prompb.TimeSeries{{
		Labels:  []prompb.Label{{Name: "__name__", Value: "up"}, {Name: "instance", Value: "host-00"}},
		Samples: []prompb.Sample{{Value: 1, Timestamp: base}},
	}}})
	if err != nil || code != http.StatusBadRequest {
		t.Fatalf("old sample: %d %s %v", code, body, err)
	}

	resp, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	m, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	var blocks string
	for _, line := range strings.Split(string(m), "\n") {
		if strings.HasPrefix(line, "minitsdb_blocks ") {
			blocks = strings.TrimPrefix(line, "minitsdb_blocks ")
		}
	}
	if blocks == "" || blocks == "0" {
		t.Fatalf("expected blocks on disk:\n%s", m)
	}
	t.Logf("query API: 480 points per up series, %d rate points all exact, gauges exact across %s blocks and the head; old sample got %d: %s",
		points, blocks, code, strings.TrimSpace(body))
}

// How fast remote write goes end to end: HTTP, snappy, protobuf, WAL
// fsync, head.
func TestRemoteWriteThroughput(t *testing.T) {
	addr := freeAddr(t)
	startServer(t, t.TempDir(), addr)
	const requests, perRequest = 100, 2000 // Prometheus sends up to 2,000 samples per request by default
	reqs := make([]*prompb.WriteRequest, requests)
	for i := range reqs {
		req := &prompb.WriteRequest{}
		for s := range perRequest {
			req.Timeseries = append(req.Timeseries, prompb.TimeSeries{
				Labels:  []prompb.Label{{Name: "__name__", Value: "load_total"}, {Name: "series", Value: strconv.Itoa(s)}},
				Samples: []prompb.Sample{{Value: float64(i), Timestamp: base + int64(i)*15000}},
			})
		}
		reqs[i] = req
	}
	start := time.Now()
	for _, req := range reqs {
		if code, body, err := send(addr, req); err != nil || code != http.StatusNoContent {
			t.Fatalf("%d %s %v", code, body, err)
		}
	}
	el := time.Since(start)
	t.Logf("%d requests of %d samples: %.0f samples/s, %v per request (one client, one request at a time)",
		requests, perRequest, float64(requests*perRequest)/el.Seconds(), (el / requests).Round(time.Microsecond))
}
