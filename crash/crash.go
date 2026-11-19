// Package crash makes a process die at a named point, like kill -9: no
// deferred cleanup, no graceful shutdown. Whatever the process already
// wrote to a file stays written (the operating system has it); whatever it
// hadn't written yet is gone.
//
// Set MINITSDB_CRASH=point:n to exit with status 137 the n-th time the point
// is reached, for example MINITSDB_CRASH=wal.torn:3.
package crash

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
)

// ExitCode is what a crashed process exits with (128 + SIGKILL).
const ExitCode = 137

var (
	target string
	at     int64 = 1
	hits   atomic.Int64
)

func init() {
	spec := os.Getenv("MINITSDB_CRASH")
	if spec == "" {
		return
	}
	target = spec
	if i := strings.LastIndexByte(spec, ':'); i > 0 {
		if n, err := strconv.ParseInt(spec[i+1:], 10, 64); err == nil {
			target, at = spec[:i], n
		}
	}
}

// Point exits the process if it is the configured crash point and has been
// reached the configured number of times.
func Point(name string) {
	if target == "" || name != target {
		return
	}
	if hits.Add(1) == at {
		fmt.Fprintf(os.Stderr, "CRASH at %s (hit %d)\n", name, at)
		os.Exit(ExitCode)
	}
}
