#!/usr/bin/env bash
# Breaks the PromQL engine in four known ways, one at a time, and shows that
# the comparison with Prometheus's engine catches each. Restores the file after.
set -u
cd "$(dirname "$0")/.."
f=promql/engine.go
cp "$f" "$f.orig"
trap 'mv "$f.orig" "$f"' EXIT
run() {
  echo "== $1"
  go test -count=1 -v -run TestMatchesPrometheus ./promql 2>&1 | grep -E "points compared|^--- "
  cp "$f.orig" "$f"
}
sed -i 's/return pts\[i\].T > from })/return pts[i].T >= from })/' "$f"; run "range window includes its left edge"
sed -i 's/if p.V < prev { \/\/ reset/if false \&\& p.V < prev { \/\/ reset/' "$f"; run "counter resets ignored"
sed -i 's/threshold := avgGap \* 1.1/threshold := avgGap * 1.0/' "$f"; run "extrapolation threshold 1.0 instead of 1.1"
sed -i 's/if increase > 0 \&\& first.V >= 0 {/if false \&\& increase > 0 \&\& first.V >= 0 {/' "$f"; run "no zero-point limit"
