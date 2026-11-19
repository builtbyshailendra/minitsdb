// Package wal is the write-ahead log: every write is appended here, and
// fsynced, before it is acknowledged. After a crash the log is replayed to
// rebuild the in-memory head.
//
// The log is a directory of numbered segment files. Each record is framed
// as: type (1 byte) | payload length (4 bytes) | CRC32C of payload (4 bytes)
// | payload. A crash can leave the last record half-written; replay detects
// it (short read or checksum mismatch), truncates the segment there, and
// carries on. Such a record was never acknowledged, because acknowledgment
// waits for the fsync that follows a complete write.
package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/builtbyshailendra/minitsdb/crash"
	"github.com/builtbyshailendra/minitsdb/labels"
)

// Record types.
const (
	RecSeries  byte = 1
	RecSamples byte = 2
)

const headerSize = 9

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Record is one entry to log.
type Record struct {
	Type    byte
	Payload []byte
}

// WAL appends records to the current segment.
type WAL struct {
	dir     string
	segSize int64
	f       *os.File
	seg     int
	size    int64
	NoSync  bool // skip fsync (only for measuring what it costs)
}

// DefaultSegmentSize is when a new segment file is started.
const DefaultSegmentSize = 32 << 20

func segName(dir string, n int) string { return filepath.Join(dir, fmt.Sprintf("%08d", n)) }

// Segments lists segment numbers in order.
func Segments(dir string) ([]int, error) {
	ents, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var segs []int
	for _, e := range ents {
		if n, err := strconv.Atoi(e.Name()); err == nil && !e.IsDir() {
			segs = append(segs, n)
		}
	}
	sort.Ints(segs)
	return segs, nil
}

// Open opens the log for appending, continuing the last segment. Call
// Replay first: it repairs a torn tail.
func Open(dir string, segSize int64) (*WAL, error) {
	if segSize == 0 {
		segSize = DefaultSegmentSize
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	segs, err := Segments(dir)
	if err != nil {
		return nil, err
	}
	w := &WAL{dir: dir, segSize: segSize, seg: 1}
	if len(segs) > 0 {
		w.seg = segs[len(segs)-1]
	}
	if err := w.openSegment(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *WAL) openSegment() error {
	f, err := os.OpenFile(segName(w.dir, w.seg), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	w.f, w.size = f, st.Size()
	return nil
}

// Segment is the number of the segment being written.
func (w *WAL) Segment() int { return w.seg }

// Cut closes the current segment and starts the next one. It returns the
// new segment's number: everything before it can be checkpointed.
func (w *WAL) Cut() (int, error) {
	if err := w.f.Sync(); err != nil {
		return 0, err
	}
	if err := w.f.Close(); err != nil {
		return 0, err
	}
	w.seg++
	return w.seg, w.openSegment()
}

// Log appends records as one write and, unless NoSync, fsyncs before
// returning. Only after Log returns may the write be acknowledged.
func (w *WAL) Log(recs ...Record) error {
	var buf []byte
	for _, r := range recs {
		var h [headerSize]byte
		h[0] = r.Type
		binary.BigEndian.PutUint32(h[1:5], uint32(len(r.Payload)))
		binary.BigEndian.PutUint32(h[5:9], crc32.Checksum(r.Payload, castagnoli))
		buf = append(buf, h[:]...)
		buf = append(buf, r.Payload...)
	}
	if w.size > 0 && w.size+int64(len(buf)) > w.segSize {
		if _, err := w.Cut(); err != nil {
			return err
		}
	}
	if len(buf) > 1 {
		half := len(buf) / 2
		if _, err := w.f.Write(buf[:half]); err != nil {
			return err
		}
		crash.Point("wal.torn") // half a write on disk, then the process dies
		if _, err := w.f.Write(buf[half:]); err != nil {
			return err
		}
	}
	w.size += int64(len(buf))
	crash.Point("wal.before-sync")
	if w.NoSync {
		return nil
	}
	return w.f.Sync()
}

// Close syncs and closes the log.
func (w *WAL) Close() error {
	if err := w.f.Sync(); err != nil {
		return err
	}
	return w.f.Close()
}

// Truncate deletes segments numbered below seg.
func Truncate(dir string, seg int) error {
	segs, err := Segments(dir)
	if err != nil {
		return err
	}
	for _, s := range segs {
		if s < seg {
			if err := os.Remove(segName(dir, s)); err != nil {
				return err
			}
		}
	}
	return nil
}

// ReplayStats describes what replay found.
type ReplayStats struct {
	Segments  int
	Records   int
	TornBytes int64 // bytes cut from the end of the log
	Dropped   int   // whole segments after a corrupt one, removed
}

// Replay calls fn for every intact record in order, from segment from
// onwards (0 = all). If a record is torn or corrupt, the segment is
// truncated just before it and any later segments are removed: a crash can
// only damage the tail, so anything after the damage was never acknowledged.
func Replay(dir string, from int, fn func(typ byte, payload []byte) error) (ReplayStats, error) {
	var st ReplayStats
	segs, err := Segments(dir)
	if err != nil {
		return st, err
	}
	for i, seg := range segs {
		if seg < from {
			continue
		}
		st.Segments++
		good, err := replaySegment(segName(dir, seg), fn, &st)
		if err == nil {
			continue
		}
		if !errors.Is(err, errTorn) {
			return st, err
		}
		// Cut the damage off and forget later segments.
		info, statErr := os.Stat(segName(dir, seg))
		if statErr != nil {
			return st, statErr
		}
		st.TornBytes += info.Size() - good
		if err := os.Truncate(segName(dir, seg), good); err != nil {
			return st, err
		}
		for _, later := range segs[i+1:] {
			if info, err := os.Stat(segName(dir, later)); err == nil {
				st.TornBytes += info.Size()
			}
			if err := os.Remove(segName(dir, later)); err != nil {
				return st, err
			}
			st.Dropped++
		}
		break
	}
	return st, nil
}

var errTorn = errors.New("torn or corrupt record")

// replaySegment returns how many bytes were good.
func replaySegment(path string, fn func(byte, []byte) error, st *ReplayStats) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	var good int64
	for {
		var h [headerSize]byte
		n, err := io.ReadFull(r, h[:])
		if err == io.EOF && n == 0 {
			return good, nil // clean end
		}
		if err != nil {
			return good, errTorn
		}
		size := binary.BigEndian.Uint32(h[1:5])
		if size > 1<<30 || (h[0] != RecSeries && h[0] != RecSamples) {
			return good, errTorn
		}
		payload := make([]byte, size)
		if _, err := io.ReadFull(r, payload); err != nil {
			return good, errTorn
		}
		if crc32.Checksum(payload, castagnoli) != binary.BigEndian.Uint32(h[5:9]) {
			return good, errTorn
		}
		if err := fn(h[0], payload); err != nil {
			return good, err
		}
		st.Records++
		good += int64(headerSize) + int64(size)
	}
}

// RefSeries is a series record entry.
type RefSeries struct {
	Ref    uint64
	Labels labels.Labels
}

// RefSample is a sample record entry.
type RefSample struct {
	Ref uint64
	T   int64
	V   float64
}

// EncodeSeries builds a series record payload.
func EncodeSeries(ss []RefSeries) []byte {
	b := binary.AppendUvarint(nil, uint64(len(ss)))
	for _, s := range ss {
		b = binary.AppendUvarint(b, s.Ref)
		b = binary.AppendUvarint(b, uint64(len(s.Labels)))
		for _, l := range s.Labels {
			b = appendString(b, l.Name)
			b = appendString(b, l.Value)
		}
	}
	return b
}

// EncodeSamples builds a samples record payload.
func EncodeSamples(ss []RefSample) []byte {
	b := binary.AppendUvarint(nil, uint64(len(ss)))
	for _, s := range ss {
		b = binary.AppendUvarint(b, s.Ref)
		b = binary.AppendVarint(b, s.T)
		b = binary.BigEndian.AppendUint64(b, math.Float64bits(s.V))
	}
	return b
}

func appendString(b []byte, s string) []byte {
	b = binary.AppendUvarint(b, uint64(len(s)))
	return append(b, s...)
}

type decoder struct {
	b   []byte
	err error
}

func (d *decoder) uvarint() uint64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Uvarint(d.b)
	if n <= 0 {
		d.err = errors.New("wal: bad uvarint")
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *decoder) varint() int64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Varint(d.b)
	if n <= 0 {
		d.err = errors.New("wal: bad varint")
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *decoder) string() string {
	n := d.uvarint()
	if d.err != nil || uint64(len(d.b)) < n {
		d.err = errors.New("wal: bad string")
		return ""
	}
	s := string(d.b[:n])
	d.b = d.b[n:]
	return s
}

func (d *decoder) u64() uint64 {
	if d.err != nil || len(d.b) < 8 {
		d.err = errors.New("wal: short")
		return 0
	}
	v := binary.BigEndian.Uint64(d.b)
	d.b = d.b[8:]
	return v
}

// DecodeSeries parses a series record.
func DecodeSeries(b []byte) ([]RefSeries, error) {
	d := &decoder{b: b}
	n := d.uvarint()
	var out []RefSeries
	for i := uint64(0); i < n && d.err == nil; i++ {
		s := RefSeries{Ref: d.uvarint()}
		nl := d.uvarint()
		for j := uint64(0); j < nl && d.err == nil; j++ {
			s.Labels = append(s.Labels, labels.Label{Name: d.string(), Value: d.string()})
		}
		out = append(out, s)
	}
	return out, d.err
}

// DecodeSamples parses a samples record.
func DecodeSamples(b []byte) ([]RefSample, error) {
	d := &decoder{b: b}
	n := d.uvarint()
	out := make([]RefSample, 0, n)
	for i := uint64(0); i < n && d.err == nil; i++ {
		out = append(out, RefSample{Ref: d.uvarint(), T: d.varint(), V: math.Float64frombits(d.u64())})
	}
	return out, d.err
}
