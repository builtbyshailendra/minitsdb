// Package chunk compresses (timestamp, value) samples the way Facebook's
// Gorilla paper (VLDB 2015) describes and Prometheus implements.
//
// Timestamps: most series are scraped at a fixed interval, so the
// difference between consecutive gaps (delta-of-delta) is usually zero and
// costs one bit. Values: consecutive values are often equal or close, so
// XOR with the previous value has long runs of zero bits; only the
// meaningful middle bits are stored.
//
// Layout: a 2-byte sample count, then a bit stream:
//
//	sample 0:  timestamp as a signed varint, value as 64 raw bits
//	sample 1:  timestamp delta as an unsigned varint, value XOR-encoded
//	sample n:  timestamp delta-of-delta in a variable-width bucket, value XOR-encoded
//
// Delta-of-delta buckets (milliseconds, as Prometheus uses):
//
//	'0'                 dod == 0
//	'10'   + 14 bits    -8191 .. 8192
//	'110'  + 17 bits    -65535 .. 65536
//	'1110' + 20 bits    -524287 .. 524288
//	'1111' + 64 bits    anything else
//
// XOR value encoding:
//
//	'0'                          same value as before
//	'10' + meaningful bits        fits in the previous leading/trailing-zero window
//	'11' + 5 bits leading zeros + 6 bits length + meaningful bits
package chunk

import (
	"encoding/binary"
	"errors"
	"math"
	"math/bits"
)

// MaxSamples is how many samples a chunk holds before a new one is started.
const MaxSamples = 120

// Chunk is an appendable compressed chunk.
type Chunk struct {
	s      bstream
	n      uint16
	t      int64
	tDelta int64
	v      float64
	// leading and trailing zero bits of the last XOR window; leading 0xff
	// means no window yet.
	leading, trailing uint8
}

// New returns an empty chunk.
func New() *Chunk {
	c := &Chunk{leading: 0xff}
	c.s.b = make([]byte, 2, 128)
	return c
}

// NumSamples is the number of samples appended.
func (c *Chunk) NumSamples() int { return int(c.n) }

// Bytes returns the encoded chunk. It stays valid until the next Append.
func (c *Chunk) Bytes() []byte {
	binary.BigEndian.PutUint16(c.s.b, c.n)
	return c.s.b
}

// Full reports whether the chunk has reached MaxSamples.
func (c *Chunk) Full() bool { return c.n >= MaxSamples }

// Append adds a sample. Timestamps must increase; the caller checks.
func (c *Chunk) Append(t int64, v float64) {
	switch c.n {
	case 0:
		var buf [binary.MaxVarintLen64]byte
		for _, b := range buf[:binary.PutVarint(buf[:], t)] {
			c.s.writeByte(b)
		}
		c.s.writeBits(math.Float64bits(v), 64)
	case 1:
		tDelta := t - c.t
		var buf [binary.MaxVarintLen64]byte
		for _, b := range buf[:binary.PutUvarint(buf[:], uint64(tDelta))] {
			c.s.writeByte(b)
		}
		c.writeValue(v)
		c.tDelta = tDelta
	default:
		tDelta := t - c.t
		dod := tDelta - c.tDelta
		switch {
		case dod == 0:
			c.s.writeBit(false)
		case fits(dod, 14):
			c.s.writeBits(0b10, 2)
			c.s.writeBits(uint64(dod), 14)
		case fits(dod, 17):
			c.s.writeBits(0b110, 3)
			c.s.writeBits(uint64(dod), 17)
		case fits(dod, 20):
			c.s.writeBits(0b1110, 4)
			c.s.writeBits(uint64(dod), 20)
		default:
			c.s.writeBits(0b1111, 4)
			c.s.writeBits(uint64(dod), 64)
		}
		c.writeValue(v)
		c.tDelta = tDelta
	}
	c.t, c.v = t, v
	c.n++
}

// fits reports whether x fits the n-bit bucket: -(2^(n-1)-1) .. 2^(n-1).
func fits(x int64, n uint) bool {
	return -((1<<(n-1))-1) <= x && x <= 1<<(n-1)
}

func (c *Chunk) writeValue(v float64) {
	x := math.Float64bits(v) ^ math.Float64bits(c.v)
	if x == 0 {
		c.s.writeBit(false)
		return
	}
	c.s.writeBit(true)
	leading := uint8(bits.LeadingZeros64(x))
	trailing := uint8(bits.TrailingZeros64(x))
	if leading >= 32 { // 5 bits can only say up to 31
		leading = 31
	}
	if c.leading != 0xff && leading >= c.leading && trailing >= c.trailing {
		// The meaningful bits fit in the previous window: reuse it.
		c.s.writeBit(false)
		c.s.writeBits(x>>c.trailing, 64-int(c.leading)-int(c.trailing))
		return
	}
	c.leading, c.trailing = leading, trailing
	c.s.writeBit(true)
	c.s.writeBits(uint64(leading), 5)
	sig := 64 - leading - trailing
	// 64 meaningful bits don't fit in 6 bits; write 0 and read it as 64
	// (0 meaningful bits never happens: that's the x == 0 case).
	c.s.writeBits(uint64(sig), 6)
	c.s.writeBits(x>>trailing, int(sig))
}

// Iterator decodes a chunk.
type Iterator struct {
	r      breader
	n, i   uint16
	t      int64
	tDelta int64
	v      float64
	lead   uint8
	trail  uint8
	err    error
}

// NewIterator reads an encoded chunk.
func NewIterator(b []byte) *Iterator {
	if len(b) < 2 {
		return &Iterator{err: errShort}
	}
	return &Iterator{r: breader{b: b[2:]}, n: binary.BigEndian.Uint16(b)}
}

// Count returns the number of samples in an encoded chunk.
func Count(b []byte) int {
	if len(b) < 2 {
		return 0
	}
	return int(binary.BigEndian.Uint16(b))
}

// At returns the current sample.
func (it *Iterator) At() (int64, float64) { return it.t, it.v }

// Err returns the first decoding error.
func (it *Iterator) Err() error { return it.err }

// Next advances to the next sample.
func (it *Iterator) Next() bool {
	if it.err != nil || it.i >= it.n {
		return false
	}
	switch it.i {
	case 0:
		t, err := binary.ReadVarint(&it.r)
		if err != nil {
			return it.fail(err)
		}
		v, err := it.r.readBits(64)
		if err != nil {
			return it.fail(err)
		}
		it.t, it.v = t, math.Float64frombits(v)
	case 1:
		d, err := binary.ReadUvarint(&it.r)
		if err != nil {
			return it.fail(err)
		}
		it.tDelta = int64(d)
		it.t += it.tDelta
		if err := it.readValue(); err != nil {
			return it.fail(err)
		}
	default:
		var prefix, n int
		for prefix < 4 {
			bit, err := it.r.readBit()
			if err != nil {
				return it.fail(err)
			}
			if !bit {
				break
			}
			prefix++
		}
		switch prefix {
		case 0:
			n = 0
		case 1:
			n = 14
		case 2:
			n = 17
		case 3:
			n = 20
		case 4:
			n = 64
		}
		var dod int64
		if n > 0 {
			u, err := it.r.readBits(n)
			if err != nil {
				return it.fail(err)
			}
			dod = signExtend(u, n)
		}
		it.tDelta += dod
		it.t += it.tDelta
		if err := it.readValue(); err != nil {
			return it.fail(err)
		}
	}
	it.i++
	return true
}

func signExtend(u uint64, n int) int64 {
	if n == 64 {
		return int64(u)
	}
	// Buckets hold -(2^(n-1)-1) .. 2^(n-1); 2^(n-1) itself has the sign
	// bit set but is positive.
	if u > 1<<(n-1) {
		return int64(u) - 1<<n
	}
	return int64(u)
}

func (it *Iterator) readValue() error {
	bit, err := it.r.readBit()
	if err != nil {
		return err
	}
	if !bit {
		return nil // same value
	}
	newWindow, err := it.r.readBit()
	if err != nil {
		return err
	}
	if newWindow {
		l, err := it.r.readBits(5)
		if err != nil {
			return err
		}
		sig, err := it.r.readBits(6)
		if err != nil {
			return err
		}
		if sig == 0 {
			sig = 64
		}
		it.lead, it.trail = uint8(l), uint8(64-l-sig)
	}
	sig := 64 - int(it.lead) - int(it.trail)
	u, err := it.r.readBits(sig)
	if err != nil {
		return err
	}
	it.v = math.Float64frombits(math.Float64bits(it.v) ^ u<<it.trail)
	return nil
}

func (it *Iterator) fail(err error) bool {
	it.err = errors.Join(errShort, err)
	return false
}
