package chunk

import (
	"errors"
	"io"
)

// bstream is an append-only stream of bits.
type bstream struct {
	b    []byte
	free uint8 // unused bits in the last byte (0..8)
}

func (s *bstream) writeBit(bit bool) {
	if s.free == 0 {
		s.b = append(s.b, 0)
		s.free = 8
	}
	if bit {
		s.b[len(s.b)-1] |= 1 << (s.free - 1)
	}
	s.free--
}

// writeBits writes the lowest n bits of u, most significant first, filling
// the current byte and then whole bytes (not one bit at a time).
func (s *bstream) writeBits(u uint64, n int) {
	for n > 0 {
		if s.free == 0 {
			s.b = append(s.b, 0)
			s.free = 8
		}
		take := min(int(s.free), n)
		part := byte(u>>uint(n-take)) & byte(1<<uint(take)-1)
		s.b[len(s.b)-1] |= part << (s.free - uint8(take))
		s.free -= uint8(take)
		n -= take
	}
}

func (s *bstream) writeByte(c byte) { s.writeBits(uint64(c), 8) }

// breader reads a bit stream.
type breader struct {
	b   []byte
	pos uint64 // bit position
}

var errShort = errors.New("chunk: truncated")

func (r *breader) readBit() (bool, error) {
	if r.pos >= uint64(len(r.b))*8 {
		return false, io.ErrUnexpectedEOF
	}
	bit := r.b[r.pos/8]>>(7-r.pos%8)&1 == 1
	r.pos++
	return bit, nil
}

func (r *breader) readBits(n int) (uint64, error) {
	if r.pos+uint64(n) > uint64(len(r.b))*8 {
		return 0, io.ErrUnexpectedEOF
	}
	var u uint64
	for n > 0 {
		// Take as many bits as are left in the current byte.
		off := r.pos % 8
		take := min(8-int(off), n)
		byt := uint64(r.b[r.pos/8])
		bitsVal := (byt >> (8 - off - uint64(take))) & (1<<uint(take) - 1)
		u = u<<uint(take) | bitsVal
		r.pos += uint64(take)
		n -= take
	}
	return u, nil
}

func (r *breader) readByte() (byte, error) {
	u, err := r.readBits(8)
	return byte(u), err
}

// ReadByte makes breader an io.ByteReader, for varints.
func (r *breader) ReadByte() (byte, error) { return r.readByte() }
