// Package remote decodes Prometheus remote-write 1.0 requests: a
// snappy-compressed (block format) protobuf WriteRequest.
//
// The protobuf is decoded by hand with protowire instead of generated code.
// The messages used are small:
//
//	message WriteRequest { repeated TimeSeries timeseries = 1; ... }
//	message TimeSeries   { repeated Label labels = 1; repeated Sample samples = 2;
//	                       repeated Exemplar exemplars = 3; repeated Histogram histograms = 4; }
//	message Label        { string name = 1; string value = 2; }
//	message Sample       { double value = 1; int64 timestamp = 2; }
//
// Fields this package doesn't use (metadata, exemplars) are skipped, as
// protobuf allows. Native histograms aren't supported and are counted.
package remote

import (
	"errors"
	"fmt"
	"math"

	"github.com/golang/snappy"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/builtbyshailendra/minitsdb/labels"
	"github.com/builtbyshailendra/minitsdb/tsdb"
)

// MaxDecodedSize limits a decompressed request.
const MaxDecodedSize = 32 << 20

// Request is a decoded write request.
type Request struct {
	Samples    []tsdb.Sample
	Series     int
	Histograms int // native histogram samples received and ignored
}

var errMalformed = errors.New("malformed protobuf")

// Decode decompresses and parses a request body.
func Decode(body []byte) (Request, error) {
	n, err := snappy.DecodedLen(body)
	if err != nil {
		return Request{}, fmt.Errorf("snappy: %w", err)
	}
	if n > MaxDecodedSize {
		return Request{}, fmt.Errorf("request decodes to %d bytes, limit %d", n, MaxDecodedSize)
	}
	raw, err := snappy.Decode(nil, body)
	if err != nil {
		return Request{}, fmt.Errorf("snappy: %w", err)
	}
	return Parse(raw)
}

// Parse reads an uncompressed WriteRequest.
func Parse(b []byte) (Request, error) {
	var req Request
	err := fields(b, func(num protowire.Number, typ protowire.Type, v []byte, _ uint64) error {
		if num != 1 || typ != protowire.BytesType {
			return nil // not a time series: skip
		}
		req.Series++
		return parseSeries(v, &req)
	})
	return req, err
}

func parseSeries(b []byte, req *Request) error {
	m := map[string]string{}
	var samples []tsdb.Sample
	err := fields(b, func(num protowire.Number, typ protowire.Type, v []byte, _ uint64) error {
		switch {
		case num == 1 && typ == protowire.BytesType:
			var name, value string
			if err := fields(v, func(n protowire.Number, t protowire.Type, s []byte, _ uint64) error {
				if t != protowire.BytesType {
					return nil
				}
				switch n {
				case 1:
					name = string(s)
				case 2:
					value = string(s)
				}
				return nil
			}); err != nil {
				return err
			}
			if _, dup := m[name]; dup {
				return fmt.Errorf("duplicate label %q", name)
			}
			if name == "" {
				return fmt.Errorf("empty label name")
			}
			m[name] = value
		case num == 2 && typ == protowire.BytesType:
			var s tsdb.Sample
			if err := fields(v, func(n protowire.Number, t protowire.Type, _ []byte, x uint64) error {
				switch {
				case n == 1 && t == protowire.Fixed64Type:
					s.V = math.Float64frombits(x)
				case n == 2 && t == protowire.VarintType:
					s.T = int64(x)
				}
				return nil
			}); err != nil {
				return err
			}
			samples = append(samples, s)
		case num == 4 && typ == protowire.BytesType:
			req.Histograms++
		}
		return nil
	})
	if err != nil {
		return err
	}
	// Labels with an empty value are the same as no label, as in Prometheus.
	for k, v := range m {
		if v == "" {
			delete(m, k)
		}
	}
	lset := labels.FromMap(m)
	for i := range samples {
		samples[i].Labels = lset
	}
	req.Samples = append(req.Samples, samples...)
	return nil
}

// fields walks the fields of one protobuf message. For length-delimited
// fields v is the payload; for varint and fixed64 fields x is the value.
func fields(b []byte, fn func(num protowire.Number, typ protowire.Type, v []byte, x uint64) error) error {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return errMalformed
		}
		b = b[n:]
		var v []byte
		var x uint64
		switch typ {
		case protowire.BytesType:
			v, n = protowire.ConsumeBytes(b)
		case protowire.VarintType:
			x, n = protowire.ConsumeVarint(b)
		case protowire.Fixed64Type:
			x, n = protowire.ConsumeFixed64(b)
		case protowire.Fixed32Type:
			var x32 uint32
			x32, n = protowire.ConsumeFixed32(b)
			x = uint64(x32)
		default:
			n = protowire.ConsumeFieldValue(num, typ, b)
		}
		if n < 0 {
			return errMalformed
		}
		b = b[n:]
		if err := fn(num, typ, v, x); err != nil {
			return err
		}
	}
	return nil
}
