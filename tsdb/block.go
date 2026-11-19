package tsdb

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"

	"github.com/builtbyshailendra/minitsdb/chunk"
	"github.com/builtbyshailendra/minitsdb/crash"
	"github.com/builtbyshailendra/minitsdb/labels"
)

// A block is an immutable directory holding all samples in [MinT, MaxT):
//
//	chunks     every chunk's bytes, one after another
//	index      symbols, series (labels + where their chunks are), postings
//	meta.json  time range, counts, and which blocks it was made from
//
// It is written to a temporary directory and renamed into place, so a crash
// never leaves a half-written block under a real name.

// BlockMeta describes a block.
type BlockMeta struct {
	ID      string   `json:"id"`
	MinT    int64    `json:"minT"`
	MaxT    int64    `json:"maxT"` // exclusive
	Level   int      `json:"level"`
	Sources []string `json:"sources"` // the level-1 blocks this one contains
	Series  int      `json:"series"`
	Chunks  int      `json:"chunks"`
	Samples int      `json:"samples"`
}

type chunkRef struct {
	mint, maxt int64
	off, size  uint64
}

type blockSeries struct {
	lset   labels.Labels
	chunks []chunkRef
}

// Block is an open block.
type Block struct {
	Meta     BlockMeta
	dir      string
	series   []blockSeries
	postings map[labels.Label][]uint32
	chunks   *os.File
	sizes    struct{ index, chunks int64 }
}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

const indexMagic = "MTSI1"

// writeBlock writes series (sorted by labels, each with chunks sorted by
// time) as a new block under dir and returns its metadata.
func writeBlock(dir string, meta BlockMeta, series []blockSeriesData) (BlockMeta, error) {
	meta.ID = fmt.Sprintf("%013d-%013d-L%d", meta.MinT, meta.MaxT, meta.Level)
	if len(meta.Sources) == 0 {
		meta.Sources = []string{meta.ID} // a level-1 block is its own source
	}
	tmp := filepath.Join(dir, meta.ID+".tmp")
	final := filepath.Join(dir, meta.ID)
	if err := os.RemoveAll(tmp); err != nil {
		return meta, err
	}
	if err := os.MkdirAll(tmp, 0o755); err != nil {
		return meta, err
	}

	// chunks file
	var cbuf []byte
	refs := make([][]chunkRef, len(series))
	for i, s := range series {
		for _, c := range s.chunks {
			refs[i] = append(refs[i], chunkRef{mint: c.mint, maxt: c.maxt, off: uint64(len(cbuf)), size: uint64(len(c.data))})
			cbuf = append(cbuf, c.data...)
			meta.Chunks++
			meta.Samples += chunk.Count(c.data)
		}
	}
	meta.Series = len(series)
	if err := writeFileSync(filepath.Join(tmp, "chunks"), cbuf); err != nil {
		return meta, err
	}
	crash.Point("block.after-chunks") // a crash here leaves only a .tmp dir

	// index: symbols, series, postings, then a CRC of everything.
	symSet := map[string]bool{}
	for _, s := range series {
		for _, l := range s.lset {
			symSet[l.Name], symSet[l.Value] = true, true
		}
	}
	syms := make([]string, 0, len(symSet))
	for s := range symSet {
		syms = append(syms, s)
	}
	sort.Strings(syms)
	symID := map[string]uint64{}
	b := []byte(indexMagic)
	b = binary.AppendUvarint(b, uint64(len(syms)))
	for i, s := range syms {
		symID[s] = uint64(i)
		b = binary.AppendUvarint(b, uint64(len(s)))
		b = append(b, s...)
	}
	postings := map[labels.Label][]uint32{}
	b = binary.AppendUvarint(b, uint64(len(series)))
	for i, s := range series {
		b = binary.AppendUvarint(b, uint64(len(s.lset)))
		for _, l := range s.lset {
			b = binary.AppendUvarint(b, symID[l.Name])
			b = binary.AppendUvarint(b, symID[l.Value])
			postings[l] = append(postings[l], uint32(i))
		}
		b = binary.AppendUvarint(b, uint64(len(refs[i])))
		for _, r := range refs[i] {
			b = binary.AppendVarint(b, r.mint)
			b = binary.AppendUvarint(b, uint64(r.maxt-r.mint))
			b = binary.AppendUvarint(b, r.off)
			b = binary.AppendUvarint(b, r.size)
		}
	}
	keys := make([]labels.Label, 0, len(postings))
	for k := range postings {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Name != keys[j].Name {
			return keys[i].Name < keys[j].Name
		}
		return keys[i].Value < keys[j].Value
	})
	b = binary.AppendUvarint(b, uint64(len(keys)))
	for _, k := range keys {
		b = binary.AppendUvarint(b, symID[k.Name])
		b = binary.AppendUvarint(b, symID[k.Value])
		list := postings[k]
		b = binary.AppendUvarint(b, uint64(len(list)))
		prev := uint32(0)
		for _, id := range list { // ids ascend: store gaps
			b = binary.AppendUvarint(b, uint64(id-prev))
			prev = id
		}
	}
	b = binary.BigEndian.AppendUint32(b, crc32.Checksum(b, castagnoli))
	if err := writeFileSync(filepath.Join(tmp, "index"), b); err != nil {
		return meta, err
	}
	mb, _ := json.MarshalIndent(meta, "", "  ")
	if err := writeFileSync(filepath.Join(tmp, "meta.json"), mb); err != nil {
		return meta, err
	}
	if err := os.Rename(tmp, final); err != nil {
		return meta, err
	}
	return meta, syncDir(dir)
}

type blockSeriesData struct {
	lset   labels.Labels
	chunks []memChunk
}

func writeFileSync(path string, b []byte) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// syncDir makes a rename durable. Windows can't fsync a directory; there
// the rename is as durable as NTFS makes it.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	_ = d.Sync() // fails on Windows, works on Linux
	return nil
}

// openBlock reads a block's meta and index; chunks are read on demand.
func openBlock(dir string) (*Block, error) {
	blk := &Block{dir: dir, postings: map[labels.Label][]uint32{}}
	mb, err := os.ReadFile(filepath.Join(dir, "meta.json"))
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(mb, &blk.Meta); err != nil {
		return nil, err
	}
	b, err := os.ReadFile(filepath.Join(dir, "index"))
	if err != nil {
		return nil, err
	}
	blk.sizes.index = int64(len(b))
	if len(b) < len(indexMagic)+4 || string(b[:len(indexMagic)]) != indexMagic {
		return nil, fmt.Errorf("%s: not an index", dir)
	}
	if crc32.Checksum(b[:len(b)-4], castagnoli) != binary.BigEndian.Uint32(b[len(b)-4:]) {
		return nil, fmt.Errorf("%s: index checksum mismatch", dir)
	}
	d := &idxDecoder{b: b[len(indexMagic) : len(b)-4]}
	syms := make([]string, d.uvarint())
	for i := range syms {
		n := d.uvarint()
		syms[i] = d.bytes(n)
	}
	sym := func() string {
		i := d.uvarint()
		if i >= uint64(len(syms)) {
			d.err = errors.New("bad symbol")
			return ""
		}
		return syms[i]
	}
	blk.series = make([]blockSeries, d.uvarint())
	for i := range blk.series {
		nl := d.uvarint()
		for range nl {
			blk.series[i].lset = append(blk.series[i].lset, labels.Label{Name: sym(), Value: sym()})
		}
		nc := d.uvarint()
		for range nc {
			mint := d.varint()
			r := chunkRef{mint: mint, maxt: mint + int64(d.uvarint()), off: d.uvarint(), size: d.uvarint()}
			blk.series[i].chunks = append(blk.series[i].chunks, r)
		}
	}
	np := d.uvarint()
	for range np {
		k := labels.Label{Name: sym(), Value: sym()}
		n := d.uvarint()
		list := make([]uint32, n)
		prev := uint32(0)
		for j := range list {
			prev += uint32(d.uvarint())
			list[j] = prev
		}
		blk.postings[k] = list
	}
	if d.err != nil {
		return nil, fmt.Errorf("%s: %w", dir, d.err)
	}
	if blk.chunks, err = os.Open(filepath.Join(dir, "chunks")); err != nil {
		return nil, err
	}
	if st, err := blk.chunks.Stat(); err == nil {
		blk.sizes.chunks = st.Size()
	}
	return blk, nil
}

// Close closes the chunks file.
func (b *Block) Close() error { return b.chunks.Close() }

// Size returns the index and chunks file sizes.
func (b *Block) Size() (index, chunks int64) { return b.sizes.index, b.sizes.chunks }

func (b *Block) readChunk(r chunkRef) ([]byte, error) {
	buf := make([]byte, r.size)
	_, err := b.chunks.ReadAt(buf, int64(r.off))
	return buf, err
}

// selectIDs finds series matching all matchers, like the head does.
func (b *Block) selectIDs(ms []*labels.Matcher) []uint32 {
	var cand []uint32
	started := false
	for _, m := range ms {
		if m.Type != labels.Equal || m.Value == "" {
			continue
		}
		list := b.postings[labels.Label{Name: m.Name, Value: m.Value}]
		if !started {
			cand, started = append([]uint32(nil), list...), true
		} else {
			cand = intersect32(cand, list)
		}
	}
	if !started {
		cand = make([]uint32, len(b.series))
		for i := range cand {
			cand[i] = uint32(i)
		}
	}
	out := cand[:0]
	for _, id := range cand {
		if labels.MatchesAll(b.series[id].lset, ms) {
			out = append(out, id)
		}
	}
	return out
}

func intersect32(a, b []uint32) []uint32 {
	var out []uint32
	for i, j := 0, 0; i < len(a) && j < len(b); {
		switch {
		case a[i] == b[j]:
			out = append(out, a[i])
			i++
			j++
		case a[i] < b[j]:
			i++
		default:
			j++
		}
	}
	return out
}

// samples reads a series' samples in [mint, maxt].
func (b *Block) samples(id uint32, mint, maxt int64) ([]Point, error) {
	var out []Point
	for _, r := range b.series[id].chunks {
		if r.maxt < mint || r.mint > maxt {
			continue
		}
		data, err := b.readChunk(r)
		if err != nil {
			return nil, err
		}
		it := chunk.NewIterator(data)
		for it.Next() {
			t, v := it.At()
			if t >= mint && t <= maxt {
				out = append(out, Point{t, v})
			}
		}
		if it.Err() != nil {
			return nil, it.Err()
		}
	}
	return out, nil
}

type idxDecoder struct {
	b   []byte
	err error
}

func (d *idxDecoder) uvarint() uint64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Uvarint(d.b)
	if n <= 0 {
		d.err = errors.New("bad uvarint")
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *idxDecoder) varint() int64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Varint(d.b)
	if n <= 0 {
		d.err = errors.New("bad varint")
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *idxDecoder) bytes(n uint64) string {
	if d.err != nil || uint64(len(d.b)) < n {
		d.err = errors.New("short")
		return ""
	}
	s := string(d.b[:n])
	d.b = d.b[n:]
	return s
}
