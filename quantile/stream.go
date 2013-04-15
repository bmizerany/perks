// The quantile package implements Effective Computation of Biased Quantiles
// over Data Streams http://www.cs.rutgers.edu/~muthu/bquant.pdf
//
// This package is useful for calculating targeted quantiles for large datasets
// within low memory and CPU bounds. You trade a small amount of accuracy in
// rank selection for efficiency. This is usually fine for a lot of large
// datasets.
//
// Multiple Stream's can be merged before a Query, allowing clients to be
// distributed across threads. See Stream.Merge and Stream.Samples.
package quantile

import (
	"container/list"
	"math"
	"sort"
)

// Sample holds an observed value and meta information for compression. JSON
// tags have been added for convenience.
type Sample struct {
	Value float64 `json:",string"`
	Width float64 `json:",string"`
	Delta float64 `json:",string"`
}

// Samples represents a slice of samples. It implements sort.Interface.
type Samples []Sample

func (a Samples) Len() int {
	return len(a)
}

func (a Samples) Less(i, j int) bool {
	return a[i].Value < a[j].Value
}

func (a Samples) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

type Invariant func(s *stream, r float64) float64

// Biased returns an Invarient for high-biased (>50th) quantiles not known a
// priori with associated error bounds e.
func Biased(e float64) Invariant {
	return func(s *stream, r float64) float64 {
		return 2 * e * r
	}
}

// Targeted returns an Invarient that is only concerned with a set
// of quantile values with associated error bounds that are supplied a priori.
func Targeted(e float64, quantiles ...float64) Invariant {
	return func(s *stream, r float64) float64 {
		var m float64 = math.MaxFloat64
		var f float64
		for _, q := range quantiles {
			if q*s.n <= r {
				f = (2 * e * r) / q
			} else {
				f = (2 * e * (s.n - r)) / (1 - q)
			}
			m = math.Min(m, f)
		}
		return m
	}
}

// Stream calculates quantiles for a stream of float64s.
type Stream struct {
	*stream
	b Samples
}

// New returns an initialized Stream with the invarient ƒ.
func New(ƒ Invariant) *Stream {
	x := &stream{ƒ: ƒ, l: list.New()}
	return &Stream{x, make(Samples, 0, 500)}
}

// Insert inserts v into the stream.
func (s *Stream) Insert(v float64) {
	s.insert(Sample{Value: v, Width: 1})
}

func (s *Stream) insert(sample Sample) {
	s.b = append(s.b, sample)
	if len(s.b) == cap(s.b) {
		s.flush()
		s.compress()
	}
}

// Query returns the calculated qth percentiles value. If q is not in the set
// of quantiles provided to New, Query will have non-deterministic results.
func (s *Stream) Query(q float64) float64 {
	if s.flushed() {
		// Fast path when there hasn't been enough data for a flush;
		// this also yeilds better accuracy for small sets of data.
		i := float64(len(s.b)) * q
		return s.b[int(i)].Value
	}
	s.flush()
	return s.stream.query(q)
}

// Merge merges samples into the underlying streams samples. This is handy when
// merging multiple streams from separate threads.
func (s *Stream) Merge(samples Samples) {
	s.stream.merge(samples)
}

// Reset reinitializes and clears the list reusing the samples buffer memory.
func (s *Stream) Reset() {
	s.stream.Init()
	s.b = s.b[:0]
}

// Samples returns stream samples held by s.
func (s *Stream) Samples() Samples {
	if !s.flushed() {
		return s.b
	}
	return s.stream.samples()
}

func (s *Stream) flush() {
	sort.Sort(s.b)
	s.stream.merge(s.b)
	s.b = s.b[:0]
}

func (s *Stream) flushed() bool {
	return s.stream.l.Len() == 0
}

type stream struct {
	n float64
	l *list.List
	ƒ Invariant
}

func (s *stream) Init() {
	s.l.Init()
	s.n = 0
}

func (s *stream) insert(v float64) {
	fn := s.mergeFunc()
	fn(v, 1)
}

func (s *stream) merge(samples Samples) {
	fn := s.mergeFunc()
	for _, s := range samples {
		fn(s.Value, s.Width)
	}
}

func (s *stream) mergeFunc() func(v, w float64) {
	// NOTE: I used a goto over defer because it bought me a few extra
	// nanoseconds. I know. I know.
	var r float64
	e := s.l.Front()
	return func(v, w float64) {
		for ; e != nil; e = e.Next() {
			c := e.Value.(*Sample)
			if c.Value > v {
				sm := &Sample{v, w, math.Floor(s.ƒ(s, r)) - 1}
				s.l.InsertBefore(sm, e)
				goto inserted
			}
			r += c.Width
		}
		s.l.PushBack(&Sample{v, w, 0})
	inserted:
		s.n += w
	}
}

// Count returns the total number of samples observed in the stream
// since initialization.
func (s *stream) Count() int {
	return int(s.n)
}

func (s *stream) query(q float64) float64 {
	e := s.l.Front()
	t := math.Ceil(q * s.n)
	t += math.Ceil(s.ƒ(s, t) / 2)
	p := e.Value.(*Sample)
	e = e.Next()
	r := float64(0)
	for e != nil {
		c := e.Value.(*Sample)
		if r+c.Width+c.Delta > t {
			return p.Value
		}
		r += p.Width
		p = c
		e = e.Next()
	}
	return p.Value
}

func (s *stream) compress() {
	if s.l.Len() < 2 {
		return
	}
	e := s.l.Back()
	x := e.Value.(*Sample)
	r := s.n - 1 - x.Width
	e = e.Prev()
	for e != nil {
		c := e.Value.(*Sample)
		if c.Width+x.Width+x.Delta <= s.ƒ(s, r) {
			x.Width += c.Width
			o := e
			e = e.Prev()
			s.l.Remove(o)
		} else {
			x = c
			e = e.Prev()
		}
		r -= c.Width
	}
}

func (s *stream) samples() Samples {
	samples := make(Samples, 0, s.l.Len())
	for e := s.l.Front(); e != nil; e = e.Next() {
		samples = append(samples, *e.Value.(*Sample))
	}
	return samples
}
