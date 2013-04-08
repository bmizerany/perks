// The quantile package implements Effective Computation of Biased Quantiles
// over Data Streams http://www.cs.rutgers.edu/~muthu/bquant.pdf
//
// This package is useful for calculating targeted quantiles for large datasets
// within low memory and cpu bounds. This means your trading a small amount of
// accuracy in rank selection, for efficiency.
//
// NOTE: Multiple streams can be merged before a Query, allowing clients to be distributed across threads.
package quantile

import (
	"container/list"
	"math"
)

type Interface interface {
	// Query returns the calculated qth percentiles value. Calling Query
	// with q not in the set quantiles given to New will have non-deterministic
	// results.
	Query(q float64) float64

	// Insert inserts v into the list.
	Insert(v float64)

	// Merge merges samples into the list. This handy when
	// merging multiple streams from seperate threads.
	Merge(samples Samples)

	// Samples returns a copy of the list of samples kept from the data
	// stream.
	Samples() Samples

	// Count returns the total number of samples observed in the stream
	// since initialization.
	Count() int

	// Init initializes or clears the list.
	Init()

	// Min returns the minimum value observed in the list.
	Min() float64

	// Max returns the maximum value observed in the list.
	Max() float64
}

type stream struct {
	e   float64
	q   []float64
	n   float64
	l   *list.List
	max float64
}

// New returns an initialized stream targeted at quantiles for error e. e is usually 0.01.
func New(e float64, quantiles ...float64) Interface {
	x := &stream{e: e, q: quantiles, l: list.New()}
	return &buffer{x, make(Samples, 0, 500)}
}

func (qt *stream) Init() {
	qt.l.Init()
	qt.n = 0
}

func (qt *stream) ƒ(r float64) float64 {
	var m float64 = math.MaxFloat64
	var f float64
	for _, q := range qt.q {
		if q*qt.n <= r {
			f = (2 * qt.e * r) / q
		} else {
			f = (2 * qt.e * (qt.n - r)) / (1 - q)
		}
		m = math.Min(m, f)
	}
	return m
}

func (qt *stream) Insert(v float64) {
	fn := qt.mergeFunc()
	fn(v, 1)
}

func (qt *stream) Merge(samples Samples) {
	fn := qt.mergeFunc()
	for _, s := range samples {
		fn(s.Value, s.Width)
	}
}

func (qt *stream) mergeFunc() func(v, w float64) {
	// NOTE: I used a goto over defer because it bought me a few extra
	// nanoseconds. I know. I know.
	var r float64
	e := qt.l.Front()
	return func(v, w float64) {
		if v > qt.max {
			qt.max = v
		}

		for ; e != nil; e = e.Next() {
			c := e.Value.(*Sample)
			if c.Value > v {
				s := &Sample{v, w, math.Floor(qt.ƒ(r)) - 1}
				qt.l.InsertBefore(s, e)
				goto inserted
			}
			r += c.Width
		}
		qt.l.PushBack(&Sample{v, w, 0})
	inserted:
		qt.n += w
	}
}

func (qt *stream) Count() int {
	return int(qt.n)
}

func (qt *stream) Query(q float64) float64 {
	e := qt.l.Front()
	t := math.Ceil(q * qt.n)
	t += math.Ceil(qt.ƒ(t) / 2)
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

func (qt *stream) compress() {
	if qt.l.Len() < 2 {
		return
	}
	e := qt.l.Back()
	x := e.Value.(*Sample)
	r := qt.n - 1 - x.Width
	e = e.Prev()
	for e != nil {
		c := e.Value.(*Sample)
		if c.Width+x.Width+x.Delta <= qt.ƒ(r) {
			x.Width += c.Width
			o := e
			e = e.Prev()
			qt.l.Remove(o)
		} else {
			x = c
			e = e.Prev()
		}
		r -= c.Width
	}
}

func (qt *stream) Samples() Samples {
	samples := make(Samples, 0, qt.l.Len())
	for e := qt.l.Front(); e != nil; e = e.Next() {
		samples = append(samples, *e.Value.(*Sample))
	}
	return samples
}

// Min returns the mininmul value observed in the stream.
func (qt *stream) Min() float64 {
	if e := qt.l.Front(); e != nil {
		return e.Value.(*Sample).Value
	}
	return math.NaN()
}

// Max returns the maximum value observed in the stream within the error epsilon.
func (qt *stream) Max() float64 {
	if qt.l.Len() > 0 {
		return qt.max
	}
	return math.NaN()
}
