package quantile

import (
	"sort"
)

type buffer struct {
	*stream
	b Samples
}

func (buf *buffer) Insert(v float64) {
	buf.insert(Sample{Value: v, Width: 1})
}

func (buf *buffer) insert(sample Sample) {
	buf.b = append(buf.b, sample)
	if len(buf.b) == cap(buf.b) {
		buf.flush()
		buf.compress()
	}
}

func (buf *buffer) Query(q float64) float64 {
	if buf.flushed() {
		// Fast path when there hasn't been enough data for a flush;
		// this also yeilds better accuracy for small sets of data.
		i := float64(len(buf.b)) * q
		return buf.b[int(i)].Value
	}
	buf.flush()
	return buf.stream.Query(q)
}

func (buf *buffer) Merge(samples Samples) {
	buf.stream.Merge(samples)
}

func (buf *buffer) Init() {
	buf.stream.Init()
	buf.b = buf.b[:0]
}

func (buf *buffer) Samples() Samples {
	if !buf.flushed() {
		return buf.b
	}
	return buf.stream.Samples()
}

func (buf *buffer) flush() {
	sort.Sort(buf.b)
	buf.stream.Merge(buf.b)
	buf.b = buf.b[:0]
}

func (buf *buffer) flushed() bool {
	return buf.stream.l.Len() == 0
}
