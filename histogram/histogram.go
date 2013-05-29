package histogram

import (
	"container/heap"
	"sort"
)

type Bin struct {
	Count int
	Sum   float64
}

func (b *Bin) Update(x *Bin) {
	b.Count += x.Count
	b.Sum += x.Sum
}

func (b *Bin) Mean() float64 {
	return b.Sum / float64(b.Count)
}

type Bins []*Bin

func (bs Bins) Len() int           { return len(bs) }
func (bs Bins) Less(i, j int) bool { return bs[i].Mean() < bs[j].Mean() }
func (bs Bins) Swap(i, j int)      { bs[i], bs[j] = bs[j], bs[i] }

func (bs *Bins) Push(x interface{}) {
	*bs = append(*bs, x.(*Bin))
}

func (bs *Bins) Pop() interface{} {
	old := *bs
	n := len(old)
	x := old[n-1]
	*bs = old[0 : n-1]
	return x
}

type Histogram struct {
	res *reservoir
}

func New(maxBins int) *Histogram {
	return &Histogram{res: newReservoir(maxBins)}
}

func (h *Histogram) Insert(f float64) {
	h.insert(&Bin{1, f})
}

func (h *Histogram) insert(bin *Bin) {
	h.res.insert(bin)
	h.res.compress()
}

func (h *Histogram) Bins() Bins {
	return h.res.bins
}

type reservoir struct {
	n       int
	maxBins int
	bins    Bins
}

func newReservoir(maxBins int) *reservoir {
	return &reservoir{maxBins: maxBins, bins: make(Bins, 0)}
}

func (r *reservoir) insert(bin *Bin) {
	r.n += bin.Count
	i := sort.Search(len(r.bins), func(i int) bool {
		return r.bins[i].Mean() >= bin.Mean()
	})
	if i < 0 || i == r.bins.Len() {
		heap.Push(&r.bins, bin)
		return
	}
	r.bins[i].Update(bin)
}

func (r *reservoir) compress() {

}
