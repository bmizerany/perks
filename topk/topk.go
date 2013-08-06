// An implementation of the SpaceSaving algorithm for computing an approximate
// solution to the problem of finding the most frequent items in a
// data stream.
//
// This implementation is based on 'Efficient Computation of Frequent and Top-k
// Elements in Data Streams'
// (http://www.cs.ucsb.edu/research/tech_reports/reports/2005-23.pdf) but
// doesn't use the StreamSummary data structure introduced by the authors.
// Instead, it maintains a heap of the inserted elements, indexed by a plain old
// `map` for quick lookups.
//
// (For a simplified explanation of the algorithm, see
// http://boundary.com/blog/2013/05/14/approximate-heavy-hitters-the-spacesaving-algorithm/)
package topk

import (
	"container/heap"
	"sort"
)

// A key-count pair
type Element struct {
	// The key being tracked
	Value string
	// The (approximate) number of times this item has been seen
	Count int
	// The upper bound on the error of this count
	Error int
	// The index of this element in the tkheap.
	index int
}

// An element heap. Implements heap.Interface for convenience, but in practice,
// just the Init (aka heapify) method will be used. Since the heap will be
// initialized once and then remain a fixed size, elements need to be sifted
// down the heap as updates to their counts arrive.
type tkheap []*Element

func (h tkheap) Len() int {
	return len(h)
}

func (h tkheap) Less(i, j int) bool {
	return h[i].Count < h[j].Count
}

func (h tkheap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *tkheap) Push(x interface{}) {
	n := len(*h)
	e := x.(*Element)
	e.index = n
	*h = append(*h, e)
}

// Unused, but neccessary for heap.Interface
func (h *tkheap) Pop() interface{} {
	old := *h
	n := len(old)
	e := old[n-1]
	e.index = -1 // just in case
	*h = old[0 : n-1]
	return e
}

func siftDown(h tkheap, i int) {
	n := len(h)
	for {
		left := 2*i + 1
		if left > n || left < 0 { // overflow
			break
		}
		smallest := left
		if right := left + 1; right < n && h.Less(right, left) {
			smallest = right
		}

		if h.Less(i, smallest) {
			break
		}
		h.Swap(i, smallest)
		i = smallest
	}
}

// TODO: DOC ME
type Stream struct {
	n      int
	size   int
	tkheap tkheap
	lookup map[string]*Element
}

// Create a new sketch that monitors up to `size` elements
func New(size int) *Stream {
	s := new(Stream)
	s.size = size
	s.tkheap = make([]*Element, 0, size) // length 0, capacity k.
	s.lookup = make(map[string]*Element)
	return s
}

// The number of distinct items being monitored
func (s *Stream) Len() int {
	return len(s.tkheap)
}

// The number of items in the data stream
func (s *Stream) N() int {
	return s.n
}

// Insert an item into the sketch
func (s *Stream) Insert(x string) {
	s.InsertWeighted(x, 1)
}

// Insert an item with weight `n` into the sketch
func (s *Stream) InsertWeighted(x string, n int) {
	e, found := s.lookup[x]
	if found {
		e.Count += n
	} else {
		if len(s.tkheap) < s.size {
			e = &Element{Value: x, Count: n, Error: 0}
			s.tkheap.Push(e)
			s.lookup[x] = e
			// Don't bother heapifying until the queue is full
			if len(s.tkheap) == s.size {
				heap.Init(&s.tkheap)
			}
		} else {
			e = s.tkheap[0]
			// Make sure the Value of the element is swapped and the lookup is current
			delete(s.lookup, e.Value)
			s.lookup[x] = e
			e.Value = x
			// The error should be equal to the old count of the element
			e.Error = e.Count
			e.Count += n
		}
	}

	if len(s.tkheap) == s.size {
		siftDown(s.tkheap, e.index)
	}
}

// Get a copy of all of the items (and their errors) currently monitored by the
// sketch. Elements will be sorted by Value
func (s *Stream) Elements() []*Element {
	var toRet tkheap = make([]*Element, len(s.tkheap))
	copy(toRet, s.tkheap)
	sort.Sort(sort.Reverse(toRet))
	return toRet
}
