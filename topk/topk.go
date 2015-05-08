package topk

import (
	"container/heap"
	"sort"
)

// http://www.cs.ucsb.edu/research/tech_reports/reports/2005-23.pdf

type Element struct {
	Value string
	Count int

	// The index of the item in a Samples array. Needed by the heap.Interface for
	// calling heap.Fix to sift up/down. Maintained by Samples.Swap
	index int
}

// implements heap.Interface
type Samples []*Element

func (sm Samples) Len() int {
	return len(sm)
}

func (sm Samples) Less(i, j int) bool {
	return sm[i].Count < sm[j].Count
}

func (sm Samples) Swap(i, j int) {
	sm[i], sm[j] = sm[j], sm[i]
	sm[i].index = i
	sm[j].index = j
}

func (sm *Samples) Push(x interface{}) {
	element := x.(*Element)
	element.index = len(*sm)
	*sm = append(*sm, element)
}

// Pop should never be called, so make it panic.
func (sm *Samples) Pop() interface{} {
	panic("Samples.Pop() should never be called")
}

type Stream struct {
	k    int
	mon  map[string]*Element
	heap Samples
}

func New(k int) *Stream {
	s := new(Stream)
	s.k = k

	// Track k+1 so that less frequenet items contended for that spot,
	// resulting in k being more accurate.
	s.mon = make(map[string]*Element)
	// Don't need to call heap.Init(&s.heap) here, since the heap starts empty.
	s.heap = make(Samples, 0, k+1)
	heap.Init(&s.heap)

	return s
}

func (s *Stream) Insert(x string) {
	s.insert(&Element{Value: x, Count: 1})
}

func (s *Stream) Merge(sm Samples) {
	for _, e := range sm {
		s.insert(e)
	}
}

func (s *Stream) insert(in *Element) {
	e := s.mon[in.Value]

	// Already tracking the element. Update the value and resort the heap.
	if e != nil {
		e.Count += in.Count
		heap.Fix(&s.heap, e.index)
	} else {
		if len(s.mon) < s.k+1 {
			// New value, fewer than k+1 values being tracked. Add a new element.
			newElement := &Element{Value: in.Value, Count: in.Count}
			s.mon[in.Value] = newElement
			s.heap.Push(newElement)
		} else {
			// New value, already tracking k+1 values. Replace the Value of the minimum
			// element with the new element and then increment.

			min := s.heap[0]
			//  Fix the heap.
			min.Value = in.Value
			min.Count += in.Count
			heap.Fix(&s.heap, min.index)
			// Update the index map.
			delete(s.mon, min.Value)
			s.mon[in.Value] = min
		}
	}
}

func (s *Stream) Query() Samples {
	var sm Samples
	for _, e := range s.mon {
		sm = append(sm, e)
	}
	sort.Sort(sort.Reverse(sm))

	if len(sm) < s.k {
		return sm
	}
	return sm[:s.k]
}
