package topk

import (
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
	k   int
	mon map[string]*Element

	// the minimum Element
	min *Element
}

func New(k int) *Stream {
	s := new(Stream)
	s.k = k
	s.mon = make(map[string]*Element)
	s.min = &Element{}

	// Track k+1 so that less frequenet items contended for that spot,
	// resulting in k being more accurate.
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
	if e != nil {
		e.Count++
	} else {
		if len(s.mon) < s.k+1 {
			e = &Element{Value: in.Value, Count: in.Count}
			s.mon[in.Value] = e
		} else {
			e = s.min
			delete(s.mon, e.Value)
			e.Value = in.Value
			e.Count += in.Count
			s.min = e
		}
	}
	if e.Count < s.min.Count {
		s.min = e
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
