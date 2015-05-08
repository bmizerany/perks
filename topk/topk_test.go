package topk

import (
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"testing"
)

func TestTopK(t *testing.T) {
	stream := New(10)
	ss := []*Stream{New(10), New(10), New(10)}
	m := make(map[string]int)
	for _, s := range ss {
		for i := 0; i < 1e6; i++ {
			v := fmt.Sprintf("%x", int8(rand.ExpFloat64()))
			s.Insert(v)
			m[v]++
		}
		stream.Merge(s.Query())
	}

	var sm Samples
	for x, s := range m {
		sm = append(sm, &Element{x, s})
	}
	sort.Sort(sort.Reverse(sm))

	g := stream.Query()
	if len(g) != 10 {
		t.Fatalf("got %d, want 10", len(g))
	}
	for i, e := range g {
		if sm[i].Value != e.Value {
			t.Errorf("at %d: want %q, got %q", i, sm[i].Value, e.Value)
		}
	}
}

func TestTopElements(t *testing.T) {
	N := 4

	// A stream keeps N+1 items internally, so prime it with N+1 items. Use the
	// integers 1 to N+1 and give int n frequency n.
	stream := New(N)
	for i := 1; i <= N+1; i++ {
		for j := 0; j < i; j++ {
			stream.Insert(fmt.Sprintf("%d", i))
		}
	}

	// Make sure the insertion went ok.
	for _, sample := range stream.Query() {
		actual, err := strconv.Atoi(sample.Value)
		if err != nil {
			panic(err)
		}
		if actual != sample.Count {
			t.Fatalf("expected element %s to have value %d: got %d", sample.Value, actual, sample.Count)
		}
	}

	// Insert 1 twice more so that "2" is the smallest element with a count of 2,
	// and then insert N * 2 four times. "2" should drop out of the top k, and
	// N * 2 should appear in the sample.
	newElement := fmt.Sprintf("%d", N*2)
	stream.Insert("1")
	stream.Insert("1")
	stream.Insert(newElement)
	stream.Insert(newElement)
	stream.Insert(newElement)
	stream.Insert(newElement)

	var sawNewElement bool
	for _, sample := range stream.Query() {
		if sample.Value == "2" {
			t.Fatalf("expected 2 to drop")
		}
		if sample.Value == newElement {
			sawNewElement = true
		}
	}
	if !sawNewElement {
		t.Fatalf("didn't see newly inserted element in the topk")
	}
}

func TestQuery(t *testing.T) {
	queryTests := []struct {
		value    string
		expected int
	}{
		{"a", 1},
		{"b", 2},
		{"c", 2},
	}

	stream := New(2)
	for _, tt := range queryTests {
		stream.Insert(tt.value)
		if n := len(stream.Query()); n != tt.expected {
			t.Errorf("want %d, got %d", tt.expected, n)
		}
	}
}
