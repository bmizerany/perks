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
		sm = append(sm, &Element{Value: x, Count: s})
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

	// Insert 1 three more times so that "2" is the smallest element with a count
	// of 2, followed by "3" with a count of 3. Insert N * 2. "2" and "3" should
	// not appear in the top k. N * 2 should appear with a value of 4 + 1.
	newElement := fmt.Sprintf("%d", N*2)
	stream.Insert("1")
	stream.Insert("1")
	stream.Insert("1")
	stream.Insert(newElement)

	var sawNewElement bool
	for _, sample := range stream.Query() {
		switch sample.Value {
		case "2", "3":
			t.Fatalf("saw elements that should have dropped")
		case "1":
			if sample.Count != 4 {
				t.Fatalf("expected '1' to have a count of 4")
			}
		case newElement:
			sawNewElement = true
			if sample.Count != 5 {
				t.Fatalf("expected new element to have a count of 5")
			}
		default:
			// Do nothing.
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
