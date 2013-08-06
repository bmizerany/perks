package topk

import (
	"fmt"
	"math/rand"
	"testing"
)

func randString() string {
	return fmt.Sprintf("%x", rand.Int63())
}

// Check that with fewer than the required number of elements, counts are exact
func TestFewElements(t *testing.T) {
	sketch := New(20)
	data := make(map[string]int)
	for i := 0; i < 10; i++ {
		key := randString()
		data[key] = i
		sketch.InsertWeighted(key, i)
	}

	elements := sketch.Elements()
	if len(elements) != len(data) {
		t.Errorf("expected %d monitored elements, but got %d", len(data), len(elements))
	}
	for _, e := range elements {
		if wanted := data[e.Value]; wanted != e.Count {
			t.Errorf("%s wanted %d but got %d", e.Value, wanted, e.Value, e.Count)
		}
	}
}

// Test that an element is bumped approximately correctly
func TestApproximates(t *testing.T) {
	sketch := New(2)
	data := []string{"foo", "bar", "baz"}
	REPEAT := 100

	// Setup expected elements. NOTE: == compares the private `index` value too.
	// Gotta set that.
	expected := []*Element{
		&Element{Value: "baz", Count: (REPEAT + 1) /*actual inserts*/ + 1 /*error*/, Error: 1},
		&Element{Value: "bar", Count: 1, Error: 0},
	}
	for i, e := range expected {
		e.index = i
	}

	// Do the deed.
	for _, d := range data {
		sketch.Insert(d)
	}
	for i := 0; i < REPEAT; i++ {
		sketch.Insert("baz")
	}

	elements := sketch.Elements()
	if len(elements) != len(expected) {
		t.Errorf("expected exactly %d elements", len(expected))
	}

	for i, e := range expected {
		actual := elements[i]
		if *e != *actual {
			t.Errorf("expected: %v, actual: %v", e, actual)
		}
	}
}

// Test that the heap property works
func TestHeap(t *testing.T) {
	sketch := New(5)
	data := []string{"one", "two", "three", "four", "five", "six"}

	// First three items in the sketch have ~~big~~ weight. Next two are
	// decreasing, but not equal. The last insert should bump up the sketch into
	// approximate region, and the count of "six" (which replaces "five") should
	// be larger than the count of "four".
	big := 300
	medium := 100
	small := medium - 1

	for i := 0; i < len(data); i++ {
		switch i {
		default:
			sketch.InsertWeighted(data[i], big + i)
		case 3:
			sketch.InsertWeighted(data[i], medium)
		case 4:
			sketch.InsertWeighted(data[i], small)
		case 5:
			sketch.InsertWeighted(data[i], 5)
		}
	}

	expected := []*Element{
		&Element{Value: "three", Count: big + 2, Error: 0},
		&Element{Value: "two", Count: big + 1, Error: 0},
		&Element{Value: "one", Count: big, Error: 0},
		&Element{Value: "six", Count: small + 5, Error: small},
		&Element{Value: "four", Count: medium, Error: 0},
	}
	for i, e := range expected {
		e.index = i
	}

	elements := sketch.Elements()
	if len(elements) != len(expected) {
		t.Errorf("expected exactly %d elements", len(expected))
	}


	for i, e := range expected {
		actual := elements[i]
		if *e != *actual {
			t.Errorf("expected: %v, actual: %v", e, actual)
		}
	}
}
