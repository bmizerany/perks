package quantile

import (
	"math"
	"math/rand"
	"sort"
	"testing"
)

func TestQuantRandQuery(t *testing.T) {
	s := New(0.01, 0.5, 0.90, 0.99)
	a := make([]float64, 0, 1e5)
	rand.Seed(42)
	for i := 0; i < cap(a); i++ {
		v := float64(rand.Int63())
		s.Insert(v)
		a = append(a, v)
	}
	t.Logf("len: %d", s.Count())
	sort.Float64s(a)
	w := getPerc(a, 0.50)
	if g := s.Query(0.50); math.Abs(w-g)/w > 0.03 {
		t.Errorf("perc50: want %v, got %v", w, g)
		t.Logf("e: %f", math.Abs(w-g)/w)
	}
	w = getPerc(a, 0.90)
	if g := s.Query(0.90); math.Abs(w-g)/w > 0.03 {
		t.Errorf("perc90: want %v, got %v", w, g)
		t.Logf("e: %f", math.Abs(w-g)/w)
	}
	w = getPerc(a, 0.99)
	if g := s.Query(0.99); math.Abs(w-g)/w > 0.03 {
		t.Errorf("perc99: want %v, got %v", w, g)
		t.Logf("e: %f", math.Abs(w-g)/w)
	}
}

func TestQuantRandMergeQuery(t *testing.T) {

	ch := make(chan float64)
	done := make(chan Interface)
	for i := 0; i < 2; i++ {
		go func() {
			s := New(0.01, 0.5, 0.90, 0.99)
			for v := range ch {
				s.Insert(v)
			}
			done <- s
		}()
	}

	rand.Seed(42)
	a := make([]float64, 0, 1e6)
	for i := 0; i < cap(a); i++ {
		v := float64(rand.Int63())
		a = append(a, v)
		ch <- v
	}
	close(ch)

	s := <-done
	o := <-done
	s.Merge(o.Samples())

	t.Logf("len: %d", s.Count())
	sort.Float64s(a)
	w := getPerc(a, 0.50)
	if g := s.Query(0.50); math.Abs(w-g)/w > 0.03 {
		t.Errorf("perc50: want %v, got %v", w, g)
		t.Logf("e: %f", math.Abs(w-g)/w)
	}
	w = getPerc(a, 0.90)
	if g := s.Query(0.90); math.Abs(w-g)/w > 0.03 {
		t.Errorf("perc90: want %v, got %v", w, g)
		t.Logf("e: %f", math.Abs(w-g)/w)
	}
	w = getPerc(a, 0.99)
	if g := s.Query(0.99); math.Abs(w-g)/w > 0.03 {
		t.Errorf("perc99: want %v, got %v", w, g)
		t.Logf("e: %f", math.Abs(w-g)/w)
	}
}

func getPerc(x []float64, p float64) float64 {
	k := int(float64(len(x)) * p)
	return x[k]
}
