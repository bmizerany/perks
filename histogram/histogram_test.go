package histogram

import (
	"math/rand"
	"testing"
)

func TestHistogram(t *testing.T) {
	h := New(10)
	for i := 0; i < 1e6; i++ {
		f := rand.ExpFloat64()
		h.Insert(f)
	}

	bins := h.Bins()
	if g := len(bins); g != 10 {
		t.Fatalf("got %d, want %d", g, 10)
	}
}
