package histogram

import (
	"math/rand"
	"testing"
)

func TestHistogram(t *testing.T) {
	numPoints := int(100)
	maxBins := 3
	h := New(maxBins)
	for i := 0; i < numPoints; i++ {
		f := rand.ExpFloat64()
		h.Insert(f)
	}

	bins := h.Bins()
	t.Log("n", h.res.n)
	binCounts := 0
	if g := len(bins); g > maxBins {
		for _, b := range bins {
			binCounts += b.Count
			t.Logf("%+v", b)
		}
		t.Fatalf("got %d bins, wanted <= %d", g, maxBins)
	}

	for _, b := range bins {
		binCounts += b.Count
	}
	if binCounts != numPoints {
		t.Fatalf("binned %d points, wanted %d", binCounts, numPoints)
	}
}
