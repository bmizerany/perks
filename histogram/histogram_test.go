package histogram

import (
	"math/rand"
	"testing"
)

func TestHistogram(t *testing.T) {
	const numPoints = 1e6
	const maxBins = 3

	h := New(maxBins)
	for i := 0; i < numPoints; i++ {
		f := rand.ExpFloat64()
		h.Insert(f)
	}

	bins := h.Bins()
	if g := len(bins); g > maxBins {
		t.Fatalf("got %d bins, wanted <= %d", g, maxBins)
	}

	binCounts := count(h.Bins())
	if binCounts != numPoints {
		t.Fatalf("binned %d points, wanted %d", binCounts, numPoints)
	}
}

func count(bins Bins) int {
	binCounts := 0
	for _, b := range bins {
		binCounts += b.Count
	}
	return binCounts
}
