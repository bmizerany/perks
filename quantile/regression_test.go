package quantile

import (
	"math"
	"math/rand"
	"testing"
	"time"
)

func eqFloat(a, b, diff float64) (isEq bool, eps float64) {
	eps = math.Abs(a-b) / a
	isEq = eps < diff
	return
}

func similarQueries(t *testing.T, tgts []float64, gt, tested *Stream, diff float64) bool {
	same := true
	for _, tgt := range tgts {
		w := gt.Query(tgt)
		g := tested.Query(tgt)
		if isEq, eps := eqFloat(w, g, diff); !isEq {
			t.Errorf("perc%2.0f: want %v, got %v", tgt*100, w, g)
			t.Logf("e: %f", eps)
			same = false
		}
	}
	return same
}

// https://github.com/bmizerany/perks/issues/8
func TestDoesntDegradeAfterResets(t *testing.T) {
	// query should be wildly different
	diff := 0.99

	// I have a long-running application which has two streams
	// (set to report 50%, 90%, and 99% quantiles)
	targets := []float64{0.5, 0.9, 0.99}
	toReset := NewTargeted(targets...)
	groundTruth := NewTargeted(targets...)

	// that both receive the same input data (latencies as float64
	// milliseconds from database operations).
	minDbQPS, maxDbQPS := 1, 50 // wild guesses
	queryThisSec := func() int { return rand.Intn(maxDbQPS-minDbQPS) + minDbQPS }

	// wild guess: should happen within a (long!) month
	secondsToFail := int((time.Hour * 1).Seconds())

	for sec := 0; sec < secondsToFail; sec++ {

		qps := queryThisSec()
		for i := 0; i < qps; i++ {
			d := rand.Float64()
			toReset.Insert(d)
			groundTruth.Insert(d)
		}
		// One stream gets reset each second after reporting a few quantiles
		// the other one reports at the same time but never gets reset.
		if ok := similarQueries(t, targets, groundTruth, toReset, diff); !ok {
			t.Logf("failed at second %d, %d qps", sec, qps)
		}
		toReset.Reset()
	}
}
