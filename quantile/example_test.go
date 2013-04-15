// +build go1.1
package quantile_test

import (
	"bufio"
	"fmt"
	"github.com/bmizerany/perks/quantile"
	"log"
	"os"
	"strconv"
)

func Example() {
	ch := make(chan float64)
	go readFloats(ch)

	// Compute the 50th, 90th, and 99th percentile for a stream within the set error epsilon of 0.01.
	q := quantile.New(quantile.Targeted(0.01, 0.50, 0.90, 0.99))
	for v := range ch {
		q.Insert(v)
	}

	fmt.Println("perc50:", q.Query(0.50))
	fmt.Println("perc90:", q.Query(0.90))
	fmt.Println("perc99:", q.Query(0.99))
	fmt.Println("count:", q.Count())
	// Output:
	// perc50: 5
	// perc90: 14
	// perc99: 40
	// count: 2388
}

func readFloats(ch chan<- float64) {
	f, err := os.Open("exampledata.txt")
	if err != nil {
		log.Fatal(err)
	}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		b := sc.Bytes()
		v, err := strconv.ParseFloat(string(b), 64)
		if err != nil {
			log.Fatal(err)
		}
		ch <- v
	}
	if sc.Err() != nil {
		log.Fatal(sc.Err())
	}
	close(ch)
}
