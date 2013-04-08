package quantile_test

import (
	"bufio"
	"fmt"
	"github.com/bmizerany/perks/quantile"
	"io"
	"log"
	"os"
	"strconv"
)

func Example() {
	f, err := os.Open("exampledata.txt")
	if err != nil {
		log.Fatal(err)
	}
	bio := bufio.NewReader(f)

	q := quantile.New(0.1, 0.50, 0.90, 0.99)
	for {
		line, err := bio.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}
		n, err := strconv.ParseFloat(line[:len(line)-1], 64)
		if err != nil {
			log.Fatal(err)
		}
		q.Insert(n)
	}

	fmt.Println("perc50:", q.Query(0.50))
	fmt.Println("perc90:", q.Query(0.90))
	fmt.Println("perc99:", q.Query(0.99))
	fmt.Println("min:", q.Min())
	fmt.Println("max:", q.Max())
	// Output:
	// perc50: 5
	// perc90: 17
	// perc99: 1545
	// min: 1
	// max: 1545
}
