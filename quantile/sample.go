package quantile

// Sample holds an observed value and meta information for compression. JSON
// tags have been added for convenience.
type Sample struct {
	Value float64 `json:",string"`
	Width float64 `json:",string"`
	Delta float64 `json:",string"`
}

type Samples []Sample

func (a Samples) Len() int {
	return len(a)
}

func (a Samples) Less(i, j int) bool {
	return a[i].Value < a[j].Value
}

func (a Samples) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
