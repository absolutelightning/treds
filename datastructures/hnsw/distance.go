package hnsw

import "math"

func EuclideanDistance(a, b Vector) float64 {
	if len(a) != len(b) {
		panic("dimension mismatch")
	}
	var sum float64
	for i := range a {
		diff := a[i] - b[i]
		sum += diff * diff
	}
	return math.Sqrt(sum)
}
