package hnsw_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"treds/datastructures/hnsw"
)

// Define Vector type and DistanceFunc type to match the library
type Vector = hnsw.Vector

// EuclideanDistance calculates the Euclidean distance between two vectors.
func EuclideanDistance(a, b Vector) float64 {
	sum := 0.0
	for i := range a {
		diff := a[i] - b[i]
		sum += diff * diff
	}
	return math.Sqrt(sum)
}

func TestHNSWInsert(t *testing.T) {
	h := hnsw.NewHNSW(5, 0.5, 10, EuclideanDistance)

	vector := Vector{1.0, 2.0}
	h.Insert(vector)

	assert.NotNil(t, h.EntryPoint, "EntryPoint should not be nil after first insert")
	assert.Equal(t, 1, len(h.Layers), "Insert should create at least one layer")
	assert.Equal(t, vector, h.EntryPoint.Value, "Inserted vector should match the EntryPoint value")
}

func TestHNSWGraphAfterThreeInserts(t *testing.T) {
	h := hnsw.NewHNSW(2, 0.5, 10, EuclideanDistance)

	// Insert three nodes
	node1 := Vector{1.0, 2.0}
	node2 := Vector{2.0, 3.0}
	node3 := Vector{3.0, 4.0}

	h.Insert(node1)
	h.Insert(node2)
	h.Insert(node3)

	// Check the number of layers
	assert.GreaterOrEqual(t, len(h.Layers), 1, "There should be at least one layer")
	assert.LessOrEqual(t, len(h.Layers), 3, "There should not be more than three layers")

	// Check connections
	for layer := 0; layer < len(h.Layers); layer++ {
		for _, node := range h.Layers[layer].Nodes {
			assert.LessOrEqual(t, len(node.Neighbors), h.MaxConnections(layer), "Node should not have more neighbors than MaxConnections")
		}
	}

	// Check EntryPoint
	assert.NotNil(t, h.EntryPoint, "EntryPoint should not be nil")
}

func TestHNSWSearch(t *testing.T) {
	h := hnsw.NewHNSW(6, 0.5, 4, EuclideanDistance)

	// Insert multiple vectors
	h.Insert(Vector{1.0, 2.0})
	h.Insert(Vector{2.0, 3.0})
	h.Insert(Vector{3.0, 4.0})

	// Perform search
	results := h.Search(Vector{1.5, 2.5}, 2)

	assert.Len(t, results, 2, "Search should return the requested number of results")
	assert.NotEqual(t, results[0], results[1], "Search results should be distinct")
}

func TestHNSWDelete(t *testing.T) {
	h := hnsw.NewHNSW(5, 0.5, 10, EuclideanDistance)

	vector := Vector{1.0, 2.0}
	h.Insert(vector)

	// Ensure the vector is inserted
	assert.Equal(t, vector, h.EntryPoint.Value, "Inserted vector should match the EntryPoint value")

	// Delete the vector
	deleted := h.Delete(h.EntryPoint.ID)
	assert.True(t, deleted, "Delete should return true for a valid node")

	// Ensure the graph is empty
	assert.Nil(t, h.EntryPoint, "EntryPoint should be nil after deleting the only node")
}
