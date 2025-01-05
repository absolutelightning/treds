package hnsw

type Vector []float64

// Node represents a single node in the graph.
type Node struct {
	ID        string             // Unique ID for the node - UUID
	Layer     int                // The layer in which the node exists
	Neighbors map[string]float64 // Neighbor IDs with their distances
	Value     Vector             // The feature vector of the node
}
