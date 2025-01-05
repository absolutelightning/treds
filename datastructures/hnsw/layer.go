package hnsw

// Graph represents a single layer of the HNSW graph.
type GraphLayer struct {
	Nodes map[string]*Node // Nodes in the graph
}
