package hnsw

import (
	"math"
	"math/rand"
	"sort"
	"sync"

	"github.com/absolutelightning/gods/queues/priorityqueue"
	"github.com/absolutelightning/gods/utils"
	"github.com/google/uuid"
	"golang.org/x/exp/maps"
)

type DistanceFunc func(a, b Vector) float64

// HNSW represents the entire hierarchical graph.
type HNSW struct {
	Layers        []*GraphLayer
	MaxNeighbors  int          // Maximum number of neighbors (M)
	MaxNeighbors0 int          // Maximum neighbors at layer 0 (Mmax0)
	LayerFactor   float64      // Probability factor for creating higher layers
	EfSearch      int          // Number of candidates during search
	DistFunc      DistanceFunc // Distance function
	lock          sync.Mutex   // Lock for thread-safe operations
	EntryPoint    *Node        // Entry point into the graph
}

type SearchCandidate struct {
	NodeID   string
	Distance float64
}

func NewHNSW(maxNeighbors int, layerFactor float64, efSearch int, distanceFunc DistanceFunc) *HNSW {
	return &HNSW{
		Layers:       make([]*GraphLayer, 0),
		MaxNeighbors: maxNeighbors,
		LayerFactor:  layerFactor,
		EfSearch:     efSearch,
		DistFunc:     distanceFunc,
	}
}

// generateID generates a unique ID for a new node.
func (h *HNSW) generateID() string {
	return uuid.New().String()
}

// randomLevel generates a random level for a new node.
func (h *HNSW) randomLevel() int {
	level := 0
	for rand.Float64() < h.LayerFactor {
		level++
	}
	return level
}

// Insert adds a new element `q` into the HNSW graph.
func (h *HNSW) Insert(vector Vector) {
	h.lock.Lock()
	defer h.lock.Unlock()

	level := h.randomLevel()
	node := &Node{
		ID:        h.generateID(),
		Value:     vector,
		Neighbors: make(map[string]float64),
		Layer:     level,
	}

	// Add new layers if needed
	for len(h.Layers) <= node.Layer {
		h.Layers = append(h.Layers, &GraphLayer{Nodes: make(map[string]*Node)})
	}

	// Set as EntryPoint if it's the first node
	if h.EntryPoint == nil {
		h.EntryPoint = node
		h.Layers[node.Layer].Nodes[node.ID] = node
		return
	}

	h.insertNode(node)
}

func (h *HNSW) Search(target Vector, k int) []string {
	// Start from the entry point
	entryPoint := h.EntryPoint
	if entryPoint == nil {
		return nil
	}

	// Descend through the layers
	for layer := len(h.Layers) - 1; layer > 0; layer-- {
		// Refine the entry point at each layer
		candidates := h.searchLayer(entryPoint, target, 1, layer)
		if len(candidates) > 0 {
			entryPoint = h.Layers[layer].Nodes[candidates[0]]
		}
	}

	// Perform final search at the base layer (layer 0)
	candidates := h.searchLayer(entryPoint, target, h.EfSearch, 0)

	// Select top-k neighbors
	return h.selectNeighborsHeuristic(&Node{Value: target}, candidates, k)
}

func (h *HNSW) Delete(nodeID string) bool {
	h.lock.Lock()
	defer h.lock.Unlock()

	if len(h.Layers) == 0 {
		return false
	}

	var deleted bool
	for layer := len(h.Layers) - 1; layer >= 0; layer-- {
		layerNodes := h.Layers[layer].Nodes
		node, exists := layerNodes[nodeID]
		if !exists {
			continue
		}

		// Remove the node from the layer
		delete(layerNodes, nodeID)

		// Disconnect the node from its neighbors and restore neighborhood connectivity
		h.isolateNode(node, layer)
		deleted = true
	}

	// Update the entry point if the deleted node was the entry point
	if h.EntryPoint != nil && h.EntryPoint.ID == nodeID {
		h.updateEntryPoint()
	}

	return deleted
}

// isolateNode removes the node's connections and replenishes its neighbors.
func (h *HNSW) isolateNode(node *Node, layer int) {
	for neighborID := range node.Neighbors {
		neighbor := h.Layers[layer].Nodes[neighborID]

		// Remove bidirectional connection
		delete(neighbor.Neighbors, node.ID)
		h.replenishNode(neighbor, layer)
	}
}

// replenishNode restores connectivity for a node by adding new neighbors up to the maxConnections limit.
func (h *HNSW) replenishNode(node *Node, layer int) {
	if len(node.Neighbors) >= h.maxConnections(layer) {
		return
	}

	// Collect potential candidates from neighbors' neighbors
	candidates := make(map[string]*Node)
	for neighborID := range node.Neighbors {
		neighbor := h.Layers[layer].Nodes[neighborID]
		for candidateID := range neighbor.Neighbors {
			// Skip if the candidate is already a neighbor or if the candidate is the node itself
			if _, exists := node.Neighbors[candidateID]; exists {
				continue
			}
			if candidateID == node.ID {
				continue
			}

			// Add the candidate
			candidates[candidateID] = h.Layers[layer].Nodes[candidateID]
		}
	}

	// Select the best candidates
	selected := h.selectNeighborsHeuristic(node, maps.Keys(candidates), h.maxConnections(layer))

	// Add the selected neighbors
	for _, neighborID := range selected {
		neighbor := h.Layers[layer].Nodes[neighborID]
		h.addBidirectionalConnection(node, neighbor, h.DistFunc(node.Value, neighbor.Value))
	}
}

// updateEntryPoint updates the entry point for the HNSW graph.
func (h *HNSW) updateEntryPoint() {
	for layer := len(h.Layers) - 1; layer >= 0; layer-- {
		for _, node := range h.Layers[layer].Nodes {
			h.EntryPoint = node
			return
		}
	}
	h.EntryPoint = nil // No nodes left in the graph
}

func (h *HNSW) insertNode(node *Node) {
	// Add the node to the appropriate layer
	// Ensure the slice has enough capacity for the new node's layer
	for len(h.Layers) <= node.Layer {
		h.Layers = append(h.Layers, &GraphLayer{Nodes: make(map[string]*Node)})
	}

	// Add the node to its layer
	h.Layers[node.Layer].Nodes[node.ID] = node

	// Start with the entry point
	entryPoint := h.EntryPoint
	if entryPoint == nil {
		// If this is the first node, make it the entry point
		h.EntryPoint = node
		return
	}

	for layer := len(h.Layers) - 1; layer > node.Layer; layer-- {
		// Search in the current layer
		result := h.searchLayer(entryPoint, node.Value, 1, layer)
		if len(result) == 0 {
			continue
		}

		graphLayer := h.Layers[layer]
		entryPointID := result[0] // Closest node ID
		entryPoint = graphLayer.Nodes[entryPointID]
	}

	// 2. Insert into lower layers
	// Insert into lower layers
	for lc := int(math.Min(float64(node.Layer), float64(len(h.Layers)-1))); lc >= 0; lc-- {
		// Search for the nearest neighbors in the current layer
		candidates := h.searchLayer(entryPoint, node.Value, h.EfSearch, lc)

		// Use the heuristic neighbor selection
		neighbors := h.selectNeighborsHeuristic(node, candidates, h.maxConnections(lc))

		// Add bidirectional connections
		for _, neighborID := range neighbors {
			neighbor := h.Layers[lc].Nodes[neighborID]
			h.addBidirectionalConnection(node, neighbor, h.DistFunc(node.Value, neighbor.Value))
		}

		// Shrink connections for each neighbor if needed
		for _, neighborID := range neighbors {
			neighbor := h.Layers[lc].Nodes[neighborID]

			if len(neighbor.Neighbors) > h.maxConnections(lc) {
				// Convert map keys (neighbor IDs) to a slice of string
				keys := make([]string, 0, len(neighbor.Neighbors))
				for id := range neighbor.Neighbors {
					keys = append(keys, id)
				}

				// Select the best neighbors using the heuristic
				selected := h.selectNeighborsHeuristic(neighbor, keys, h.maxConnections(lc))

				// Rebuild the map for selected neighbors
				newNeighbors := make(map[string]float64)
				for _, id := range selected {
					newNeighbors[id] = neighbor.Neighbors[id] // Retain distances for selected neighbors
				}
				neighbor.Neighbors = newNeighbors
			}
		}

		// Update the entry point for the next layer
		if len(candidates) > 0 {
			entryPoint = h.Layers[lc].Nodes[candidates[0]]
		}
	}

	// Update the entry point if the new node is at a higher level
	if node.Layer > len(h.Layers)-1 {
		h.EntryPoint = node
	}
}

func (h *HNSW) searchLayer(entryPoint *Node, target Vector, ef int, layer int) []string {
	// Min-heap for candidates
	candidates := priorityqueue.NewWith(func(a, b interface{}) int {
		ca := a.(*SearchCandidate)
		cb := b.(*SearchCandidate)
		return utils.Float64Comparator(ca.Distance, cb.Distance) // Min-heap
	})

	// Max-heap for results
	results := priorityqueue.NewWith(func(a, b interface{}) int {
		ca := a.(*SearchCandidate)
		cb := b.(*SearchCandidate)
		return utils.Float64Comparator(cb.Distance, ca.Distance) // Max-heap
	})

	// Add the entry point to the candidates
	candidates.Enqueue(&SearchCandidate{
		NodeID:   entryPoint.ID,
		Distance: h.DistFunc(target, entryPoint.Value),
	})

	// Track visited nodes
	visited := make(map[string]bool)
	visited[entryPoint.ID] = true

	// Search logic
	for !candidates.Empty() {
		// Get the closest candidate
		currentRaw, _ := candidates.Dequeue()
		current := currentRaw.(*SearchCandidate)

		// Stop if the current distance exceeds the farthest result
		if results.Size() >= ef {
			topResultRaw, _ := results.Peek()
			topResult := topResultRaw.(*SearchCandidate)
			if current.Distance > topResult.Distance {
				break
			}
		}

		// Get the current node
		currentNodeLayer := h.Layers[layer]
		graphLayerNodes := currentNodeLayer.Nodes
		currentNode, exists := graphLayerNodes[current.NodeID]
		if !exists || currentNode == nil {
			continue
		}
		// Iterate over neighbors
		for neighborID := range currentNode.Neighbors {
			if visited[neighborID] {
				continue
			}
			visited[neighborID] = true

			// Calculate the distance
			neighborNodeLayer := h.Layers[layer]
			neighborNode := neighborNodeLayer.Nodes[neighborID]
			dist := h.DistFunc(target, neighborNode.Value)

			// Get the current farthest element in results (if available)
			farthestDist := math.Inf(1)
			if results.Size() > 0 {
				topResultRaw, _ := results.Peek()
				topResult := topResultRaw.(*SearchCandidate)
				farthestDist = topResult.Distance
			}

			// Add to candidates if conditions are met
			if dist < farthestDist || results.Size() < ef {
				candidates.Enqueue(&SearchCandidate{
					NodeID:   neighborID,
					Distance: dist,
				})

				// Maintain results heap
				results.Enqueue(&SearchCandidate{
					NodeID:   neighborID,
					Distance: dist,
				})

				// Prune results if size exceeds ef
				if results.Size() > ef {
					results.Dequeue() // Remove the farthest element
				}
			}
		}
	}

	// Extract results
	finalResults := make([]string, 0, results.Size())
	for !results.Empty() {
		resultRaw, _ := results.Dequeue()
		finalResults = append(finalResults, resultRaw.(*SearchCandidate).NodeID)
	}

	return finalResults
}

func (h *HNSW) selectNeighborsHeuristic(q *Node, candidates []string, M int) []string {

	// Calculate distances for all candidates
	candidateDistances := make([]SearchCandidate, 0, len(candidates))
	for _, candidateID := range candidates {
		candidateNode := h.Layers[q.Layer].Nodes[candidateID]
		dist := h.DistFunc(q.Value, candidateNode.Value)
		candidateDistances = append(candidateDistances, SearchCandidate{
			NodeID:   candidateID,
			Distance: dist,
		})
	}

	// Sort candidates by distance to `q`
	sort.Slice(candidateDistances, func(i, j int) bool {
		return candidateDistances[i].Distance < candidateDistances[j].Distance
	})

	// Use a heuristic to select neighbors
	selected := make([]string, 0, M)
	for _, candidate := range candidateDistances {
		if len(selected) >= M {
			break
		}

		// Check if the candidate maintains diversity
		isDiverse := true
		for _, selectedID := range selected {
			selectedNode := h.Layers[q.Layer].Nodes[selectedID]
			distToSelected := h.DistFunc(
				h.Layers[q.Layer].Nodes[candidate.NodeID].Value,
				selectedNode.Value,
			)

			// Heuristic: ensure diversity by checking mutual distances
			if distToSelected < candidate.Distance {
				isDiverse = false
				break
			}
		}

		if isDiverse {
			selected = append(selected, candidate.NodeID)
		}
	}

	return selected
}

func (h *HNSW) maxConnections(layer int) int {
	if layer == 0 {
		return h.MaxNeighbors0 // Maximum connections for base layer
	}
	return h.MaxNeighbors // Maximum connections for higher layers
}

func (h *HNSW) addBidirectionalConnection(node1, node2 *Node, distance float64) {
	// Add node2 as a neighbor of node1
	if node1.Neighbors == nil {
		node1.Neighbors = make(map[string]float64)
	}
	node1.Neighbors[node2.ID] = distance

	// Add node1 as a neighbor of node2
	if node2.Neighbors == nil {
		node2.Neighbors = make(map[string]float64)
	}
	node2.Neighbors[node1.ID] = distance
}
