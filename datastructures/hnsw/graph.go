package hnsw

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/absolutelightning/gods/queues/priorityqueue"
	"github.com/absolutelightning/gods/utils"
	"github.com/google/uuid"
	"golang.org/x/exp/maps"
)

type DistanceFunc func(a, b Vector) float64

// HNSW represents the entire hierarchical graph.
type HNSW struct {
	Layers        []*GraphLayer
	MaxNeighbors  int          // M for higher layers
	MaxNeighbors0 int          // Mmax0 for base layer
	LayerFactor   float64      // Probability factor, but we won't use it with the current randomLevel
	EfSearch      int          // number of candidates during search
	DistFunc      DistanceFunc // distance function
	lock          sync.Mutex   // lock for thread-safe operations
	EntryPoint    *Node        // top entry point into the graph
}

type SearchCandidate struct {
	NodeID   string
	Distance float64
}

func NewHNSW(maxNeighbors int, layerFactor float64, efSearch int, distanceFunc DistanceFunc) *HNSW {
	return &HNSW{
		Layers:        make([]*GraphLayer, 0),
		MaxNeighbors:  maxNeighbors,
		MaxNeighbors0: maxNeighbors, // ensure base layer is also set
		LayerFactor:   layerFactor,
		EfSearch:      efSearch,
		DistFunc:      distanceFunc,
	}
}

// generateID generates a unique ID for a new node.
func (h *HNSW) generateID() string {
	return uuid.New().String()
}

// defaultRand just returns a random generator seeded by time.
func defaultRand() *rand.Rand {
	return rand.New(rand.NewSource(time.Now().UnixNano()))
}

// randomLevel simply returns len(h.Layers).
// That means every new node is assigned to the "next new layer."
// We'll fix code so the node also goes into layers 0..level.
func (h *HNSW) randomLevel() int {
	level := 0
	// Cap at 32 to avoid unbounded levels (adjust as you wish)
	for defaultRand().Float64() < h.LayerFactor && level < 32 {
		level++
	}
	return level
}

// Insert adds a new element `vector` into the HNSW graph.
func (h *HNSW) Insert(vector Vector) {
	h.lock.Lock()
	defer h.lock.Unlock()

	level := h.randomLevel()

	// Create the new node
	node := &Node{
		ID:        h.generateID(),
		Value:     vector,
		Neighbors: make(map[string]float64),
		Layer:     level, // "highest" layer for the node
	}

	// Ensure we have enough layers up to `level`
	for len(h.Layers) <= level {
		h.Layers = append(h.Layers, &GraphLayer{Nodes: make(map[string]*Node)})
	}

	// **Critical**: In HNSW, if a node's top layer is L,
	// it still exists in layers 0..L. We store the *same Node pointer*
	// in all those layer maps. This ensures that if future code references
	// the node at layer 0, it actually exists in h.Layers[0].Nodes.
	for l := 0; l <= level; l++ {
		h.Layers[l].Nodes[node.ID] = node
	}

	// Now integrate the node into the structure (neighbor linking, entry point updates)
	h.insertNode(node)
}

// insertNode does top-down refinement of the entry point and connects neighbors.
func (h *HNSW) insertNode(node *Node) {
	// If we have no entry point yet, this is the first node
	if h.EntryPoint == nil {
		h.EntryPoint = node
		return
	}

	entryPoint := h.EntryPoint

	// Step 1: Refine the entry point from top layers down to node.Layer+1 (ef=1)
	for layer := entryPoint.Layer; layer > node.Layer; layer-- {
		candidates := h.searchLayer(entryPoint, node.Value, 1, layer)
		if len(candidates) > 0 {
			newEP := h.Layers[layer].Nodes[candidates[0]]
			if newEP != nil {
				entryPoint = newEP
			}
		}
	}

	// Step 2: Insert the node in layers from min(node.Layer, entryPoint.Layer) down to 0
	startLayer := min(node.Layer, entryPoint.Layer)
	for lc := startLayer; lc >= 0; lc-- {
		// We do a search with ef = h.EfSearch
		candidates := h.searchLayer(entryPoint, node.Value, h.EfSearch, lc)

		// Connect node <-> each candidate
		for _, candidateID := range candidates {
			neighbor := h.Layers[lc].Nodes[candidateID]
			if neighbor == nil {
				continue
			}
			dist := h.DistFunc(node.Value, neighbor.Value)
			h.addBidirectionalConnection(node, neighbor, dist)
		}

		// Update entryPoint for next layer
		if len(candidates) > 0 {
			nextEP := h.Layers[lc].Nodes[candidates[0]]
			if nextEP != nil {
				entryPoint = nextEP
			}
		}
	}

	// Step 3: If the new node's layer is higher than the old entry point, it becomes the new entry point
	if node.Layer > h.EntryPoint.Layer {
		h.EntryPoint = node
	}
}

// Search returns up to k nearest neighbors for 'target'.
func (h *HNSW) Search(target Vector, k int) []string {
	entryPoint := h.EntryPoint
	if entryPoint == nil {
		return nil
	}

	// Refine entry point from top layer down to 1 using ef=1
	for layer := entryPoint.Layer; layer > 0; layer-- {
		candidates := h.searchLayer(entryPoint, target, 1, layer)
		if len(candidates) > 0 {
			entryPoint = h.Layers[layer].Nodes[candidates[0]]
		}
	}

	// Final search at layer 0 with ef = h.EfSearch
	candidates := h.searchLayer(entryPoint, target, h.EfSearch, 0)

	// If we have fewer than k results, return them all
	if len(candidates) <= k {
		return candidates
	}
	// Otherwise, pick the top k by distance
	return h.selectNeighborsHeuristic(&Node{Value: target, Layer: 0}, candidates, k)
}

// searchLayer is a best-first or greedy BFS in a single layer.
func (h *HNSW) searchLayer(entryPoint *Node, target Vector, ef int, layer int) []string {
	// Min-heap for candidates
	candidates := priorityqueue.NewWith(func(a, b interface{}) int {
		ca := a.(*SearchCandidate)
		cb := b.(*SearchCandidate)
		return utils.Float64Comparator(ca.Distance, cb.Distance)
	})

	// Max-heap for results
	results := priorityqueue.NewWith(func(a, b interface{}) int {
		ca := a.(*SearchCandidate)
		cb := b.(*SearchCandidate)
		// Reverse comparator => top is the farthest
		return utils.Float64Comparator(cb.Distance, ca.Distance)
	})

	distEP := h.DistFunc(target, entryPoint.Value)
	results.Enqueue(&SearchCandidate{NodeID: entryPoint.ID, Distance: distEP})
	candidates.Enqueue(&SearchCandidate{NodeID: entryPoint.ID, Distance: distEP})

	visited := make(map[string]bool)
	visited[entryPoint.ID] = true

	for !candidates.Empty() {
		currentRaw, _ := candidates.Dequeue()
		current := currentRaw.(*SearchCandidate)

		if results.Size() >= ef {
			farthestRaw, _ := results.Peek()
			farthest := farthestRaw.(*SearchCandidate)
			// If the current candidate is farther than the farthest in results, break
			if current.Distance > farthest.Distance {
				break
			}
		}

		currentNode := h.Layers[layer].Nodes[current.NodeID]
		if currentNode == nil {
			fmt.Printf("Node %s not found in layer %d\n", current.NodeID, layer)
			continue
		}

		// Check neighbors
		for neighborID := range currentNode.Neighbors {
			if visited[neighborID] {
				continue
			}
			visited[neighborID] = true

			neighborNode := h.Layers[layer].Nodes[neighborID]
			if neighborNode == nil {
				continue
			}
			dist := h.DistFunc(target, neighborNode.Value)

			candidates.Enqueue(&SearchCandidate{NodeID: neighborID, Distance: dist})
			results.Enqueue(&SearchCandidate{NodeID: neighborID, Distance: dist})

			// If we exceed ef in results, pop the farthest
			if results.Size() > ef {
				results.Dequeue()
			}
		}
	}

	// Extract results from the max-heap
	finalResults := make([]string, 0, results.Size())
	for !results.Empty() {
		sc, _ := results.Dequeue()
		finalResults = append(finalResults, sc.(*SearchCandidate).NodeID)
	}
	return finalResults
}

// selectNeighborsHeuristic picks the top M by distance.
func (h *HNSW) selectNeighborsHeuristic(q *Node, candidates []string, M int) []string {
	if len(candidates) == 0 {
		return nil
	}

	// Compute distances
	candidateDistances := make([]SearchCandidate, 0, len(candidates))
	for _, candidateID := range candidates {
		candidateNode := h.Layers[q.Layer].Nodes[candidateID]
		if candidateNode == nil {
			continue
		}
		dist := h.DistFunc(q.Value, candidateNode.Value)
		candidateDistances = append(candidateDistances, SearchCandidate{NodeID: candidateID, Distance: dist})
	}

	// Sort ascending by distance
	sort.Slice(candidateDistances, func(i, j int) bool {
		return candidateDistances[i].Distance < candidateDistances[j].Distance
	})

	// Take top M
	selected := make([]string, 0, M)
	for i := 0; i < M && i < len(candidateDistances); i++ {
		selected = append(selected, candidateDistances[i].NodeID)
	}
	return selected
}

// MaxConnections returns how many neighbors are allowed in the given layer.
func (h *HNSW) MaxConnections(layer int) int {
	if layer == 0 {
		return h.MaxNeighbors0
	}
	return h.MaxNeighbors
}

// addBidirectionalConnection links node1 and node2 both ways.
func (h *HNSW) addBidirectionalConnection(node1, node2 *Node, distance float64) {
	node1.Neighbors[node2.ID] = distance
	node2.Neighbors[node1.ID] = distance
}

// DebugPrintGraph logs each layer and its nodes.
func (h *HNSW) DebugPrintGraph() {
	for i, layer := range h.Layers {
		fmt.Printf("Layer %d: %d nodes\n", i, len(layer.Nodes))
		for id, node := range layer.Nodes {
			neighbors := maps.Keys(node.Neighbors)
			fmt.Printf("  Node %s (layer=%d) => neighbors: %v\n",
				id, node.Layer, neighbors)
		}
	}
}

// isolateNode, replenishNode, delete, etc. can remain if you need them for other operations:

func (h *HNSW) isolateNode(node *Node, layer int) {
	for neighborID := range node.Neighbors {
		neighbor := h.Layers[layer].Nodes[neighborID]
		if neighbor == nil {
			continue
		}
		delete(neighbor.Neighbors, node.ID)
		h.replenishNode(neighbor, layer)
	}
}

func (h *HNSW) replenishNode(node *Node, layer int) {
	if len(node.Neighbors) >= h.MaxConnections(layer) {
		return
	}
	candidates := make(map[string]*Node)
	for neighborID := range node.Neighbors {
		neighbor := h.Layers[layer].Nodes[neighborID]
		if neighbor == nil {
			continue
		}
		for candidateID := range neighbor.Neighbors {
			if _, exists := node.Neighbors[candidateID]; exists || candidateID == node.ID {
				continue
			}
			candidates[candidateID] = h.Layers[layer].Nodes[candidateID]
		}
	}
	selected := h.selectNeighborsNormal(node, maps.Keys(candidates), h.MaxConnections(layer))
	for _, neighborID := range selected {
		neighbor := h.Layers[layer].Nodes[neighborID]
		if neighbor != nil {
			dist := h.DistFunc(node.Value, neighbor.Value)
			h.addBidirectionalConnection(node, neighbor, dist)
		}
	}
}

// selectNeighborsNormal is used in replenishNode to pick top M.
func (h *HNSW) selectNeighborsNormal(q *Node, candidateIDs []string, M int) []string {
	candidateDistances := make([]SearchCandidate, 0, len(candidateIDs))
	for _, cid := range candidateIDs {
		cn := h.Layers[q.Layer].Nodes[cid]
		if cn == nil {
			continue
		}
		dist := h.DistFunc(q.Value, cn.Value)
		candidateDistances = append(candidateDistances, SearchCandidate{
			NodeID:   cid,
			Distance: dist,
		})
	}
	sort.Slice(candidateDistances, func(i, j int) bool {
		return candidateDistances[i].Distance < candidateDistances[j].Distance
	})

	selected := make([]string, 0, M)
	for i := 0; i < M && i < len(candidateDistances); i++ {
		selected = append(selected, candidateDistances[i].NodeID)
	}
	return selected
}

// updateEntryPoint re-selects the top-layer node, used after deletes.
func (h *HNSW) updateEntryPoint() {
	for layer := len(h.Layers) - 1; layer >= 0; layer-- {
		for _, node := range h.Layers[layer].Nodes {
			h.EntryPoint = node
			return
		}
	}
	h.EntryPoint = nil
}

// small helper
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
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
