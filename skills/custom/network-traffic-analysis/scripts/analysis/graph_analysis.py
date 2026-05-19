"""
Graph Analysis Module

Implementations of:
- Efficient graph construction with hash-based edge lookup
- Greedy modularity local-moving community detection
- Brandes' algorithm for betweenness centrality
- PageRank with power iteration
- BFS-based attack path discovery

References:
- Blondel, V. D., et al. (2008). "Fast unfolding of communities in large networks"
- Brandes, U. (2001). "A faster algorithm for betweenness centrality"
- Page, L., et al. (1999). "The PageRank citation ranking"
"""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any


@dataclass
class GraphAnalysisResult:
    """Result of graph analysis."""
    node_count: int
    edge_count: int
    communities: list[dict[str, Any]]
    central_nodes: list[dict[str, Any]]
    anomalous_nodes: list[dict[str, Any]]
    graph_summary: str


class LouvainCommunityDetector:
    """
    Greedy modularity local-moving community detection.

    The Louvain method is a greedy optimization method for modularity maximization:

    Modularity Q = (1/2m) * Σ_{ij} [A_{ij} - k_i * k_j / 2m] * δ(c_i, c_j)

    where:
    - A_{ij} is the adjacency matrix
    - k_i is the degree of node i
    - m is the total number of edges
    - c_i is the community of node i
    - δ is the Kronecker delta function

    This implementation performs the local-moving phase only. It does not
    rebuild supernodes, so results should not be presented as full Louvain.

    Time complexity: O(n * log(n)) on sparse networks
    Space complexity: O(n + m)

    Reference: Blondel et al., JSTAT 2008
    """

    def __init__(self, resolution: float = 1.0):
        """
        Initialize Louvain detector.

        Args:
            resolution: Resolution parameter (higher = more communities)
        """
        self.resolution = resolution

    def detect_communities(self, nodes: list[str],
                          edges: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Detect communities using greedy modularity local moves.

        Args:
            nodes: List of node identifiers
            edges: List of edge dictionaries with 'src', 'dst', 'weight'

        Returns:
            List of communities with member nodes and modularity
        """
        if not nodes or not edges:
            return []

        # Build adjacency structure
        adjacency: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
        for edge in edges:
            src = edge.get("src", "")
            dst = edge.get("dst", "")
            weight = edge.get("weight", 1.0)

            if src and dst:
                adjacency[src][dst] += weight
                adjacency[dst][src] += weight  # Undirected

        # Initialize: each node in its own community
        community = {node: node for node in nodes}

        # Compute node degrees
        degree: dict[str, float] = defaultdict(float)
        for node in nodes:
            degree[node] = sum(adjacency[node].values())

        # Total edge weight
        m = sum(degree.values()) / 2

        if m == 0:
            return [{"community_id": i, "members": [node], "modularity_contribution": 0.0}
                    for i, node in enumerate(nodes)]

        # Phase 1: Local moving
        improved = True
        while improved:
            improved = False

            for node in nodes:
                current_community = community[node]

                # Compute gain for moving to each neighbor's community
                best_community = current_community
                best_gain = 0.0

                # Get neighboring communities
                neighbor_communities = set()
                for neighbor in adjacency[node]:
                    neighbor_communities.add(community[neighbor])

                for target_community in neighbor_communities:
                    if target_community == current_community:
                        continue

                    # Compute modularity gain
                    gain = self._compute_modularity_gain(
                        node, target_community, community, adjacency, degree, m
                    )

                    if gain > best_gain:
                        best_gain = gain
                        best_community = target_community

                # Move node to best community
                if best_community != current_community:
                    community[node] = best_community
                    improved = True

        # Build community structure
        communities: dict[str, list[str]] = defaultdict(list)
        for node, comm_id in community.items():
            communities[comm_id].append(node)

        # Compute modularity
        modularity = self._compute_modularity(community, adjacency, degree, m)

        # Convert to result format
        result = []
        for i, (comm_id, members) in enumerate(communities.items()):
            result.append({
                "community_id": i,
                "members": members,
                "member_count": len(members),
                "modularity": modularity,
                "density": self._compute_community_density(members, adjacency)
            })

        # Sort by size (largest first)
        result.sort(key=lambda x: x["member_count"], reverse=True)

        return result

    def _compute_modularity_gain(self, node: str, target_community: str,
                                community: dict[str, str],
                                adjacency: dict[str, dict[str, float]],
                                degree: dict[str, float],
                                m: float) -> float:
        """
        Compute modularity gain for moving node to target community.

        ΔQ = [Σ_in + 2 * k_{i,in}] / 2m - [(Σ_tot + k_i) / 2m]^2
           - [Σ_in / 2m - (Σ_tot / 2m)^2 - (k_i / 2m)^2]

        where:
        - Σ_in is the sum of weights of links inside target community
        - Σ_tot is the sum of degrees of nodes in target community
        - k_{i,in} is the sum of links from node i to nodes in target community
        - k_i is the degree of node i
        """
        # Sum of weights from node to target community
        k_i_in = sum(
            adjacency[node].get(neighbor, 0.0)
            for neighbor, comm in community.items()
            if comm == target_community
        )

        # Sum of degrees in target community
        sigma_tot = sum(
            degree[neighbor]
            for neighbor, comm in community.items()
            if comm == target_community
        )

        # Node degree
        k_i = degree[node]

        # Modularity gain formula
        delta_q = (k_i_in / m) - self.resolution * (sigma_tot * k_i) / (2 * m * m)

        return delta_q

    def _compute_modularity(self, community: dict[str, str],
                           adjacency: dict[str, dict[str, float]],
                           degree: dict[str, float],
                           m: float) -> float:
        """Compute overall modularity of the partition."""
        q = 0.0

        for node_i in community:
            for node_j in community:
                if community[node_i] != community[node_j]:
                    continue

                # A_{ij}
                a_ij = adjacency[node_i].get(node_j, 0.0)

                # Expected edge weight
                expected = (degree[node_i] * degree[node_j]) / (2 * m)

                q += a_ij - expected

        return q / (2 * m)

    def _compute_community_density(self, members: list[str],
                                  adjacency: dict[str, dict[str, float]]) -> float:
        """Compute density of a community."""
        n = len(members)
        if n <= 1:
            return 0.0

        # Count internal edges
        member_set = set(members)
        internal_edges = 0

        for node in members:
            for neighbor, weight in adjacency[node].items():
                if neighbor in member_set:
                    internal_edges += 1

        internal_edges /= 2  # Each edge counted twice

        # Maximum possible edges
        max_edges = n * (n - 1) / 2

        return internal_edges / max_edges if max_edges > 0 else 0.0


class BrandesBetweenness:
    """
    Brandes' algorithm for betweenness centrality.

    Betweenness centrality of node v:

    C_B(v) = Σ_{s≠v≠t} [σ_{st}(v) / σ_{st}]

    where:
    - σ_{st} is the number of shortest paths from s to t
    - σ_{st}(v) is the number of those paths passing through v

    Brandes' algorithm computes this in O(nm) time using
    dependency accumulation from sources.

    Time complexity: O(nm) for unweighted, O(nm + n^2 log n) for weighted
    Space complexity: O(n + m)

    Reference: Brandes, JMS 2001
    """

    @staticmethod
    def compute(nodes: list[str], edges: list[dict[str, Any]]) -> dict[str, float]:
        """
        Compute betweenness centrality using Brandes' algorithm.

        Args:
            nodes: List of node identifiers
            edges: List of edge dictionaries

        Returns:
            Dictionary mapping nodes to betweenness scores
        """
        # Build adjacency list
        adjacency: dict[str, list[str]] = defaultdict(list)
        for edge in edges:
            src = edge.get("src", "")
            dst = edge.get("dst", "")
            if src and dst:
                adjacency[src].append(dst)
                adjacency[dst].append(src)  # Undirected

        betweenness = {node: 0.0 for node in nodes}

        for source in nodes:
            # Single-source shortest paths
            stack = []
            pred = {node: [] for node in nodes}
            sigma = {node: 0 for node in nodes}
            sigma[source] = 1
            dist = {node: -1 for node in nodes}
            dist[source] = 0

            queue = [source]
            while queue:
                v = queue.pop(0)
                stack.append(v)

                for w in adjacency[v]:
                    # First visit
                    if dist[w] < 0:
                        queue.append(w)
                        dist[w] = dist[v] + 1

                    # Shortest path to w via v
                    if dist[w] == dist[v] + 1:
                        sigma[w] += sigma[v]
                        pred[w].append(v)

            # Back-propagation of dependencies
            delta = {node: 0.0 for node in nodes}
            while stack:
                w = stack.pop()
                for v in pred[w]:
                    delta[v] += (sigma[v] / sigma[w]) * (1 + delta[w])

                if w != source:
                    betweenness[w] += delta[w]

        # Normalize for undirected graphs (divide by 2)
        for node in betweenness:
            betweenness[node] /= 2.0

        return betweenness


class PageRank:
    """
    PageRank algorithm using power iteration method.

    PageRank PR(v) = (1-d)/N + d * Σ_{u∈B(v)} [PR(u) / L(u)]

    where:
    - d is the damping factor (typically 0.85)
    - N is the total number of nodes
    - B(v) is the set of nodes linking to v
    - L(u) is the number of outgoing links from u

    Converges when max change < tolerance.

    Time complexity: O(k * (n + m)) where k is iterations
    Space complexity: O(n + m)

    Reference: Page et al., 1999
    """

    @staticmethod
    def compute(nodes: list[str], edges: list[dict[str, Any]],
               damping: float = 0.85, max_iter: int = 100,
               tolerance: float = 1e-6) -> dict[str, float]:
        """
        Compute PageRank using power iteration.

        Args:
            nodes: List of node identifiers
            edges: List of edge dictionaries
            damping: Damping factor
            max_iter: Maximum iterations
            tolerance: Convergence tolerance

        Returns:
            Dictionary mapping nodes to PageRank scores
        """
        if not nodes:
            return {}

        N = len(nodes)

        # Build adjacency list
        out_degree: dict[str, int] = defaultdict(int)
        in_links: dict[str, list[str]] = defaultdict(list)

        for edge in edges:
            src = edge.get("src", "")
            dst = edge.get("dst", "")
            if src and dst:
                out_degree[src] += 1
                in_links[dst].append(src)

        # Initialize PageRank
        pagerank = {node: 1.0 / N for node in nodes}

        # Power iteration
        for iteration in range(max_iter):
            new_pagerank = {}
            dangling_mass = sum(pagerank[node] for node in nodes if out_degree[node] == 0)

            for node in nodes:
                # Sum of PR(u) / L(u) for all u linking to node
                rank_sum = 0.0
                for u in in_links.get(node, []):
                    if out_degree[u] > 0:
                        rank_sum += pagerank[u] / out_degree[u]

                new_pagerank[node] = (1 - damping) / N + damping * (rank_sum + dangling_mass / N)

            # Check convergence
            max_change = max(abs(new_pagerank[node] - pagerank[node]) for node in nodes)

            pagerank = new_pagerank

            if max_change < tolerance:
                break

        return pagerank


class TrafficGraphAnalyzer:
    """
    Analyzes network communication graphs with real algorithms.
    """

    def build_graph(self, flows: list[dict[str, Any]]) -> dict[str, Any]:
        """
        Build communication graph from flow data.

        Uses hash-based edge lookup for O(1) edge updates.

        Args:
            flows: List of flow records

        Returns:
            Graph dictionary
        """
        nodes = set()
        edges_dict: dict[tuple[str, str], dict[str, Any]] = {}

        for flow in flows:
            src_ip = flow.get("src_ip", "")
            dst_ip = flow.get("dst_ip", "")

            if not src_ip or not dst_ip:
                continue

            nodes.add(src_ip)
            nodes.add(dst_ip)

            edge_key = (src_ip, dst_ip)

            if edge_key in edges_dict:
                # Update existing edge (O(1) hash lookup)
                edge = edges_dict[edge_key]
                edge["total_bytes"] += flow.get("bytes", 0)
                edge["total_packets"] += flow.get("packets", 0)
                edge["flow_count"] += 1
                edge["ports"].add(flow.get("dst_port", 0))
                edge["protocols"].add(flow.get("protocol", ""))
            else:
                # Create new edge
                edges_dict[edge_key] = {
                    "src": src_ip,
                    "dst": dst_ip,
                    "total_bytes": flow.get("bytes", 0),
                    "total_packets": flow.get("packets", 0),
                    "flow_count": 1,
                    "ports": set([flow.get("dst_port", 0)]),
                    "protocols": set([flow.get("protocol", "")])
                }

        # Convert sets to lists for JSON
        edges = []
        for edge in edges_dict.values():
            edge["ports"] = list(edge["ports"])
            edge["protocols"] = list(edge["protocols"])
            edges.append(edge)

        return {
            "nodes": sorted(list(nodes)),
            "edges": edges,
            "node_count": len(nodes),
            "edge_count": len(edges)
        }

    def detect_communities(self, graph: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Detect communities using Louvain algorithm.

        Args:
            graph: Graph dictionary from build_graph

        Returns:
            List of communities
        """
        nodes = graph.get("nodes", [])
        edges = graph.get("edges", [])

        # Prepare edges with weights
        weighted_edges = []
        for edge in edges:
            weighted_edges.append({
                "src": edge["src"],
                "dst": edge["dst"],
                "weight": edge.get("total_bytes", 1.0)
            })

        detector = LouvainCommunityDetector()
        return detector.detect_communities(nodes, weighted_edges)

    def calculate_centrality(self, graph: dict[str, Any]) -> dict[str, Any]:
        """
        Calculate centrality metrics using real algorithms.

        Args:
            graph: Graph dictionary

        Returns:
            Dictionary with centrality metrics
        """
        nodes = graph.get("nodes", [])
        edges = graph.get("edges", [])

        # Degree centrality
        degree: dict[str, int] = defaultdict(int)
        for edge in edges:
            degree[edge["src"]] += 1
            degree[edge["dst"]] += 1

        degree_centrality = {
            node: degree.get(node, 0) / (len(nodes) - 1) if len(nodes) > 1 else 0
            for node in nodes
        }

        # Betweenness centrality (Brandes' algorithm)
        betweenness = BrandesBetweenness.compute(nodes, edges)

        # Normalize betweenness
        max_betweenness = max(betweenness.values()) if betweenness else 1.0
        if max_betweenness > 0:
            betweenness_normalized = {
                node: score / max_betweenness
                for node, score in betweenness.items()
            }
        else:
            betweenness_normalized = betweenness

        # PageRank
        pagerank = PageRank.compute(nodes, edges)

        # Build central nodes list
        central_nodes = []
        for node in nodes:
            central_nodes.append({
                "node": node,
                "degree_centrality": degree_centrality.get(node, 0),
                "betweenness_centrality": betweenness_normalized.get(node, 0),
                "pagerank": pagerank.get(node, 0)
            })

        # Sort by PageRank
        central_nodes.sort(key=lambda x: x["pagerank"], reverse=True)

        return {
            "degree_centrality": degree_centrality,
            "betweenness_centrality": betweenness,
            "pagerank": pagerank,
            "top_central_nodes": central_nodes[:10]
        }

    def find_anomalous_nodes(self, graph: dict[str, Any],
                            centrality: dict[str, Any]) -> list[dict[str, Any]]:
        """Find anomalous nodes based on graph structure."""
        nodes = graph.get("nodes", [])
        edges = graph.get("edges", [])
        degree_centrality = centrality.get("degree_centrality", {})

        if not degree_centrality:
            return []

        def _quantile(values: list[float], q: float) -> float:
            if not values:
                return 0.0
            ordered = sorted(values)
            index = min(len(ordered) - 1, max(0, int((len(ordered) - 1) * q)))
            return ordered[index]

        # Calculate statistics
        degrees = list(degree_centrality.values())
        avg_degree = sum(degrees) / len(degrees) if degrees else 0

        # Calculate traffic per node
        node_traffic: dict[str, int] = defaultdict(int)
        for edge in edges:
            node_traffic[edge["src"]] += edge.get("total_bytes", 0)
            node_traffic[edge["dst"]] += edge.get("total_bytes", 0)

        anomalous_nodes = []
        high_degree_threshold = max(avg_degree * 3, _quantile(degrees, 0.95))
        traffic_values = [float(value) for value in node_traffic.values()]
        high_traffic_threshold = _quantile(traffic_values, 0.95)

        for node in nodes:
            degree = degree_centrality.get(node, 0)
            anomaly_reasons = []

            # High degree (hub node)
            if avg_degree > 0 and degree > high_degree_threshold and degree > avg_degree:
                anomaly_reasons.append("high_connectivity_hub")

            # Very low degree (isolated)
            if degree < 0.01 and len(nodes) > 10:
                anomaly_reasons.append("isolated_node")

            # High traffic
            traffic = node_traffic.get(node, 0)
            if high_traffic_threshold > 0 and traffic > high_traffic_threshold:
                anomaly_reasons.append("high_traffic_volume")

            if anomaly_reasons:
                anomalous_nodes.append({
                    "node": node,
                    "degree_centrality": degree,
                    "total_bytes": traffic,
                    "anomaly_reasons": anomaly_reasons
                })

        anomalous_nodes.sort(key=lambda x: x["degree_centrality"], reverse=True)
        return anomalous_nodes

    def discover_attack_paths(self, graph: dict[str, Any],
                             source_ip: str, target_ip: str,
                             max_paths: int = 10) -> list[dict[str, Any]]:
        """
        Discover shortest attack paths using breadth-first search.

        Args:
            graph: Graph dictionary
            source_ip: Source IP
            target_ip: Target IP
            max_paths: Maximum paths to find

        Returns:
            List of attack paths
        """
        nodes = graph.get("nodes", [])
        edges = graph.get("edges", [])

        if source_ip not in nodes or target_ip not in nodes:
            return []

        # Build adjacency list
        adjacency: dict[str, list[str]] = defaultdict(list)
        for edge in edges:
            adjacency[edge["src"]].append(edge["dst"])

        # Breadth-first search returns shortest hop-count paths first.
        paths = []
        queue = deque([(source_ip, [source_ip])])

        while queue and len(paths) < max_paths:
            current, path = queue.popleft()

            if current == target_ip:
                paths.append({
                    "path": path,
                    "hop_count": len(path) - 1,
                    "path_string": " -> ".join(path)
                })
                continue

            for neighbor in adjacency.get(current, []):
                if neighbor not in path:  # Avoid cycles
                    queue.append((neighbor, path + [neighbor]))

        return paths

    def analyze_graph(self, flows: list[dict[str, Any]]) -> GraphAnalysisResult:
        """Comprehensive graph analysis."""
        graph = self.build_graph(flows)
        communities = self.detect_communities(graph)
        centrality = self.calculate_centrality(graph)
        anomalous_nodes = self.find_anomalous_nodes(graph, centrality)

        node_count = graph.get("node_count", 0)
        edge_count = graph.get("edge_count", 0)

        summary = (
            f"Graph with {node_count} nodes and {edge_count} edges. "
            f"Communities detected: {len(communities)}. "
            f"Anomalous nodes: {len(anomalous_nodes)}."
        )

        return GraphAnalysisResult(
            node_count=node_count,
            edge_count=edge_count,
            communities=communities,
            central_nodes=centrality.get("top_central_nodes", []),
            anomalous_nodes=anomalous_nodes,
            graph_summary=summary
        )
