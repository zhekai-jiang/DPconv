#ifndef UNION_DP_RUNNER_HPP
#define UNION_DP_RUNNER_HPP

#include "DPccp.cpp"
#include "DPsub.cpp"
#include "QueryGraph.hpp"
#include "Util.hpp"
#include <algorithm>
#include <chrono>
#include <fstream>
#include <iostream>
#include <numeric>
#include <queue>
#include <utility>
#include <vector>

// UnionDP Algorithm Implementation
// "Uses DPsub or DPccp to implement UnionDP ... Use the cost model C_out"

// UnionDP Strategy:
// 1. Partition the query graph into multiple (possibly overlapping) clusters
// (subgraphs).
// 2. Explore the search space for each cluster using a DP algorithm
// (DPsub/DPccp).
// 3. Coordinate via a shared DP table.

template <typename U> class UnionDPSolver {
public:
  UnionDPSolver(unsigned n, const QueryGraph &qg, const std::unordered_map<uint64_t, U> &sizes)
      : n_(n), qg_(qg), sizes_(sizes) {}

  std::pair<U, std::shared_ptr<JoinNode>> solve(unsigned k) {
    // Initial scopes: each node i maps to bitmask (1<<i)
    std::vector<uint64_t> scopes(n_);
    for (unsigned i = 0; i < n_; ++i)
      scopes[i] = (1ULL << i);

    return solveRecursive(n_, qg_, scopes, k);
  }

private:
  unsigned n_;
  const QueryGraph &qg_;
  const std::unordered_map<uint64_t, U> &sizes_;

  // Recursive solver
  std::pair<U, std::shared_ptr<JoinNode>>
  solveRecursive(unsigned current_n, const QueryGraph &current_qg,
                 const std::vector<uint64_t> &scopes, unsigned k) {

    // Base Case: If graph is small enough, solve exactly
    if (current_n <= k) {
      return runExactDP(current_n, scopes);
    }

    // Partition Phase
    auto clusters = generate_clusters_for_graph(
        current_n, current_qg, current_qg.get_joins(), scopes, k);

    // If partitioning failed to reduce problem size (e.g. single cluster of
    // size n), we must fall back to exact DP to avoid infinite recursion. This
    // happens if the connectivity is high and k is small. However, Algorithm 4
    // implies we should just solve it if we can't partition further? Or maybe
    // generate_clusters guarantees splitting? If the whole graph is a single
    // component > k and we can't split it (e.g. clique?), the max-weight-edge
    // removal *will* eventually split it (unless single node). So distinct
    // clusters are guaranteed unless current_n=1 (handled by base case).
    if (clusters.size() == 1 && clusters[0] == ((1ULL << current_n) - 1)) {
      // Fallback
      return runExactDP(current_n, scopes);
    }

    // Solve Sub-problems (Composite Nodes)
    std::vector<std::shared_ptr<JoinNode>> sub_trees;
    std::vector<U> sub_costs;
    std::vector<uint64_t> result_cardinalities;
    std::vector<uint64_t> composite_cluster_masks; // Masks in current_n context

    for (uint64_t cluster_mask : clusters) {
      // cluster_mask is a bitmask of nodes in current_qg (0..current_n-1)
      composite_cluster_masks.push_back(cluster_mask);

      // Solve exactly for this cluster
      // We need to extract the "scope" of this cluster in the ORIGINAL graph
      // to look up sizes.
      // We can create a "virtual" exact DP call for restricted subset.
      // Actually, we can just run DPsub but restricted to this mask?
      // Or better: construct relevant `sub_sizes` map?
      // DPsub takes a full `sizes` vector. We can pass the GLOBAL sizes,
      // but getting the tree out requires care.
      // Let's implement `runExactDP_restricted`.

      auto [cost, tree] = runExactDP_restricted(cluster_mask, scopes);
      sub_trees.push_back(tree);
      sub_costs.push_back(cost);
      if (tree)
        result_cardinalities.push_back(tree->size); // size of root
      else {
        result_cardinalities.push_back(std::numeric_limits<U>::max());
        // If any cluster cannot be solved, the whole plan is invalid
        return {std::numeric_limits<U>::max(), nullptr};
      }
    }

    // Construct Reduced Graph G'
    unsigned m = clusters.size();

    // Edges of G'
    std::vector<std::pair<unsigned, unsigned>> new_edges;
    const auto &old_edges = current_qg.get_joins();

    // Map old nodes to new cluster indices
    std::vector<int> node_to_cluster(current_n, -1);
    for (unsigned c = 0; c < m; ++c) {
      for (unsigned i = 0; i < current_n; ++i) {
        if ((clusters[c] >> i) & 1)
          node_to_cluster[i] = c;
      }
    }

    for (const auto &edge : old_edges) {
      int u = edge.first;
      int v = edge.second;
      // If edge connects different clusters, add edge in G'
      if (node_to_cluster[u] != -1 && node_to_cluster[v] != -1 &&
          node_to_cluster[u] != node_to_cluster[v]) {
        new_edges.push_back(
            {(unsigned)node_to_cluster[u], (unsigned)node_to_cluster[v]});
      }
    }
    // Remove duplicates
    std::sort(new_edges.begin(), new_edges.end());
    new_edges.erase(std::unique(new_edges.begin(), new_edges.end()),
                    new_edges.end());

    QueryGraph new_qg(m, new_edges);

    // New Scopes: scope of composite node i is the union of original scopes of
    // nodes in cluster i
    std::vector<uint64_t> new_scopes(m, 0);
    for (unsigned i = 0; i < m; ++i) {
      for (unsigned bit = 0; bit < current_n; ++bit) {
        if ((clusters[i] >> bit) & 1) {
          new_scopes[i] |= scopes[bit];
        }
      }
    }

    // Recursive Call
    auto [macro_cost, macro_tree] = solveRecursive(m, new_qg, new_scopes, k);

    // Combine Costs and Trees
    if (macro_cost == std::numeric_limits<U>::max()) {
      return {std::numeric_limits<U>::max(), nullptr};
    }

    U total_cost = macro_cost;
    for (U c : sub_costs)
      total_cost += c;

    auto final_tree = expandTree(macro_tree, sub_trees);

    return {total_cost, final_tree};
  }

  // Run exact DP (DPsub) on the full graph defined by scopes
  std::pair<U, std::shared_ptr<JoinNode>>
  runExactDP(unsigned n, const std::vector<uint64_t> &scopes) {
    // We need to construct a 'sizes' vector for DPsub.
    // DPsub expects sizes vector of length 2^n.
    // sizes[mask] where mask is subset of 0..n-1
    // maps to original_sizes[ scopes[mask] ]

    std::vector<U> local_sizes(1 << n);
    local_sizes[0] = 0;
    for (uint64_t mask = 1; mask < (1ULL << n); ++mask) {
      uint64_t original_mask = 0;
      for (unsigned i = 0; i < n; ++i) {
        if ((mask >> i) & 1)
          original_mask |= scopes[i];
      }
      // Check bounds
      if (auto it = sizes_.find(original_mask); it != sizes_.end())
        local_sizes[mask] = it->second;
      else
        local_sizes[mask] = std::numeric_limits<U>::max();
    }

    auto [cost, tree] = runMinPlusDPsub(local_sizes);

    // Remove fixTreeMasks here. solveRecursive expects 0..n-1 indices (local).
    // if (tree)
    //   fixTreeMasks(tree, scopes);

    return {cost, tree};
  }

  std::pair<U, std::shared_ptr<JoinNode>>
  runExactDP_restricted(uint64_t cluster_mask,
                        const std::vector<uint64_t> &scopes) {
    // Map cluster_mask bits to 0..popcount-1
    std::vector<unsigned> nodes;
    for (unsigned i = 0; i < 64; ++i) { // n_ is max, but cluster_mask is 64bit
      if ((cluster_mask >> i) & 1)
        nodes.push_back(i);
    }
    unsigned local_n = nodes.size();
    std::vector<uint64_t> local_scopes(local_n);
    for (unsigned i = 0; i < local_n; ++i)
      local_scopes[i] = scopes[nodes[i]];

    auto [cost, tree] = runExactDP(local_n, local_scopes);
    
    // Fix tree masks to map from 0..local_n-1 back to parent indices (nodes[i])
    // Parent indices are in the context of 'current_n' (bitmasks 1<<nodes[i])
    if (tree) {
        std::vector<uint64_t> parent_map(local_n);
        for(unsigned i=0; i<local_n; ++i) {
            parent_map[i] = (1ULL << nodes[i]); // Map index i to bitmask of parent node
        }
        fixTreeMasks(tree, parent_map);
    }

    return {cost, tree};
  }

  void fixTreeMasks(std::shared_ptr<JoinNode> node,
                    const std::vector<uint64_t> &map) {
    if (!node)
      return;
    uint64_t local_set = node->set;
    uint64_t orig_set = 0;
    for (unsigned i = 0; i < map.size(); ++i) {
      if ((local_set >> i) & 1)
        orig_set |= map[i]; // map[i] is the mask in target space
    }
    node->set = orig_set;
    fixTreeMasks(node->left, map);
    fixTreeMasks(node->right, map);
  }

  std::shared_ptr<JoinNode>
  expandTree(std::shared_ptr<JoinNode> macro_node,
             const std::vector<std::shared_ptr<JoinNode>> &sub_trees) {
    if (!macro_node)
      return nullptr;

    // If leaf in macro tree:
    // It represents a SINGLE composite node index.
    // We must check if macro_node->set has exactly one bit set.
    // If so, replace with corresponding sub_tree.

    if ((macro_node->set & (macro_node->set - 1)) == 0) {
      unsigned idx = __builtin_ctzll(macro_node->set);
      if (idx < sub_trees.size()) {
        return sub_trees[idx];
      }
    }

    // Internal node: Recurse
    auto left = expandTree(macro_node->left, sub_trees);
    auto right = expandTree(macro_node->right, sub_trees);

    if (!left || !right)
      return nullptr;

    // Recompute properties
    uint64_t new_set = left->set | right->set;

    // Cost/Size?
    // For MinPlus (C_out), the 'size' member in JoinNode is usually the result
    // cardinality of that join. We need to look it up from GLOBAL sizes using
    // the new_set.
    U new_size = std::numeric_limits<U>::max();
    if (auto it = sizes_.find(new_set); it != sizes_.end())
      new_size = it->second;

    return std::make_shared<JoinNode>(new_set, new_size, left, right);
  }

  std::vector<uint64_t> generate_clusters_for_graph(
      unsigned n, const QueryGraph &qg,
      const std::vector<std::pair<unsigned, unsigned>> &edges,
      const std::vector<uint64_t> &scopes, unsigned max_size) {
    // Agglomerative Clustering
    std::vector<int> parent(n);
    std::vector<uint64_t> cluster_mask(n);
    std::iota(parent.begin(), parent.end(), 0);
    for (unsigned i = 0; i < n; ++i)
      cluster_mask[i] = (1ULL << i);

    auto find_set = [&](unsigned i) {
      int root = i;
      while (parent[root] != root)
        root = parent[root];
      int curr = i;
      while (curr != root) {
        int nxt = parent[curr];
        parent[curr] = root;
        curr = nxt;
      }
      return root;
    };

    auto union_sets = [&](unsigned i, unsigned j) {
      int root_i = find_set(i);
      int root_j = find_set(j);
      if (root_i != root_j) {
        parent[root_j] = root_i;
        cluster_mask[root_i] |= cluster_mask[root_j];
        return true;
      }
      return false;
    };

    // Sort Edges by Weight
    // Weight = size(u) + size(v).
    // Note: size here refers to Cardinality.
    // u, v are indices in current graph 0..n-1.
    // Their cardinalities are `sizes[ scopes[u] ]`.

    using Edge = std::pair<unsigned, unsigned>;
    std::vector<Edge> all_edges = edges;

    std::sort(all_edges.begin(), all_edges.end(),
              [&](const Edge &a, const Edge &b) {
                // Look up global sizes
                double size_a_u = 1e18;
                if (auto it = sizes_.find(scopes[a.first]); it != sizes_.end())
                  size_a_u = it->second;

                double size_a_v = 1e18;
                if (auto it = sizes_.find(scopes[a.second]); it != sizes_.end())
                  size_a_v = it->second;

                double size_b_u = 1e18;
                if (auto it = sizes_.find(scopes[b.first]); it != sizes_.end())
                  size_b_u = it->second;

                double size_b_v = 1e18;
                if (auto it = sizes_.find(scopes[b.second]); it != sizes_.end())
                  size_b_v = it->second;

                return (size_a_u + size_a_v) < (size_b_u + size_b_v);
              });

    for (const auto &edge : all_edges) {
      int root_u = find_set(edge.first);
      int root_v = find_set(edge.second);

      if (root_u != root_v) {
        uint64_t mask_u = cluster_mask[root_u];
        uint64_t mask_v = cluster_mask[root_v];
        if ((unsigned)__builtin_popcountll(mask_u | mask_v) <= max_size) {
          union_sets(root_u, root_v);
        }
      }
    }

    std::vector<uint64_t> result_clusters;
    std::vector<bool> seen(n, false);
    for (unsigned i = 0; i < n; ++i) {
      int root = find_set(i);
      if (!seen[root]) {
        result_clusters.push_back(cluster_mask[root]);
        seen[root] = true;
      }
    }
    std::sort(result_clusters.begin(), result_clusters.end());
    return result_clusters;
  }
};

template <typename U>
std::tuple<MetaInfo, std::string>
optimize_query_union(std::string fn, unsigned n, QueryGraph qg,
                     const std::unordered_map<uint64_t, U> &sizes, std::vector<std::string> tns,
                     unsigned k, // Added threshold parameter
                     bool analyzeRatios = false) {

  double cout_opt = 0.0;

  auto start = std::chrono::high_resolution_clock::now();
  std::cerr << "[START] " << fn << " {UNION_DP} k=" << k << std::endl;

  UnionDPSolver<double> solver(n, qg, sizes);
  // We'll set k=12 for verification as requested.
  auto [ret, tree] = solver.solve(k);

  cout_opt = ret;

  auto end = std::chrono::high_resolution_clock::now();
  auto union_dp_time =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start)
          .count();

  std::cerr << "[STOP] " << fn << " cost=" << cout_opt << std::endl;
  std::cerr << "UNION_DP (time): " << union_dp_time << std::endl;

  if (tree) {
    tree->debug(fn, tns, "union_opt");
  }

  MetaInfo meta_info;
  meta_info.cout_opt = static_cast<uint64_t>(cout_opt);

  return {meta_info,
          fn + "," + std::to_string(n) + "," + std::to_string(cout_opt)};
}

#endif
