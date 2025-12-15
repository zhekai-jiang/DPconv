#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <random>
#include <ranges>
#include <thread>
#include <unordered_map>
#include <vector>

#include "UnionDPRunner.hpp"

namespace fs = std::filesystem;

// Source: https://stackoverflow.com/a/18681293/10348282
inline bool endswith(const std::string &s, const std::string &suffix) {
  return suffix.size() > s.size()
             ? false
             : (s.rfind(suffix) == s.size() - suffix.size());
}

std::tuple<MetaInfo, std::string>
run_query_from_file(std::string filename, unsigned k,
                    bool analyzeRatios = false) {
  assert(
      endswith(filename, ".csv")); // Ensure endswith is available or redefined
  std::cerr << "[start] " << filename << std::endl;
  std::ifstream in(filename);
  assert(in.is_open());
  unsigned n, m, s;
  in >> n >> m >> s;
  std::cerr << "n=" << n << " m=" << m << " s=" << s << std::endl;
  std::vector<std::string> tns(n);
  std::vector<std::pair<unsigned, unsigned>> edges(m);
  for (unsigned index = 0; index != n; ++index) {
    in >> tns[index];
  }
  for (unsigned index = 0; index != m; ++index) {
    in >> edges[index].first >> edges[index].second;
  }
  std::unordered_map<uint64_t, double> sizes;

  std::cout << "Reading sizes..." << std::endl;
  double maxCard = 0;
  while (s--) {
    uint64_t bitset;
    double card;
    in >> bitset >> card;
    sizes[bitset] = static_cast<double>(card);
  }

  QueryGraph qg(n, edges);

  return optimize_query_union(filename, n, qg, sizes, tns, k, analyzeRatios);
}

// Helper to separate file/directory list expansion
std::vector<std::string> expand_inputs(const std::vector<std::string> &inputs) {
  std::vector<std::string> fns;
  for (const auto &input : inputs) {
    if (fs::is_directory(input)) {
      for (const auto &entry : fs::directory_iterator(input)) {
        if (entry.is_regular_file() &&
            endswith(entry.path().string(), ".csv")) {
          fns.push_back(entry.path());
        }
      }
    } else {
      fns.push_back(input);
    }
  }
  return fns;
}

std::vector<std::tuple<std::string, MetaInfo>>
benchmark_inputs(const std::vector<std::string> &inputs, unsigned k,
                 bool analyzeRatios = false) {
  auto fns = expand_inputs(inputs);
  std::vector<std::tuple<std::string, MetaInfo>> ratios;

  for (const auto &fn : fns) {
    // Process each file
    auto [meta_info, actual_ret] = run_query_from_file(fn, k, analyzeRatios);
    ratios.push_back({fn, meta_info});
  }
  return ratios;
}

int main(int argc, char **argv) {
  if (argc < 3) {
    std::cerr << "Usage: " << argv[0] << " <k:int> <input_file_or_dir>..."
              << std::endl;
    exit(-1);
  }

  // 1. Parse Threshold k
  unsigned k = std::stoi(argv[1]);
  std::cerr << "Benchmark UnionDP with k=" << k << std::endl;

  // 2. Collect Input Arguments
  std::vector<std::string> inputs;
  for (int i = 2; i < argc; ++i) {
    inputs.push_back(argv[i]);
  }

  auto ratios = benchmark_inputs(inputs, k, true);

  std::ofstream out("../cout_uniondp.csv");
  assert(out.is_open());
  out << "query" << ","
      << "n" << ","
      << "cost" << std::endl;
  for (auto [fn, meta_info] : ratios) {
    std::ifstream in(fn);
    assert(in.is_open());
    unsigned n;
    in >> n;
    in.close();

    out << fn << "," << n << "," << meta_info.cout_opt << "\n";
  }
  out.close();

  return 0;
}
