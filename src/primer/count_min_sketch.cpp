//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// count_min_sketch.cpp
//
// Identification: src/primer/count_min_sketch.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/count_min_sketch.h"
#include <atomic>
#include <mutex>
#include <stdexcept>
#include <string>
namespace bustub {

/**
 * Constructor for the count-min sketch.
 *
 * @param width The width of the sketch matrix.
 * @param depth The
 * depth of the sketch matrix.
 * @throws std::invalid_argument if width or depth are zero.
 */
template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(uint32_t width, uint32_t depth) : width_(width), depth_(depth) {
  /** @TODO(student) Implement this function! */

  /** @spring2026 PLEASE DO NOT MODIFY THE FOLLOWING */
  // Initialize seeded hash functions
  if (width_ == 0 || depth_ == 0) {
    throw std::invalid_argument("CountMinSketch dimensions must be postive");
  }

  sketch_.resize(depth_);
  for (auto &row : sketch_) {
    row = std::vector<std::atomic<uint32_t>>(width_);

    for (auto &counter : row) {
      counter.store(0, std::memory_order_relaxed);
    }
  }
  hash_functions_.reserve(depth_);

  for (uint32_t i = 0; i < depth_; i++) {
    hash_functions_.push_back(HashFunction(SEED_BASE + i));
  }
}

template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(CountMinSketch &&other) noexcept {
  std::lock_guard<std::mutex> lock(other.mutex_);

  width_ = other.width_;
  depth_ = other.depth_;
  sketch_ = std::move(other.sketch_);

  hash_functions_.clear();
  hash_functions_.reserve(depth_);
  for (uint32_t i = 0; i < depth_; i++) {
    hash_functions_.push_back(HashFunction(SEED_BASE + i));
  }
}

template <typename KeyType>
auto CountMinSketch<KeyType>::operator=(CountMinSketch &&other) noexcept -> CountMinSketch & {
  if (this == &other) {
    return *this;
  }

  std::scoped_lock lock(mutex_, other.mutex_);

  width_ = other.width_;
  depth_ = other.depth_;
  sketch_ = std::move(other.sketch_);

  hash_functions_.clear();
  hash_functions_.reserve(depth_);
  for (uint32_t i = 0; i < depth_; i++) {
    hash_functions_.push_back(HashFunction(SEED_BASE + i));
  }

  return *this;
}

template <typename KeyType>
void CountMinSketch<KeyType>::Insert(const KeyType &item) {
  for (uint32_t row = 0; row < depth_; row++) {
    size_t col = hash_functions_[row](item);
    sketch_[row][col].fetch_add(1, std::memory_order_relaxed);
  }
}

template <typename KeyType>
auto CountMinSketch<KeyType>::Count(const KeyType &item) const -> uint32_t {
  uint32_t min_count = std::numeric_limits<uint32_t>::max();

  for (uint32_t row = 0; row < depth_; row++) {
    size_t col = hash_functions_[row](item);
    uint32_t count = sketch_[row][col].load(std::memory_order_relaxed);
    min_count = std::min(min_count, count);
  }

  return min_count;
}

template <typename KeyType>
void CountMinSketch<KeyType>::Merge(const CountMinSketch<KeyType> &other) {
  if (width_ != other.width_ || depth_ != other.depth_) {
    throw std::invalid_argument("Incompatible CountMinSketch dimensions for merge.");
  }

  if (this == &other) {
    std::lock_guard<std::mutex> lock(mutex_);

    for (uint32_t row = 0; row < depth_; row++) {
      for (uint32_t col = 0; col < width_; col++) {
        uint32_t value = sketch_[row][col].load(std::memory_order_relaxed);
        sketch_[row][col].fetch_add(value, std::memory_order_relaxed);
      }
    }

    return;
  }

  std::scoped_lock lock(mutex_, other.mutex_);

  for (uint32_t row = 0; row < depth_; row++) {
    for (uint32_t col = 0; col < width_; col++) {
      uint32_t value = other.sketch_[row][col].load(std::memory_order_relaxed);
      sketch_[row][col].fetch_add(value, std::memory_order_relaxed);
    }
  }
}

template <typename KeyType>
void CountMinSketch<KeyType>::Clear() {
  std::lock_guard<std::mutex> lock(mutex_);

  for (auto &row : sketch_) {
    for (auto &counter : row) {
      counter.store(0, std::memory_order_relaxed);
    }
  }
}

template <typename KeyType>
auto CountMinSketch<KeyType>::TopK(uint16_t k, const std::vector<KeyType> &candidates)
    -> std::vector<std::pair<KeyType, uint32_t>> {
  std::vector<std::pair<KeyType, uint32_t>> result;
  result.reserve(candidates.size());

  for (const auto &candidate : candidates) {
    result.emplace_back(candidate, Count(candidate));
  }

  std::sort(result.begin(), result.end(), [](const auto &a, const auto &b) {
    if (a.second == b.second) {
      return a.first < b.first;
    }

    return a.second > b.second;
  });

  if (result.size() > k) {
    result.resize(k);
  }

  return result;
}

// Explicit instantiations for all types used in tests
template class CountMinSketch<std::string>;
template class CountMinSketch<int64_t>;  // For int64_t tests
template class CountMinSketch<int>;      // This covers both int and int32_t
}  // namespace bustub
