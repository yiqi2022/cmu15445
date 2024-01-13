//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <algorithm>
#include <cstddef>
#include <iostream>
#include <iterator>
#include "common/exception.h"
#include "fmt/core.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}
LRUKReplacer::~LRUKReplacer() {
  for (auto node : less_k_) {  // 防止内存泄露
    delete node;
  }
  for (auto node : more_k_) {
    delete node;
  }
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(mu_);
  if (!less_k_.empty()) {
    for (auto node : less_k_) {
      if (node->is_evictable_) {
        *frame_id = node->fid_;
        less_k_.remove(node);
        node_store_.erase(*frame_id);
        curr_size_--;
        delete node;
        return true;
      }
    }
  }
  if (!more_k_.empty()) {
    for (auto node : more_k_) {
      if (node->is_evictable_) {
        *frame_id = node->fid_;
        more_k_.remove(node);
        node_store_.erase(*frame_id);
        curr_size_--;
        delete node;
        return true;
      }
    }
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  std::lock_guard<std::mutex> lock(mu_);
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw "unexpected fram_id";
  }
  current_timestamp_++;
  if (node_store_.find(frame_id) == node_store_.end()) {
    auto node = new LRUKNode;
    node->fid_ = frame_id;
    node->history_.emplace_back(current_timestamp_);
    node_store_[frame_id] = node;
    less_k_.emplace_back(node);
    return;
  }
  auto node = node_store_[frame_id];
  node->history_.emplace_back(current_timestamp_);
  auto history_size = node->history_.size();
  if (history_size < k_) {
    return;
  }
  if (history_size == k_) {
    less_k_.remove(node);
  } else {
    more_k_.remove(node);
    node->history_.pop_front();
  }
  bool done = false;
  auto kth = node->history_.front();
  for (auto it = more_k_.begin(); it != more_k_.end(); ++it) {
    if ((*it)->history_.front() > kth) {
      more_k_.emplace(it, node);
      done = true;
      break;
    }
  }
  if (!done) {
    more_k_.emplace_back(node);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(mu_);
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw "unexpected fram_id";
  }
  if (node_store_.find(frame_id) == node_store_.end()) {
    return;
  }
  bool is_evictable = node_store_[frame_id]->is_evictable_;
  if (is_evictable) {
    if (set_evictable) {
      return;
    }
    node_store_[frame_id]->is_evictable_ = set_evictable;
    curr_size_--;
  } else {
    if (!set_evictable) {
      return;
    }
    node_store_[frame_id]->is_evictable_ = set_evictable;
    curr_size_++;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(mu_);
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw "unexpected fram_id";
  }
  if (node_store_.find(frame_id) == node_store_.end()) {
    return;
  }
  auto node = node_store_[frame_id];
  if (!node->is_evictable_) {
    throw "Remove is called on a non-evictable frame";
  }
  if (node->history_.size() < k_) {
    less_k_.remove(node);
  } else {
    more_k_.remove(node);
  }
  delete node;
  node_store_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(mu_);
  return curr_size_;
}

}  // namespace bustub
