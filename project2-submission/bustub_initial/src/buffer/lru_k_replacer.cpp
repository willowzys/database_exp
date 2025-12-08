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
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock lock(latch_);
  
  if (curr_size_ == 0) {
    return false;
  }

  frame_id_t candidate = -1;
  size_t candidate_distance = 0;
  size_t candidate_earliest = 0;

  for (const auto &[fid, frame_info] : frame_table_) {
    if (!frame_info.is_evictable) {
      continue;
    }

    size_t distance = CalculateBackwardKDistance(frame_info);
    size_t earliest = frame_info.earliest_timestamp;

    if (candidate == -1) {
      candidate = fid;
      candidate_distance = distance;
      candidate_earliest = earliest;
      continue;
    }

    // Compare backward k-distance
    if (distance > candidate_distance) {
      candidate = fid;
      candidate_distance = distance;
      candidate_earliest = earliest;
    } else if (distance == candidate_distance) {
      // If both have +inf distance, choose the one with earliest timestamp
      if (distance == std::numeric_limits<size_t>::max()) {
        if (earliest < candidate_earliest) {
          candidate = fid;
          candidate_earliest = earliest;
        }
      }
      // If both have finite distance and same value, LRU-K doesn't specify which to choose
      // We'll stick with the first found in this case
    }
  }

  if (candidate != -1) {
    *frame_id = candidate;
    frame_table_.erase(candidate);
    curr_size_--;
    return true;
  }

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock lock(latch_);
  
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw bustub::Exception("frame_id is invalid");
  }

  current_timestamp_++;
  
  auto &frame_info = frame_table_[frame_id];
  frame_info.history.push_back(current_timestamp_);
  
  // Maintain only k most recent accesses
  if (frame_info.history.size() > k_) {
    frame_info.history.pop_front();
  }
  
  // Update earliest timestamp if this is the first access
  if (frame_info.history.size() == 1) {
    frame_info.earliest_timestamp = current_timestamp_;
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock lock(latch_);
  
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw bustub::Exception("frame_id is invalid");
  }

  auto it = frame_table_.find(frame_id);
  if (it == frame_table_.end()) {
    return;
  }

  auto &frame_info = it->second;
  if (frame_info.is_evictable == set_evictable) {
    return;
  }

  frame_info.is_evictable = set_evictable;
  if (set_evictable) {
    curr_size_++;
  } else {
    curr_size_--;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock lock(latch_);
  
  auto it = frame_table_.find(frame_id);
  if (it == frame_table_.end()) {
    return;
  }

  if (!it->second.is_evictable) {
    throw bustub::Exception("Cannot remove non-evictable frame");
  }

  frame_table_.erase(it);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock lock(latch_);
  return curr_size_;
}

auto LRUKReplacer::CalculateBackwardKDistance(const FrameInfo &frame_info) -> size_t {
  if (frame_info.history.size() < k_) {
    return std::numeric_limits<size_t>::max();
  }
  
  // The k-th most recent access is at the front of the list
  // Backward k-distance = current_timestamp - kth_previous_access_timestamp
  return current_timestamp_ - frame_info.history.front();
}

}  // namespace bustub