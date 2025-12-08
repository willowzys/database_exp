//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size) : bucket_size_(bucket_size) {
  // +++ 新增：初始化目录，包含一个初始桶 +++
  dir_.push_back(std::make_shared<Bucket>(bucket_size, 0));

}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  
  size_t index = IndexOf(key);
  auto bucket = dir_[index];
  return bucket->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  
  size_t index = IndexOf(key);
  auto bucket = dir_[index];
  return bucket->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  
  while (true) {
    size_t index = IndexOf(key);
    auto bucket = dir_[index];
    
    // Try to insert into the bucket
    if (bucket->Insert(key, value)) {
      return;  // Successfully inserted or updated
    }
    
    // Bucket is full, need to split
    if (bucket->GetDepth() == global_depth_) {
      // Double the directory size
      size_t old_size = dir_.size();
      dir_.resize(old_size * 2);
      
      // Copy the pointers for the new directory entries
      for (size_t i = 0; i < old_size; i++) {
        dir_[i + old_size] = dir_[i];
      }
      
      global_depth_++;
    }
    
    // Split the bucket
    RedistributeBucket(bucket);
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void {
  int local_depth = bucket->GetDepth();
  bucket->IncrementDepth();
  
  // Create a new bucket
  auto new_bucket = std::make_shared<Bucket>(bucket_size_, local_depth + 1);
  num_buckets_++;
  
  // Calculate the split image index
  size_t original_index = 0;
  for (size_t i = 0; i < dir_.size(); i++) {
    if (dir_[i] == bucket) {
      original_index = i;
      break;
    }
  }
  
  size_t split_image_index = original_index ^ (1 << local_depth);
  
  // Update directory pointers
  size_t step = 1 << (local_depth + 1);
  for (size_t i = split_image_index; i < dir_.size(); i += step) {
    dir_[i] = new_bucket;
  }
  
  // Redistribute key-value pairs from the old bucket
  auto &items = bucket->GetItems();
  auto it = items.begin();
  while (it != items.end()) {
    size_t new_index = IndexOf(it->first);
    if (dir_[new_index] == new_bucket) {
      // This item belongs to the new bucket
      new_bucket->Insert(it->first, it->second);
      it = items.erase(it);
    } else {
      ++it;
    }
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (const auto &item : list_) {
    if (item.first == key) {
      value = item.second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto it = list_.begin(); it != list_.end(); ++it) {
    if (it->first == key) {
      list_.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  for (auto &item : list_) {
    if (item.first == key) {
      item.second = value;  // Update value
      return true;
    }
  }
  
  // Check if bucket is full
  if (IsFull()) {
    return false;
  }
  
  // Insert new key-value pair
  list_.emplace_back(key, value);
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
