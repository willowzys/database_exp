#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"
#include "common/exception.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.emplace_back(std::make_shared<Bucket>(bucket_size, 0));
}

// 新增：析构函数实现
template <typename K, typename V>
ExtendibleHashTable<K, V>::~ExtendibleHashTable() {
  std::scoped_lock<std::mutex> lock(latch_);
  dir_.clear();  // 清空目录，释放所有Bucket的shared_ptr引用
  global_depth_ = 0;
  num_buckets_ = 0;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  return std::hash<K>()(key) & ((1 << global_depth_) - 1);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  if (dir_index < 0 || static_cast<size_t>(dir_index) >= dir_.size()) {
    return -1;
  }
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t idx = IndexOf(key);
  return dir_[idx]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t idx = IndexOf(key);
  return dir_[idx]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::SplitBucket(std::shared_ptr<Bucket> bucket, size_t bucket_idx) {
  auto new_bucket = std::make_shared<Bucket>(bucket_size_, bucket->GetDepth());
  num_buckets_++;

  int old_depth = bucket->GetDepth();
  bucket->IncrementDepth();
  new_bucket->IncrementDepth();

  auto &items = bucket->GetItems();
  auto it = items.begin();
  while (it != items.end()) {
    size_t hash = std::hash<K>()(it->first);
    bool belongs_to_new = (hash >> old_depth) & 1;  // 用旧深度判断归属

    if (belongs_to_new) {
      new_bucket->Insert(it->first, it->second);
      it = items.erase(it);
    } else {
      ++it;
    }
  }

  // 遍历所有目录项，更新指向旧桶的条目
  for (size_t i = 0; i < dir_.size(); ++i) {
    if (dir_[i] == bucket) {
      bool point_to_new = (i >> old_depth) & 1;  // 用旧深度判断新桶方向
      if (point_to_new) {
        dir_[i] = new_bucket;
      }
    }
  }
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);

  while (true) {
    size_t idx = IndexOf(key);
    auto bucket = dir_[idx];

    if (bucket->Insert(key, value)) {
      return;
    }

    if (bucket->GetDepth() == global_depth_) {
      size_t old_size = dir_.size();
      global_depth_++;
      dir_.resize(old_size * 2);
      for (size_t i = 0; i < old_size; ++i) {
        dir_[i + old_size] = dir_[i];  // 扩展目录，复制原有指针
      }
    }

    SplitBucket(bucket, idx);
  }
}

// Bucket 成员函数实现
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t size, int depth) : size_(size), depth_(depth) {}

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
      item.second = value;
      return true;
    }
  }

  if (IsFull()) {
    return false;
  }

  list_.emplace_back(key, value);
  return true;
}

// 显式实例化
template class ExtendibleHashTable<bustub::page_id_t, bustub::Page *>;
template class ExtendibleHashTable<bustub::Page *, std::list<bustub::Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;
 
}  // namespace bustub
