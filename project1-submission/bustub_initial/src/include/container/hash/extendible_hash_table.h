//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.h
//
// Identification: src/include/container/hash/extendible_hash_table.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * extendible_hash_table.h
 *
 * Implementation of in-memory hash table using extendible hashing
 */

#pragma once

#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <utility>
#include <vector>

#include "container/hash/hash_table.h"

namespace bustub {

/**
 * ExtendibleHashTable implements a hash table using the extendible hashing algorithm.
 * @tparam K key type
 * @tparam V value type
 */
template <typename K, typename V>
class ExtendibleHashTable : public HashTable<K, V> {
 public:
  /**
   * @brief Create a new ExtendibleHashTable.
   * @param bucket_size: fixed size for each bucket
   */
  explicit ExtendibleHashTable(size_t bucket_size);

  /**
   * @brief Destroy the ExtendibleHashTable object
   */
  ~ExtendibleHashTable() override;  // 新增：显式析构函数

  /**
   * @brief Get the global depth of the directory.
   * @return The global depth of the directory.
   */
  auto GetGlobalDepth() const -> int;

  /**
   * @brief Get the local depth of the bucket that the given directory index points to.
   * @param dir_index The index in the directory.
   * @return The local depth of the bucket.
   */
  auto GetLocalDepth(int dir_index) const -> int;

  /**
   * @brief Get the number of buckets in the directory.
   * @return The number of buckets in the directory.
   */
  auto GetNumBuckets() const -> int;

  /**
   * @brief Find the value associated with the given key.
   * @param key The key to be searched.
   * @param[out] value The value associated with the key.
   * @return True if the key is found, false otherwise.
   */
  auto Find(const K &key, V &value) -> bool override;

  /**
   * @brief Insert the given key-value pair into the hash table.
   * If a key already exists, the value should be updated.
   * If the bucket is full, split and redistribute as needed.
   * @param key The key to be inserted.
   * @param value The value to be inserted.
   */
  void Insert(const K &key, const V &value) override;

  /**
   * @brief Remove the corresponding key-value pair in the hash table.
   * @param key The key to be deleted.
   * @return True if the key exists, false otherwise.
   */
  auto Remove(const K &key) -> bool override;

  /**
   * Bucket class for each hash table bucket that the directory points to.
   */
  class Bucket {
   public:
    explicit Bucket(size_t size, int depth = 0);

    /** @brief Destroy the Bucket object */
    ~Bucket() { list_.clear(); }  // 新增：显式析构函数，清空list

    /** @brief Check if a bucket is full. */
    inline auto IsFull() const -> bool { return list_.size() == size_; }

    /** @brief Get the local depth of the bucket. */
    inline auto GetDepth() const -> int { return depth_; }

    /** @brief Increment the local depth of a bucket. */
    inline void IncrementDepth() { depth_++; }

    inline auto GetItems() -> std::list<std::pair<K, V>> & { return list_; }

    /**
     * @brief Find the value associated with the given key in the bucket.
     * @param key The key to be searched.
     * @param[out] value The value associated with the key.
     * @return True if the key is found, false otherwise.
     */
    auto Find(const K &key, V &value) -> bool;

    /**
     * @brief Remove the corresponding key-value pair in the bucket.
     * @param key The key to be deleted.
     * @return True if the key exists, false otherwise.
     */
    auto Remove(const K &key) -> bool;

    /**
     * @brief Insert the given key-value pair into the bucket.
     * @param key The key to be inserted.
     * @param value The value to be inserted.
     * @return True if inserted, false if full or key exists (and updated).
     */
    auto Insert(const K &key, const V &value) -> bool;

   private:
    size_t size_;       // 桶的最大容量
    int depth_;         // 桶的本地深度
    std::list<std::pair<K, V>> list_;  // 存储键值对的列表
  };

 private:
  // 成员声明顺序需与构造函数初始化列表一致
  int global_depth_;          // 全局深度
  size_t bucket_size_;        // 每个桶的容量
  int num_buckets_;           // 桶的数量
  mutable std::mutex latch_;  // 互斥锁
  std::vector<std::shared_ptr<Bucket>> dir_;  // 目录（指向桶的指针向量）

  /**
   * @brief 计算键对应的目录索引
   * @param key 目标键
   * @return 目录索引
   */
  auto IndexOf(const K &key) -> size_t;

  /**
   * @brief 分裂桶并重新分布键值对
   * @param bucket 待分裂的桶
   * @param bucket_idx 桶在目录中的索引
   */
  void SplitBucket(std::shared_ptr<Bucket> bucket, size_t bucket_idx);

  // 内部获取函数（需先加锁）
  auto GetGlobalDepthInternal() const -> int { return global_depth_; }
  auto GetLocalDepthInternal(int dir_index) const -> int { return dir_[dir_index]->GetDepth(); }
  auto GetNumBucketsInternal() const -> int { return num_buckets_; }
};

}  // namespace bustub
