//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree.h
//
// Identification: src/include/storage/index/b_plus_tree.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <queue>
#include <string>
#include <vector>

#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

class BufferPoolManager;

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>
#define READ_MODE 0
#define INSERT_MODE 1
#define REMOVE_MODE 2

/**
 * Main class providing the API for the Interactive B+ Tree.
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;
  using BPlusTreePage = ::bustub::BPlusTreePage;

 public:
  explicit BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                     int leaf_max_size = LEAF_PAGE_SIZE, int internal_max_size = INTERNAL_PAGE_SIZE);

  // Returns true if this B+ tree has no keys and values.
  auto IsEmpty() const -> bool;

  // Insert a key-value pair into this B+ tree.
  auto Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr) -> bool;

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *transaction = nullptr);

  // return the value associated with a given key
  auto GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction = nullptr) -> bool;

  // return the page id of the root node
  auto GetRootPageId() -> page_id_t;

  // index iterator
  auto Begin() -> INDEXITERATOR_TYPE;
  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;
  auto End() -> INDEXITERATOR_TYPE;

  // print the B+ tree
  void Print(BufferPoolManager *bpm);

  // draw the B+ tree
  void Draw(BufferPoolManager *bpm, const std::string &outf);

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *transaction = nullptr);

 private:
  // 在并发模式下寻找叶子结点
  auto FindLeaf(const KeyType &key, int latch_mode, Transaction *transaction = nullptr) -> Page *;

  // 释放事务中记录的整条锁链
  void ReleaseLockChain(Transaction *transaction, int latch_mode);

  // 释放单个 page 的 latch + unpin
  void ReleaseSinglePage(Page *page_ptr, int latch_mode);

  // 分裂一个节点，返回新 page*
  template <class N>
  auto SplitNode(N *old_node, Transaction *transaction) -> Page *;

  // 分裂后向父结点插入新键值
  auto InsertIntoParent(const KeyType &old_key, BPlusTreePage *old_node, const KeyType &new_key,
                        BPlusTreePage *new_node, Transaction *transaction) -> bool;

  // 删除后再平衡（借 / 合并），对任意 N（Leaf / Internal）
  template <class N>
  void RedistributeOrMerge(N *node, Transaction *transaction);

  // 节点之间借 key/value
  template <typename N>
  void RedistributeNode(N *neighbor_node, N *cur_node, bool from_prev);

  // 合并两个节点
  template <typename N>
  void CoalesceNode(N *neighbor_node, N *cur_node);

  // 叶子节点合并实现
  void CoalesceLeafNode(LeafPage *neighbor_leaf, LeafPage *cur_leaf);

  // 内部节点合并实现
  void CoalesceInternalNode(InternalPage *neighbor_internal, InternalPage *cur_internal);

  // 叶子节点借 key/value 实现
  void RedistributeLeafNode(LeafPage *neighbor_leaf, LeafPage *cur_leaf, bool from_prev);

  // 内部节点借 entry 实现
  void RedistributeInternalNode(InternalPage *neighbor_internal, InternalPage *cur_internal, bool from_prev);

  // 删除后若 node 是 root，处理 root 特殊情况
  template <typename N>
  void HandleRootAfterDelete(N *node, Transaction *transaction);

  // 回收事务中标记删除的页面
  void ReclaimDeletedPages(Transaction *transaction);

  // 实际执行 DeletePage 的封装，方便以后改策略
  void DeletePageHelper(page_id_t page_id);

  // 更新 / 插入 root page id 到 header_page
  void UpdateRootPageId(int insert_record = 0);

  /* Debug Routines for FREE!! */
  void ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const;

  void ToString(BPlusTreePage *page, BufferPoolManager *bpm) const;

  // member variable
  std::string index_name_;
  page_id_t root_page_id_;

  mutable ReaderWriterLatch tree_guard_{};

  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;
  int leaf_max_size_;
  int internal_max_size_;
};

}  // namespace bustub
