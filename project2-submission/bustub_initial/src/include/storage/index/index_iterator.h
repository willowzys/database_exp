#pragma once

#include "storage/page/b_plus_tree_leaf_page.h"
#include "concurrency/transaction.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

  IndexIterator();
  IndexIterator(LeafPage *leaf, int index, BufferPoolManager *bpm, Transaction *txn);
  ~IndexIterator();  // NOLINT

  /** 是否到 end() */
  auto IsEnd() -> bool;

  /** 解引用当前 <key, value> */
  auto operator*() -> const MappingType &;

  /** ++it */
  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    return leaf_ == itr.leaf_ && index_ == itr.index_;
  }

  auto operator!=(const IndexIterator &itr) const -> bool { return !(*this == itr); }

 private:
  /** 当前所在的叶子 page */
  LeafPage *leaf_{nullptr};

  /** 当前所在的 buffer pool Page */
  Page *page_{nullptr};

  int index_{0};

  BufferPoolManager *buffer_pool_manager_{nullptr};

  Transaction *txn_{nullptr};

  bool locked_{false};
};

}  // namespace bustub
