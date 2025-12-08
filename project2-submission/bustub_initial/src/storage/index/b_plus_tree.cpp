#include <string>

#include "buffer/buffer_pool_manager.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/header_page.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result,
                              Transaction *transaction) -> bool {  // NOLINT
  result->clear();

  tree_guard_.RLock();
  if (transaction != nullptr) {
    transaction->AddIntoPageSet(nullptr);
  }

  if (IsEmpty()) {
    if (transaction != nullptr) {
      ReleaseLockChain(transaction, READ_MODE);
    } else {
      tree_guard_.RUnlock();
    }
    return false;
  }

  Page *page_ptr = FindLeaf(key, READ_MODE, transaction);
  auto *leaf = reinterpret_cast<LeafPage *>(page_ptr->GetData());

  ValueType value{};
  bool found = leaf->Lookup(key, &value, comparator_);

  if (transaction != nullptr) {
    ReleaseLockChain(transaction, READ_MODE);
  } else {
    page_ptr->RUnlatch();
    buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), false);
    tree_guard_.RUnlock();
  }

  if (found) {
    result->push_back(value);
  }
  return found;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value,
                            Transaction *transaction) -> bool {  // NOLINT
  tree_guard_.WLock();
  if (transaction != nullptr) {
    transaction->AddIntoPageSet(nullptr);
  }

  if (IsEmpty()) {
    // 创建第一棵树：单个叶子即 root
    page_id_t new_root_pid;
    Page *page = buffer_pool_manager_->NewPage(&new_root_pid);
    if (page == nullptr) {
      if (transaction != nullptr) {
        ReleaseLockChain(transaction, INSERT_MODE);
      } else {
        tree_guard_.WUnlock();
      }
      throw Exception("BPlusTree::Insert: cannot allocate new page for root");
    }

    auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());
    leaf->Init(new_root_pid, INVALID_PAGE_ID, leaf_max_size_);
    bool ok = (leaf->Insert(key, value, comparator_) != -1);

    root_page_id_ = new_root_pid;
    UpdateRootPageId(1);

    buffer_pool_manager_->UnpinPage(new_root_pid, true);

    if (transaction != nullptr) {
      ReleaseLockChain(transaction, INSERT_MODE);
    } else {
      tree_guard_.WUnlock();
    }

    return ok;
  }

  Page *page_ptr = FindLeaf(key, INSERT_MODE, transaction);
  auto *leaf = reinterpret_cast<LeafPage *>(page_ptr->GetData());

  int rv = leaf->Insert(key, value, comparator_);
  if (rv == -1) {
    // 重复键，直接返回
    if (transaction != nullptr) {
      ReleaseLockChain(transaction, INSERT_MODE);
    } else {
      page_ptr->WUnlatch();
      buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), false);
      tree_guard_.WUnlock();
    }
    return false;
  }

  // 未触发分裂
  if (leaf->GetSize() < leaf->GetMaxSize()) {
    if (transaction != nullptr) {
      ReleaseLockChain(transaction, INSERT_MODE);
    } else {
      page_ptr->WUnlatch();
      buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), true);
      tree_guard_.WUnlock();
    }
    return true;
  }

  // 需要分裂叶子
  Page *new_page_ptr = SplitNode<LeafPage>(leaf, transaction);
  auto *new_leaf = reinterpret_cast<LeafPage *>(new_page_ptr->GetData());

  // 更新叶子链表
  new_leaf->SetNextPageId(leaf->GetNextPageId());
  leaf->SetNextPageId(new_leaf->GetPageId());

  // 将部分元素从 leaf 挪到 new_leaf，保证两边至少 min_size
  while (new_leaf->GetSize() < new_leaf->GetMinSize()) {
    leaf->ShiftTailItemToFront(new_leaf);
  }

  KeyType old_first_key = leaf->KeyAt(0);
  KeyType new_first_key = new_leaf->KeyAt(0);

  if (leaf->IsRootPage()) {
    // 叶子本身是 root，需要创建新的 root internal
    page_id_t new_root_pid;
    Page *root_page = buffer_pool_manager_->NewPage(&new_root_pid);
    if (root_page == nullptr) {
      if (transaction != nullptr) {
        ReleaseLockChain(transaction, INSERT_MODE);
      } else {
        page_ptr->WUnlatch();
        new_page_ptr->WUnlatch();
        buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), true);
        buffer_pool_manager_->UnpinPage(new_page_ptr->GetPageId(), true);
        tree_guard_.WUnlock();
      }
      throw Exception("BPlusTree::Insert: cannot allocate new internal root");
    }

    auto *root_node = reinterpret_cast<InternalPage *>(root_page->GetData());
    root_node->Init(new_root_pid, INVALID_PAGE_ID, internal_max_size_);
    root_node->BuildRoot(old_first_key, leaf->GetPageId(), new_first_key, new_leaf->GetPageId());

    leaf->SetParentPageId(new_root_pid);
    new_leaf->SetParentPageId(new_root_pid);

    root_page_id_ = new_root_pid;
    UpdateRootPageId(0);

    buffer_pool_manager_->UnpinPage(new_root_pid, true);
  } else {
    // 普通叶子分裂，把新信息插入父结点
    InsertIntoParent(old_first_key, leaf, new_first_key, new_leaf, transaction);
  }

  buffer_pool_manager_->UnpinPage(new_page_ptr->GetPageId(), true);

  if (transaction != nullptr) {
    ReleaseLockChain(transaction, INSERT_MODE);
  } else {
    page_ptr->WUnlatch();
    buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), true);
    tree_guard_.WUnlock();
  }
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::SplitNode(N *old_node, Transaction *transaction) -> Page * {
  page_id_t new_pid;
  Page *new_page_ptr = buffer_pool_manager_->NewPage(&new_pid);
  if (new_page_ptr == nullptr) {
    throw Exception("SplitNode: NewPage failed");
  }

  auto *new_node = reinterpret_cast<N *>(new_page_ptr->GetData());
  new_node->Init(new_pid, old_node->GetParentPageId(), old_node->GetMaxSize());

  return new_page_ptr;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoParent(const KeyType &old_key, BPlusTreePage *old_node, const KeyType &new_key,
                                      BPlusTreePage *new_node, Transaction *transaction) -> bool {
  page_id_t parent_id = old_node->GetParentPageId();
  if (parent_id == INVALID_PAGE_ID) {
    // 旧结点是 root，需要新建一个 root internal
    page_id_t new_root_pid;
    Page *root_page = buffer_pool_manager_->NewPage(&new_root_pid);
    if (root_page == nullptr) {
      throw Exception("InsertIntoParent: NewPage root failed");
    }

    auto *root_node = reinterpret_cast<InternalPage *>(root_page->GetData());
    root_node->Init(new_root_pid, INVALID_PAGE_ID, internal_max_size_);
    root_node->BuildRoot(old_key, old_node->GetPageId(), new_key, new_node->GetPageId());

    old_node->SetParentPageId(new_root_pid);
    new_node->SetParentPageId(new_root_pid);

    root_page_id_ = new_root_pid;
    UpdateRootPageId(0);

    buffer_pool_manager_->UnpinPage(new_root_pid, true);
    return true;
  }

  Page *parent_page = buffer_pool_manager_->FetchPage(parent_id);
  if (parent_page == nullptr) {
    throw Exception("InsertIntoParent: FetchPage(parent) failed");
  }
  auto *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());

  // 更新 old_node 对应的 key（它的第一个 key 可能已经变化）
  int idx = parent_node->ValueIndex(old_node->GetPageId());
  parent_node->SetKeyAt(idx, old_key);

  parent_node->Insert(new_key, new_node->GetPageId(), comparator_);
  new_node->SetParentPageId(parent_id);

  // 如果父结点太大，继续分裂
  if (parent_node->GetSize() > parent_node->GetMaxSize()) {
    Page *new_internal_page = SplitNode<InternalPage>(parent_node, transaction);
    auto *new_internal = reinterpret_cast<InternalPage *>(new_internal_page->GetData());

    // 把一部分 entry 挪到新的 internal
    while (new_internal->GetSize() < new_internal->GetMinSize()) {
      parent_node->RelocateTailToFront(new_internal, buffer_pool_manager_);
    }

    KeyType middle_key = new_internal->KeyAt(0);
    InsertIntoParent(parent_node->KeyAt(0), parent_node, middle_key, new_internal, transaction);

    buffer_pool_manager_->UnpinPage(new_internal_page->GetPageId(), true);
  }

  buffer_pool_manager_->UnpinPage(parent_id, true);
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {  // NOLINT
  tree_guard_.WLock();
  if (transaction != nullptr) {
    transaction->AddIntoPageSet(nullptr);
  }

  if (IsEmpty()) {
    if (transaction != nullptr) {
      ReleaseLockChain(transaction, REMOVE_MODE);
    } else {
      tree_guard_.WUnlock();
    }
    return;
  }

  Page *page_ptr = FindLeaf(key, REMOVE_MODE, transaction);
  auto *leaf = reinterpret_cast<LeafPage *>(page_ptr->GetData());

  int idx = leaf->KeyIndex(key, comparator_);
  if (idx >= leaf->GetSize() || comparator_(leaf->KeyAt(idx), key) != 0) {
    // 没有要删除的 key
    if (transaction != nullptr) {
      ReleaseLockChain(transaction, REMOVE_MODE);
    } else {
      page_ptr->WUnlatch();
      buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), false);
      tree_guard_.WUnlock();
    }
    return;
  }

  leaf->RemoveAt(idx);

  // 删除后需要考虑再平衡
  if (leaf->GetSize() < leaf->GetMinSize()) {
    RedistributeOrMerge(leaf, transaction);
  }

  // 统一回收已标记删除的页
  if (transaction != nullptr) {
    ReclaimDeletedPages(transaction);
    ReleaseLockChain(transaction, REMOVE_MODE);
  } else {
    page_ptr->WUnlatch();
    buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), true);
    tree_guard_.WUnlock();
  }
}

/*****************************************************************************
 * DELETE HELPERS
 *****************************************************************************/

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::HandleRootAfterDelete(N *node, Transaction *transaction) {
  if (!node->IsRootPage()) {
    return;
  }

  if (node->IsLeafPage()) {
    auto *leaf_root = reinterpret_cast<LeafPage *>(node);
    if (leaf_root->GetSize() == 0) {
      // 叶子 root：空则整个树置空
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId(0);
    }
  } else {
    auto *internal_root = reinterpret_cast<InternalPage *>(node);
    if (internal_root->GetSize() == 1) {
      // 把唯一 child 提升为新的 root
      page_id_t child_pid = internal_root->ValueAt(0);
      root_page_id_ = child_pid;
      UpdateRootPageId(0);

      Page *child_page = buffer_pool_manager_->FetchPage(child_pid);
      auto *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
      child_node->SetParentPageId(INVALID_PAGE_ID);
      buffer_pool_manager_->UnpinPage(child_pid, true);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::RedistributeOrMerge(N *node, Transaction *transaction) {
  // root 特判
  if (node->IsRootPage()) {
    HandleRootAfterDelete(node, transaction);
    return;
  }

  page_id_t parent_pid = node->GetParentPageId();
  Page *parent_page = buffer_pool_manager_->FetchPage(parent_pid);
  if (parent_page == nullptr) {
    throw Exception("RedistributeOrMerge: FetchPage(parent) failed");
  }
  auto *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());

  int index = parent->ValueIndex(node->GetPageId());

  // 先 try 向左兄弟借
  if (index > 0) {
    page_id_t left_pid = parent->ValueAt(index - 1);
    Page *left_page = buffer_pool_manager_->FetchPage(left_pid);
    left_page->WLatch();
    auto *left = reinterpret_cast<N *>(left_page->GetData());

    if (left->GetSize() > left->GetMinSize()) {
      RedistributeNode(left, node, true);
      if (node->IsLeafPage()) {
        parent->SetKeyAt(index, node->KeyAt(0));
      } else {
        parent->SetKeyAt(index, node->KeyAt(0));
      }
      left_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(left_pid, true);
      buffer_pool_manager_->UnpinPage(parent_pid, true);
      return;
    }

    left_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(left_pid, false);
  }

  // 再试向右兄弟借
  if (index < parent->GetSize() - 1) {
    page_id_t right_pid = parent->ValueAt(index + 1);
    Page *right_page = buffer_pool_manager_->FetchPage(right_pid);
    right_page->WLatch();
    auto *right = reinterpret_cast<N *>(right_page->GetData());

    if (right->GetSize() > right->GetMinSize()) {
      RedistributeNode(right, node, false);
      if (right->IsLeafPage()) {
        parent->SetKeyAt(index + 1, right->KeyAt(0));
      } else {
        parent->SetKeyAt(index + 1, right->KeyAt(0));
      }
      right_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(right_pid, true);
      buffer_pool_manager_->UnpinPage(parent_pid, true);
      return;
    }

    right_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(right_pid, false);
  }

  // 借不到则合并
  if (index > 0) {
    page_id_t left_pid = parent->ValueAt(index - 1);
    Page *left_page = buffer_pool_manager_->FetchPage(left_pid);
    left_page->WLatch();
    auto *left = reinterpret_cast<N *>(left_page->GetData());

    if (left->GetSize() == left->GetMinSize()) {
      CoalesceNode(left, node);
      parent->RemoveAt(index);
      if (transaction != nullptr) {
        transaction->AddIntoDeletedPageSet(node->GetPageId());
      }
    }

    left_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(left_pid, true);
  } else if (index < parent->GetSize() - 1) {
    page_id_t right_pid = parent->ValueAt(index + 1);
    Page *right_page = buffer_pool_manager_->FetchPage(right_pid);
    right_page->WLatch();
    auto *right = reinterpret_cast<N *>(right_page->GetData());

    if (right->GetSize() == right->GetMinSize()) {
      CoalesceNode(node, right);
      parent->RemoveAt(index + 1);
      if (transaction != nullptr) {
        transaction->AddIntoDeletedPageSet(right->GetPageId());
      }
    }

    right_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(right_pid, true);
  }

  if (parent->GetSize() < parent->GetMinSize()) {
    RedistributeOrMerge(parent, transaction);
  }
  buffer_pool_manager_->UnpinPage(parent_pid, true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::CoalesceLeafNode(LeafPage *neighbor_leaf, LeafPage *cur_leaf) {
  while (cur_leaf->GetSize() > 0) {
    cur_leaf->ShiftHeadItemToBack(neighbor_leaf);
  }
  neighbor_leaf->SetNextPageId(cur_leaf->GetNextPageId());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::CoalesceInternalNode(InternalPage *neighbor_internal, InternalPage *cur_internal) {
  while (cur_internal->GetSize() > 0) {
    cur_internal->RelocateHeadToBack(neighbor_internal, buffer_pool_manager_);
  }
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::CoalesceNode(N *neighbor_node, N *cur_node) {
  if (cur_node->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(cur_node);
    auto *neighbor_leaf = reinterpret_cast<LeafPage *>(neighbor_node);
    CoalesceLeafNode(neighbor_leaf, leaf);
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(cur_node);
    auto *neighbor_internal = reinterpret_cast<InternalPage *>(neighbor_node);
    CoalesceInternalNode(neighbor_internal, internal);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RedistributeLeafNode(LeafPage *neighbor_leaf, LeafPage *cur_leaf, bool from_prev) {
  if (from_prev) {
    neighbor_leaf->ShiftTailItemToFront(cur_leaf);
  } else {
    neighbor_leaf->ShiftHeadItemToBack(cur_leaf);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RedistributeInternalNode(InternalPage *neighbor_internal, InternalPage *cur_internal,
                                              bool from_prev) {
  if (from_prev) {
    neighbor_internal->RelocateTailToFront(cur_internal, buffer_pool_manager_);
  } else {
    neighbor_internal->RelocateHeadToBack(cur_internal, buffer_pool_manager_);
  }
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::RedistributeNode(N *neighbor_node, N *cur_node, bool from_prev) {
  if (cur_node->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(cur_node);
    auto *neighbor_leaf = reinterpret_cast<LeafPage *>(neighbor_node);
    RedistributeLeafNode(neighbor_leaf, leaf, from_prev);
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(cur_node);
    auto *neighbor_internal = reinterpret_cast<InternalPage *>(neighbor_node);
    RedistributeInternalNode(neighbor_internal, internal, from_prev);
  }
}

/*****************************************************************************
 * FIND LEAF
 *****************************************************************************/

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeaf(const KeyType &key, int latch_mode, Transaction *transaction) -> Page * {
  if (latch_mode != READ_MODE) {
    BUSTUB_ASSERT(transaction != nullptr, "Insert/Delete should carry a Transaction for latch tracking");
  }

  page_id_t page_id = root_page_id_;
  Page *page_ptr = nullptr;
  Page *prev_page_ptr = nullptr;
  auto *node = static_cast<BPlusTreePage *>(nullptr);

  while (true) {
    prev_page_ptr = page_ptr;
    page_ptr = buffer_pool_manager_->FetchPage(page_id);
    if (page_ptr == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "FindLeaf: FetchPage failed");
    }
    node = reinterpret_cast<BPlusTreePage *>(page_ptr->GetData());

    // 为当前结点加 latch（读/写）
    switch (latch_mode) {
      case READ_MODE: {
        // 读操作：允许多线程并发读取
        page_ptr->RLatch();
        if (transaction != nullptr) {
          ReleaseLockChain(transaction, READ_MODE);
          transaction->AddIntoPageSet(page_ptr);
        } else {
          if (prev_page_ptr != nullptr) {
            prev_page_ptr->RUnlatch();
            buffer_pool_manager_->UnpinPage(prev_page_ptr->GetPageId(), false);
          }
        }
        break;
      }

      case INSERT_MODE:
      case REMOVE_MODE: {
        page_ptr->WLatch();
        transaction->AddIntoPageSet(page_ptr);
        break;
      }

      default:
        break;
    }

    // 到叶子了，返回
    if (node->IsLeafPage()) {
      return page_ptr;
    }

    // 继续向下走 internal
    auto *internal = reinterpret_cast<InternalPage *>(node);
    page_id = internal->Lookup(key, comparator_);
  }
}

/*****************************************************************************
 * LOCK / UNLOCK
 *****************************************************************************/

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseSinglePage(Page *page_ptr, int latch_mode) {
  if (page_ptr == nullptr) {
    return;
  }
  switch (latch_mode) {
    case READ_MODE:
      page_ptr->RUnlatch();
      buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), false);
      break;
    case INSERT_MODE:
    case REMOVE_MODE:
      page_ptr->WUnlatch();
      buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), true);
      break;
    default:
      break;
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseLockChain(Transaction *transaction, int latch_mode) {
  if (transaction == nullptr) {
    return;
  }

  auto page_queue = transaction->GetPageSet();
  while (!page_queue->empty()) {
    Page *page_ptr = page_queue->front();
    page_queue->pop_front();

    if (page_ptr != nullptr) {
      // 统一通过一个 helper 释放 latch + unpin
      ReleaseSinglePage(page_ptr, latch_mode);
    } else {
      // nullptr 用来标记 tree_guard_
      switch (latch_mode) {
        case READ_MODE:
          tree_guard_.RUnlock();
          break;
        case INSERT_MODE:
        case REMOVE_MODE:
          tree_guard_.WUnlock();
          break;
        default:
          break;
      }
    }
  }
}

/*****************************************************************************
 * RECLAIM DELETED PAGES
 *****************************************************************************/

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeletePageHelper(page_id_t page_id) { buffer_pool_manager_->DeletePage(page_id); }

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReclaimDeletedPages(Transaction *transaction) {
  if (transaction == nullptr) {
    return;
  }
  auto deleted_pages = transaction->GetDeletedPageSet();
  for (const auto &pid : *deleted_pages) {
    DeletePageHelper(pid);
  }
  deleted_pages->clear();
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return End();
  }

  page_id_t pid = root_page_id_;
  Page *page = buffer_pool_manager_->FetchPage(pid);
  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());

  while (!node->IsLeafPage()) {
    auto *internal = reinterpret_cast<InternalPage *>(node);
    pid = internal->ValueAt(0);
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
    page = buffer_pool_manager_->FetchPage(pid);
    node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }

  auto *leaf = reinterpret_cast<LeafPage *>(node);
  auto iter = INDEXITERATOR_TYPE(leaf, 0, buffer_pool_manager_, nullptr);
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
  return iter;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return End();
  }

  page_id_t pid = root_page_id_;
  Page *page = buffer_pool_manager_->FetchPage(pid);
  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());

  while (!node->IsLeafPage()) {
    auto *internal = reinterpret_cast<InternalPage *>(node);
    pid = internal->Lookup(key, comparator_);
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
    page = buffer_pool_manager_->FetchPage(pid);
    node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }

  auto *leaf = reinterpret_cast<LeafPage *>(node);
  int index = leaf->KeyIndex(key, comparator_);
  auto iter = INDEXITERATOR_TYPE(leaf, index, buffer_pool_manager_, nullptr);
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
  return iter;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId()
              << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
