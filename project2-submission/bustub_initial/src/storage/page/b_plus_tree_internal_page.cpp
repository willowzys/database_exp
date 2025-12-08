#include <iostream>
#include <sstream>

#include "buffer/buffer_pool_manager.h"
#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
}

/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  KeyType key{};
  if (index >= 0 && index < GetSize()) {
    key = array_[index].first;
  }
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  array_[index].first = key;
}

/*
 * Helper method to get/set the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  if (index >= 0 && index < GetSize()) {
    return array_[index].second;
  }
  return INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  array_[index].second = value;
}



INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comp) const -> int {
  // 只在 [1, size-1] 范围内做二分，因为 0 号 key 无效
  int size = GetSize();
  if (size <= 1) {
    return size;
  }

  int left = 1;
  int right = size - 1;
  int ans = size;  // 默认：所有 key 都 < 给定 key，则返回 size 作为“插入到末尾”的位置

  while (left <= right) {
    int mid = (left + right) / 2;
    if (comp(KeyAt(mid), key) >= 0) {
      ans = mid;
      right = mid - 1;
    } else {
      left = mid + 1;
    }
  }
  return ans;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comp) const -> ValueType {
  int size = GetSize();
  // 内部节点至少要有一个 child（size >= 1）
  BUSTUB_ASSERT(size >= 1, "internal page must have at least one child");

  if (size == 1) {
    return ValueAt(0);
  }

  // 若 key < KeyAt(1) → 返回 ValueAt(0)
  if (comp(key, KeyAt(1)) < 0) {
    return ValueAt(0);
  }

  int left = 1;
  int right = size - 1;
  int upper = size;  // 默认：所有 key 都 <= key，则 child index 为 size-1

  while (left <= right) {
    int mid = (left + right) / 2;
    if (comp(KeyAt(mid), key) > 0) {
      upper = mid;
      right = mid - 1;
    } else {
      left = mid + 1;
    }
  }

  int child_index = upper - 1;
  return ValueAt(child_index);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  int size = GetSize();
  int idx = -1;

  // 找到 old_value 所在的 child 下标
  for (int i = 0; i < size; i++) {
    if (ValueAt(i) == old_value) {
      idx = i;
      break;
    }
  }

  BUSTUB_ASSERT(idx != -1, "old_value not found in internal page");

  int insert_pos = idx + 1;

  // 从尾部开始整体向右挪一格，为 insert_pos 腾位置
  for (int i = size; i > insert_pos; i--) {
    array_[i] = array_[i - 1];
  }

  array_[insert_pos].first = new_key;
  array_[insert_pos].second = new_value;

  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient, BufferPoolManager *bpm) {
  int total = GetSize();
  BUSTUB_ASSERT(total > 1, "cannot split an internal page with size <= 1");

  // 从 mid 开始，把右半部分搬到 recipient
  int mid = total / 2;
  int move_count = total - mid;

  int dest_start = recipient->GetSize(); 

  for (int i = 0; i < move_count; i++) {
    // 拷贝条目
    recipient->array_[dest_start + i] = array_[mid + i];

    // 更新 child 的 parent_page_id
    page_id_t child_pid = recipient->array_[dest_start + i].second;
    Page *child_page = bpm->FetchPage(child_pid);
    BUSTUB_ASSERT(child_page != nullptr, "FetchPage for child failed");
    auto *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    child_node->SetParentPageId(recipient->GetPageId());
    bpm->UnpinPage(child_pid, true);
  }

  recipient->SetSize(dest_start + move_count);
  SetSize(mid);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::RelocateHeadToBack(BPlusTreeInternalPage *recipient,
                                                           BufferPoolManager *bpm) {
  int donor_size = GetSize();
  if (donor_size == 0) {
    return;
  }

  // 取出当前节点的第一个元素
  MappingType item = array_[0];

  // 本页整体前移一位
  for (int i = 1; i < donor_size; i++) {
    array_[i - 1] = array_[i];
  }
  IncreaseSize(-1);

  // 追加到 recipient 末尾
  int recip_size = recipient->GetSize();
  recipient->array_[recip_size] = item;
  recipient->IncreaseSize(1);

  // 更新被移动 child 页面的 parent_page_id
  page_id_t child_pid = item.second;
  Page *child_page = bpm->FetchPage(child_pid);
  BUSTUB_ASSERT(child_page != nullptr, "FetchPage for child failed");
  auto *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
  child_node->SetParentPageId(recipient->GetPageId());
  bpm->UnpinPage(child_pid, true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::RelocateTailToFront(BPlusTreeInternalPage *recipient,
                                                           BufferPoolManager *bpm) {
  int donor_size = GetSize();
  if (donor_size == 0) {
    return;
  }

  // 取出当前节点的最后一个元素
  MappingType item = array_[donor_size - 1];
  IncreaseSize(-1);

  // recipient 整体后移一位，为插入到开头腾出空间
  int recip_size = recipient->GetSize();
  for (int i = recip_size; i > 0; i--) {
    recipient->array_[i] = recipient->array_[i - 1];
  }
  recipient->array_[0] = item;
  recipient->IncreaseSize(1);

  // 更新被移动 child 页面的 parent_page_id
  page_id_t child_pid = item.second;
  Page *child_page = bpm->FetchPage(child_pid);
  BUSTUB_ASSERT(child_page != nullptr, "FetchPage for child failed");
  auto *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
  child_node->SetParentPageId(recipient->GetPageId());
  bpm->UnpinPage(child_pid, true);
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAt(int index) {
  int size = GetSize();
  if (index < 0 || index >= size) {
    return;
  }

  // index 之后的整体向前挪一格
  for (int i = index + 1; i < size; i++) {
    array_[i - 1] = array_[i];
  }

  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() -> ValueType {
  BUSTUB_ASSERT(GetSize() == 1, "RemoveAndReturnOnlyChild requires size == 1");
  ValueType child = ValueAt(0);
  SetSize(0);
  return child;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::BuildRoot(const KeyType &key1, page_id_t val1,
                                               const KeyType &key2, page_id_t val2) {
  // Build root with two children: val1 -(key2)-> val2
  array_[0].second = val1;
  array_[1].first = key2;
  array_[1].second = val2;

  SetSize(2);
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(page_id_t value) const -> int {
  int size = GetSize();
  for (int i = 0; i < size; i++) {
    if (array_[i].second == value) {
      return i;
    }
  }
  return -1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, page_id_t value,
                                            const KeyComparator &comp) -> int {
  int size = GetSize();

  // 找到第一个 > key 的位置（内部节点 key 从下标 1 开始）
  int idx = 1;
  while (idx < size && comp(array_[idx].first, key) < 0) {
    idx++;
  }

  // 向右移动，为 idx 腾位置
  for (int i = size; i > idx; i--) {
    array_[i] = array_[i - 1];
  }

  array_[idx].first = key;
  array_[idx].second = value;

  IncreaseSize(1);
  return GetSize();
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;

}  // namespace bustub
