#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  next_page_id_ = INVALID_PAGE_ID;
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  KeyType key{};
  if (index >= 0 && index < GetSize()) {
    key = array_[index].first;
  }
  return key;
}

/*
 * Helper method to find and return the value associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  if (index >= 0 && index < GetSize()) {
    return array_[index].second;
  }
  return ValueType{};
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  if (index >= 0 && index < GetSize()) {
    array_[index].second = value;
  }
}


INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comp) const -> int {
  int left = 0;
  int right = GetSize() - 1;
  int ans = GetSize();  // 默认返回 size

  while (left <= right) {
    int mid = (left + right) / 2;
    // comparator(a, b) < 0 : a < b
    // comparator(a, b) == 0: a == b
    // comparator(a, b) > 0 : a > b
    if (comp(array_[mid].first, key) >= 0) {
      ans = mid;
      right = mid - 1;
    } else {
      left = mid + 1;
    }
  }
  return ans;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value,
                                        const KeyComparator &comp) const -> bool {
  int idx = KeyIndex(key, comp);
  if (idx < GetSize() && comp(array_[idx].first, key) == 0) {
    if (value != nullptr) {
      *value = array_[idx].second;
    }
    return true;
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value,
                                        const KeyComparator &comp) -> int {
  int size = GetSize();
  int idx = KeyIndex(key, comp);

  // 唯一键：如果 KeyIndex 找到的位置恰好是相等的 key，则不插入，返回 -1
  if (idx < size && comp(array_[idx].first, key) == 0) {
    return -1;
  }

  // 从后往前移动，给 idx 腾出一个位置
  for (int i = size; i > idx; i--) {
    array_[i] = array_[i - 1];
  }

  array_[idx] = MappingType(key, value);
  IncreaseSize(1);
  return GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
    int total = GetSize();
    int start = total / 2;
    int move_count = total - start;

    // recipient 必须最开始 size = 0
    for (int i = 0; i < move_count; i++) {
        recipient->array_[i] = array_[start + i];
    }

    recipient->SetSize(move_count);
    this->SetSize(start);

    recipient->SetNextPageId(this->GetNextPageId());
    this->SetNextPageId(recipient->GetPageId());
}



INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAt(int index) {
  int size = GetSize();
  if (index < 0 || index >= size) {
    return;
  }

  // 将 index 之后的元素整体前移一位
  for (int i = index + 1; i < size; i++) {
    array_[i - 1] = array_[i];
  }

  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::ShiftHeadItemToBack(BPlusTreeLeafPage *recipient) {
  int donor_size = GetSize();
  if (donor_size == 0) {
    return;
  }

  // 取出当前节点的第一个元素
  MappingType item = array_[0];

  // 本页元素整体前移一位
  for (int i = 1; i < donor_size; i++) {
    array_[i - 1] = array_[i];
  }
  IncreaseSize(-1);

  // 追加到 recipient 的末尾
  int recip_size = recipient->GetSize();
  recipient->array_[recip_size] = item;
  recipient->IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::ShiftTailItemToFront(BPlusTreeLeafPage *recipient) {
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
}



template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
