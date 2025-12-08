#pragma once

#include <queue>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

class BufferPoolManager;

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>
#define INTERNAL_PAGE_HEADER_SIZE 24
#define INTERNAL_PAGE_SIZE ((BUSTUB_PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (sizeof(MappingType)))
/**
 * Store n indexed keys and n+1 child pointers (page_id) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 * NOTE: since the number of keys does not equal to number of child pointers,
 * the first key always remains invalid. That is to say, any search/lookup
 * should ignore the first key.
 *
 * Internal page format (keys are stored in increasing order):
 *  --------------------------------------------------------------------------
 * | HEADER | KEY(1)+PAGE_ID(1) | KEY(2)+PAGE_ID(2) | ... | KEY(n)+PAGE_ID(n) |
 *  --------------------------------------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeInternalPage : public BPlusTreePage {
 public:
  // must call initialize method after "create" a new node
  void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID, int max_size = INTERNAL_PAGE_SIZE);

  auto KeyAt(int index) const -> KeyType;
  void SetKeyAt(int index, const KeyType &key);
  auto ValueAt(int index) const -> ValueType;
  void SetValueAt(int index, const ValueType &value);

  auto KeyIndex(const KeyType &key, const KeyComparator &comp) const -> int;

  auto Lookup(const KeyType &key, const KeyComparator &comp) const -> ValueType;

  void InsertNodeAfter(const ValueType &old_value, const KeyType &new_key, const ValueType &new_value);

  void MoveHalfTo(BPlusTreeInternalPage *recipient, BufferPoolManager *bpm);

  void RemoveAt(int index);

  auto RemoveAndReturnOnlyChild() -> ValueType;

  void RelocateHeadToBack(BPlusTreeInternalPage *recipient, BufferPoolManager *bpm);

  void RelocateTailToFront(BPlusTreeInternalPage *recipient, BufferPoolManager *bpm);

  // Helper methods to manage the array
  auto GetArray() -> MappingType * { return array_; }
  auto GetArray() const -> const MappingType * { return array_; }

  /** 创建新的 root 节点：root = [val1] key2 [val2] */
  void BuildRoot(const KeyType &key1, page_id_t val1,
                 const KeyType &key2, page_id_t val2);

  /** 返回 child page_id 在 array_ 中的下标 */
  auto ValueIndex(page_id_t value) const -> int;

  /** 在内部节点按 key 插入 key-child */
  auto Insert(const KeyType &key, page_id_t value,
              const KeyComparator &comp) -> int;

 private:
  // Flexible array member for page data.
  MappingType array_[1];
};
}  // namespace bustub
