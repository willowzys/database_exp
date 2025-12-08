#pragma once

#include <utility>
#include <vector>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_LEAF_PAGE_TYPE BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>
#define LEAF_PAGE_HEADER_SIZE 28
#define LEAF_PAGE_SIZE ((BUSTUB_PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / sizeof(MappingType))

/**
 * Store indexed key and record id(record id = page id combined with slot id,
 * see include/common/rid.h for detailed implementation) together within leaf
 * page. Only support unique key.
 *
 * Leaf page format (keys are stored in order):
 *  ----------------------------------------------------------------------
 * | HEADER | KEY(1) + RID(1) | KEY(2) + RID(2) | ... | KEY(n) + RID(n)
 *  ----------------------------------------------------------------------
 *
 *  Header format (size in byte, 28 bytes in total):
 *  ---------------------------------------------------------------------
 * | PageType (4) | LSN (4) | CurrentSize (4) | MaxSize (4) |
 *  ---------------------------------------------------------------------
 *  -----------------------------------------------
 * | ParentPageId (4) | PageId (4) | NextPageId (4)
 *  -----------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeLeafPage : public BPlusTreePage {
 public:
  // After creating a new leaf page from buffer pool, must call initialize
  // method to set default values
  void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID, int max_size = LEAF_PAGE_SIZE);

  // helper methods
  auto GetNextPageId() const -> page_id_t;
  void SetNextPageId(page_id_t next_page_id);
  auto KeyAt(int index) const -> KeyType;
  auto ValueAt(int index) const -> ValueType;
  void SetValueAt(int index, const ValueType &value);

  /**
   * 在当前 leaf 内二分查找，返回「第一个 >= key 的下标」。
   */
  auto KeyIndex(const KeyType &key, const KeyComparator &comp) const -> int;

  /**
   * 在叶子中查找具体的 key，如果找到，填 *value 并返回 true，否则返回 false。
   */
  auto Lookup(const KeyType &key, ValueType *value, const KeyComparator &comp) const -> bool;

  /**
   * 在叶子内插入 key-value（保持有序）。返回插入后的 size。
   * 若检测到重复 key，则不插入，返回 -1。
   */
  auto Insert(const KeyType &key, const ValueType &value, const KeyComparator &comp) -> int;

  /**
   * 叶子分裂时，把右半部分移动到 recipient。
   * 同时维护好 next_page_id_ 链表。
   */
  void MoveHalfTo(BPlusTreeLeafPage *recipient);

  /**
   * 删除数组中第 index 个键值对。
   */
  void RemoveAt(int index);


  /**
   * 把当前节点的第一个元素搬到 recipient 的末尾。
   */
  void ShiftHeadItemToBack(BPlusTreeLeafPage *recipient);

  /**
   * 把当前节点的最后一个元素搬到 recipient 的开头。
   */
  void ShiftTailItemToFront(BPlusTreeLeafPage *recipient);

  // Helper methods to manage the array
  auto GetArray() -> MappingType * { return array_; }
  auto GetArray() const -> const MappingType * { return array_; }

 private:
  page_id_t next_page_id_;
  // Flexible array member for page data.
  MappingType array_[1];
};

}  // namespace bustub
