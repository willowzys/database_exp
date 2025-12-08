#include <cassert>
#include "storage/index/index_iterator.h"
#include "buffer/buffer_pool_manager.h"
#include "storage/page/page.h"

namespace bustub {

/*****************************************************
 * 默认构造：end() 迭代器
 *****************************************************/
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator()
    : leaf_(nullptr),
      page_(nullptr),
      index_(0),
      buffer_pool_manager_(nullptr),
      txn_(nullptr),
      locked_(false) {}

/*****************************************************
 * 带参构造：
 *****************************************************/
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(LeafPage *leaf, int index,
                                  BufferPoolManager *buffer_pool_manager,
                                  Transaction *txn)
    : leaf_(nullptr),
      page_(nullptr),
      index_(index),
      buffer_pool_manager_(buffer_pool_manager),
      txn_(txn),
      locked_(false) {
  if (leaf_ == nullptr && leaf != nullptr && buffer_pool_manager_ != nullptr) {
    // 通过 page_id 重新从缓冲池拿 page
    page_id_t pid = leaf->GetPageId();
    page_ = buffer_pool_manager_->FetchPage(pid);
    if (page_ != nullptr) {
      // 对叶子页加读锁
      page_->RLatch();
      locked_ = true;
      leaf_ = reinterpret_cast<LeafPage *>(page_->GetData());
    }
  }
}

/*****************************************************
 * 析构：如果当前持有锁，则释放 & unpin
 *****************************************************/
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (locked_ && page_ != nullptr && buffer_pool_manager_ != nullptr) {
    page_->RUnlatch();
    buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);
    locked_ = false;
    page_ = nullptr;
    leaf_ = nullptr;
  }
}

/*****************************************************
 * 判断是否结束
 *****************************************************/
INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  return leaf_ == nullptr;
}

/*****************************************************
 * 解引用操作
 *****************************************************/
INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  if (leaf_ == nullptr || index_ < 0 || index_ >= leaf_->GetSize()) {
    throw std::runtime_error("IndexIterator: dereference out of bound");
  }
  return leaf_->GetArray()[index_];
}

/*****************************************************
 * operator++()：
 *****************************************************/
INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (leaf_ == nullptr) {
    return *this;
  }

  index_++;
  if (index_ < leaf_->GetSize()) {
    return *this;
  }

  page_id_t next_pid = leaf_->GetNextPageId();

  // 先释放当前页的锁和 pin
  if (locked_ && page_ != nullptr && buffer_pool_manager_ != nullptr) {
    page_->RUnlatch();
    buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);
  }
  locked_ = false;
  page_ = nullptr;

  if (next_pid == INVALID_PAGE_ID) {
    // 没有下一页，变为 end()
    leaf_ = nullptr;
    index_ = 0;
    return *this;
  }

  // Fetch 下一页并加读锁
  if (buffer_pool_manager_ == nullptr) {
    leaf_ = nullptr;
    index_ = 0;
    return *this;
  }

  Page *next_page = buffer_pool_manager_->FetchPage(next_pid);
  if (next_page == nullptr) {
    // 失败时直接变成 end()，防止悬空
    leaf_ = nullptr;
    index_ = 0;
    return *this;
  }

  next_page->RLatch();
  locked_ = true;
  page_ = next_page;
  leaf_ = reinterpret_cast<LeafPage *>(next_page->GetData());
  index_ = 0;

  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;
template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;
template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
