//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);

  // Step 1: Check if there's any free frame or evictable frame
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    // Get a free frame from free list
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Evict(&frame_id)) {
    // Evict a page from the buffer pool
    Page *old_page = &pages_[frame_id];
    if (old_page->IsDirty()) {
      disk_manager_->WritePage(old_page->GetPageId(), old_page->GetData());
      old_page->is_dirty_ = false;
    }
    page_table_->Remove(old_page->GetPageId());
  } else {
    // No available frames
    *page_id = INVALID_PAGE_ID;
    return nullptr;
  }

  // Step 2: Allocate a new page id
  *page_id = AllocatePage();

  // Step 3: Reset the page metadata and data
  Page *new_page = &pages_[frame_id];
  new_page->ResetMemory();
  new_page->page_id_ = *page_id;
  new_page->pin_count_ = 1;
  new_page->is_dirty_ = false;

  // Step 4: Update page table and replacer
  page_table_->Insert(*page_id, frame_id);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return new_page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);

  // Step 1: Check if the page is already in the buffer pool
  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id)) {
    Page *page = &pages_[frame_id];
    page->pin_count_++;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return page;
  }

  // Step 2: If not, find a frame to hold the page
  frame_id_t new_frame_id;
  if (!free_list_.empty()) {
    new_frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Evict(&new_frame_id)) {
    Page *old_page = &pages_[new_frame_id];
    if (old_page->IsDirty()) {
      disk_manager_->WritePage(old_page->GetPageId(), old_page->GetData());
      old_page->is_dirty_ = false;
    }
    page_table_->Remove(old_page->GetPageId());
  } else {
    return nullptr;
  }

  // Step 3: Read the page from disk
  Page *new_page = &pages_[new_frame_id];
  new_page->ResetMemory();
  new_page->page_id_ = page_id;
  new_page->pin_count_ = 1;
  new_page->is_dirty_ = false;

  disk_manager_->ReadPage(page_id, new_page->GetData());

  // Step 4: Update page table and replacer
  page_table_->Insert(page_id, new_frame_id);
  replacer_->RecordAccess(new_frame_id);
  replacer_->SetEvictable(new_frame_id, false);

  return new_page;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  Page *page = &pages_[frame_id];
  if (page->GetPinCount() <= 0) {
    return false;
  }

  page->pin_count_--;
  if (is_dirty) {
    page->is_dirty_ = true;
  }

  if (page->GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  Page *page = &pages_[frame_id];
  disk_manager_->WritePage(page_id, page->GetData());
  page->is_dirty_ = false;

  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::lock_guard<std::mutex> lock(latch_);

  for (size_t i = 0; i < pool_size_; i++) {
    Page *page = &pages_[i];
    if (page->GetPageId() != INVALID_PAGE_ID && page->IsDirty()) {
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
      page->is_dirty_ = false;
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }

  Page *page = &pages_[frame_id];
  if (page->GetPinCount() > 0) {
    return false;
  }

  if (page->IsDirty()) {
    disk_manager_->WritePage(page_id, page->GetData());
  }

  page_table_->Remove(page_id);
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);

  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  page->is_dirty_ = false;

  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub