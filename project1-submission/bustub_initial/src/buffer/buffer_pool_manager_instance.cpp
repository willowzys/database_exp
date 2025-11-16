//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager_instance.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every frame is in the free list.
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

  frame_id_t frame_id;

  // First, try to get a frame from the free list
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    // If no free frames available, try to evict a page
    if (!replacer_->Evict(&frame_id)) {
      // No evictable frame found
      return nullptr;
    }

    // Evict the page from the frame
    Page *evict_page = &pages_[frame_id];
    
    // If the page is dirty, write it back to disk
    if (evict_page->IsDirty()) {
      disk_manager_->WritePage(evict_page->GetPageId(), evict_page->GetData());
      evict_page->is_dirty_ = false;
    }

    // Remove the page from page table
    page_table_->Remove(evict_page->GetPageId());
    
    // Reset the page metadata
    evict_page->ResetMemory();
    evict_page->page_id_ = INVALID_PAGE_ID;
    evict_page->pin_count_ = 0;
  }

  // Allocate a new page id
  *page_id = AllocatePage();

  // Set up the new page
  Page *new_page = &pages_[frame_id];
  new_page->page_id_ = *page_id;
  new_page->pin_count_ = 1;
  new_page->is_dirty_ = false;

  // Add to page table
  page_table_->Insert(*page_id, frame_id);

  // Update replacer
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return new_page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);

  frame_id_t frame_id;

  // Check if page is already in buffer pool
  if (page_table_->Find(page_id, frame_id)) {
    Page *page = &pages_[frame_id];
    page->pin_count_++;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return page;
  }

  // Page not in buffer pool, need to load from disk
  // First, get a frame
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }

    // Evict the current page in this frame
    Page *evict_page = &pages_[frame_id];
    if (evict_page->IsDirty()) {
      disk_manager_->WritePage(evict_page->GetPageId(), evict_page->GetData());
      evict_page->is_dirty_ = false;
    }
    page_table_->Remove(evict_page->GetPageId());
    
    // Reset the evicted page
    evict_page->ResetMemory();
    evict_page->page_id_ = INVALID_PAGE_ID;
    evict_page->pin_count_ = 0;
  }

  // Read the page from disk
  Page *page = &pages_[frame_id];
  disk_manager_->ReadPage(page_id, page->GetData());

  // Set up page metadata
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;

  // Add to page table and update replacer
  page_table_->Insert(page_id, frame_id);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return page;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  Page *page = &pages_[frame_id];
  
  if (page->pin_count_ <= 0) {
    return false;
  }

  page->pin_count_--;

  // Update dirty flag - only set to dirty, never clean a dirty page
  if (is_dirty) {
    page->is_dirty_ = true;
  }

  if (page->pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  if (page_id == INVALID_PAGE_ID) {
    return false;
  }

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

  for (size_t i = 0; i < pool_size_; ++i) {
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

  if (page->pin_count_ > 0) {
    return false;
  }

  // If the page is dirty, write it back to disk
  if (page->IsDirty()) {
    disk_manager_->WritePage(page_id, page->GetData());
  }

  // Remove from page table
  page_table_->Remove(page_id);
  
  // Remove from replacer
  replacer_->Remove(frame_id);

  // Reset page and add frame to free list
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  
  free_list_.push_back(frame_id);

  // Deallocate page on disk
  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t {
  return next_page_id_++;
}

}  // namespace bustub
