//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lock(mu_);
  if (free_list_.empty() && replacer_->Size() == 0) {
    *page_id = INVALID;
    return nullptr;
  }

  auto frame_id = GetFreeFrame();

  *page_id = AllocatePage();
  auto page = InitNewPage(*page_id, frame_id);

  Pinpage(*page_id);
  return page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lock(mu_);
  if (page_table_.find(page_id) != page_table_.end()) {  // 目标page在buffer pool中
    Pinpage(page_id);
    return pages_ + page_table_[page_id];
  }
  if (free_list_.empty() && replacer_->Size() == 0) {
    return nullptr;
  }
  auto frame_id = GetFreeFrame();  // 在buffer pool中构建新的空闲page
  auto page = InitNewPage(page_id, frame_id);
  Pinpage(page_id);
  // 把page_id在磁盘中的数据读到buffer pool新空出来的page里
  disk_manager_->ReadPage(page_id, page->GetData());
  return page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lock(mu_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  auto frame_id = page_table_[page_id];
  auto page = pages_ + frame_id;
  if (page->GetPinCount() == 0) {
    return false;
  }
  if (--page->pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  page->is_dirty_ |= is_dirty;
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  // 不能加锁，因为flushPage是在驱逐dirty page前调用的，本身就已持有锁
  if (page_id == INVALID_PAGE_ID) {
    throw "invalid page ID";
  }
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  auto page = pages_ + page_table_[page_id];
  disk_manager_->WritePage(page_id, page->GetData());
  page->is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> lock(mu_);
  for (auto [page_id, frame_id] : page_table_) {
    auto page = pages_ + frame_id;
    disk_manager_->WritePage(page_id, page->GetData());
    page->is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(mu_);

  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }

  auto frame_id = page_table_[page_id];
  auto page = pages_ + frame_id;
  if (page->GetPinCount() > 0) {
    return false;
  }

  page_table_.erase(page_id);
  replacer_->Remove(frame_id);
  free_list_.emplace_back(frame_id);

  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  page->is_dirty_ = false;

  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto page = FetchPage(page_id);
  if (page == nullptr) {
    return ReadPageGuard{this, nullptr};
  }
  page->RLatch();
  return ReadPageGuard{this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto page = FetchPage(page_id);
  if (page == nullptr) {
    return WritePageGuard{this, nullptr};
  }
  page->WLatch();
  return WritePageGuard{this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  return BasicPageGuard{this, NewPage(page_id)};
}
}  // namespace bustub
