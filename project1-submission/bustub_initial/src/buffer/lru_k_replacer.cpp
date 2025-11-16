#include "buffer/lru_k_replacer.h"
#include "common/exception.h"
#include "common/macros.h"
#include <algorithm>
#include <cstddef>
#include <mutex>

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) 
    : replacer_size_(num_frames), k_(k) {
  // 初始化顺序与头文件成员声明顺序一致
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);  // 加锁保护

  if (curr_size_ == 0) {
    return false;  // 无可驱逐帧
  }

  frame_id_t victim = INVALID_FRAME_ID;  // 使用类内定义的无效帧ID
  int64_t max_distance = -1;
  size_t earliest_timestamp = std::numeric_limits<size_t>::max();

  // 遍历所有可驱逐帧，寻找最优驱逐目标
  for (const auto &[fid, entry] : entries_) {
    if (!entry.evictable) {
      continue;  // 跳过不可驱逐帧
    }

    // 计算k距离：不足k次访问则为+inf，否则为当前时间戳 - 第k次访问时间戳
    int64_t distance;
    if (entry.history.size() < k_) {
      distance = std::numeric_limits<int64_t>::max();  // +inf
    } else {
      auto it = entry.history.begin();
      std::advance(it, entry.history.size() - k_);  // 第k次访问的迭代器
      distance = static_cast<int64_t>(current_timestamp_ - *it);
    }

    // 优先选择k距离最大的帧
    if (distance > max_distance) {
      max_distance = distance;
      victim = fid;
      earliest_timestamp = entry.history.front();  // 记录最早访问时间（用于平局）
    } else if (distance == max_distance) {
      // k距离相同则选择最早访问的帧（LRU逻辑）
      if (entry.history.front() < earliest_timestamp) {
        victim = fid;
        earliest_timestamp = entry.history.front();
      }
    }
  }

  if (victim == INVALID_FRAME_ID) {
    return false;  // 未找到可驱逐帧（理论上不会发生）
  }

  // 执行驱逐：移除帧信息，更新可驱逐数量
  *frame_id = victim;
  entries_.erase(victim);
  curr_size_--;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);  // 加锁保护

  // 检查帧ID有效性
  BUSTUB_ASSERT(frame_id < static_cast<frame_id_t>(replacer_size_), "Invalid frame id");

  current_timestamp_++;  // 时间戳递增

  if (entries_.find(frame_id) == entries_.end()) {
    // 新帧：初始化访问历史
    entries_[frame_id] = {{current_timestamp_}, false};  // 默认为不可驱逐
  } else {
    // 已有帧：添加新时间戳，保持历史长度不超过k（超过则删除最早的）
    auto &history = entries_[frame_id].history;
    history.push_back(current_timestamp_);
    if (history.size() > k_) {
      history.pop_front();
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);  // 加锁保护

  // 检查帧ID有效性
  BUSTUB_ASSERT(frame_id < static_cast<frame_id_t>(replacer_size_), "Invalid frame id");

  auto it = entries_.find(frame_id);
  if (it == entries_.end()) {
    return;  // 帧不存在，不操作
  }

  auto &entry = it->second;
  if (entry.evictable == set_evictable) {
    return;  // 状态未变，不操作
  }

  // 更新可驱逐状态和数量
  entry.evictable = set_evictable;
  if (set_evictable) {
    curr_size_++;
  } else {
    curr_size_--;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);  // 加锁保护

  // 检查帧ID有效性
  BUSTUB_ASSERT(frame_id < static_cast<frame_id_t>(replacer_size_), "Invalid frame id");

  auto it = entries_.find(frame_id);
  if (it == entries_.end()) {
    return;  // 帧不存在，直接返回
  }

  // 检查是否可驱逐
  BUSTUB_ASSERT(it->second.evictable, "Cannot remove non-evictable frame");

  // 移除帧并更新数量
  entries_.erase(it);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);  // 加锁保护
  return curr_size_;
}

}  // namespace bustub
