#pragma once

#include <limits>
#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/macros.h"

namespace bustub {

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 */
class LRUKReplacer {
 public:
  /**
   * @brief 内部结构体，存储帧的访问历史和可驱逐状态
   */
  struct Entry {
    std::list<size_t> history;  // 访问时间戳列表（最近的在末尾）
    bool evictable;             // 是否可驱逐
  };

  /**
   * @brief 构造函数
   * @param num_frames 最大帧数量
   * @param k LRU-K的k值
   */
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * @brief 析构函数
   */
  ~LRUKReplacer() = default;

  /**
   * @brief 驱逐最远k距离的帧
   * @param[out] frame_id 被驱逐的帧ID
   * @return 成功驱逐返回true，否则false
   */
  auto Evict(frame_id_t *frame_id) -> bool;

  /**
   * @brief 记录帧的访问（更新时间戳）
   * @param frame_id 被访问的帧ID
   */
  void RecordAccess(frame_id_t frame_id);

  /**
   * @brief 设置帧的可驱逐状态
   * @param frame_id 目标帧ID
   * @param set_evictable 是否可驱逐
   */
  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  /**
   * @brief 移除指定帧（必须是可驱逐的）
   * @param frame_id 目标帧ID
   */
  void Remove(frame_id_t frame_id);

  /**
   * @brief 返回可驱逐帧的数量
   * @return 可驱逐帧数量
   */
  auto Size() -> size_t;

 private:
  // 成员声明顺序需与构造函数初始化列表顺序一致
  size_t current_timestamp_{0};  // 当前时间戳（每次访问递增）
  size_t curr_size_{0};          // 可驱逐帧的数量
  size_t replacer_size_;         // 最大帧数量
  size_t k_;                     // LRU-K的k值
  std::mutex latch_;             // 互斥锁，保护共享数据
  std::unordered_map<frame_id_t, Entry> entries_;  // 帧ID到Entry的映射
  static constexpr frame_id_t INVALID_FRAME_ID = -1;  // 新增：无效帧ID定义
};

}  // namespace bustub
