//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <cstddef>
#include <limits>
#include <list>
#include <mutex>  // NOLINT
#include <optional>
#include <queue>
#include <unordered_map>
#include <utility>
#include <vector>

//#include "buffer/arc_replacer.h"
#include "common/config.h"
#include "common/macros.h"

namespace bustub {
//帧的管理
enum class AccessType { Unknown = 0, Lookup, Scan, Index };

class LRUKNode {
 private:
  /** History of last seen K timestamps of this page. Least recent timestamp stored in front. */
  // Remove maybe_unused if you start using them. Feel free to change the member variables as you want.

  std::list<size_t> history_;  //访问历史，最近k次访问的时间戳，队头最老
  size_t k_;                   // k
  bool is_evictable_{false};  //能不能被剔除
  friend class LRUKReplacer;

 public:
  explicit LRUKNode(size_t k) : k_(k) {}
  LRUKNode() = default;
};

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
//替换器
class LRUKReplacer {
 public:
  //缓冲池的帧数和k构造函数
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() = default;
  //找出最大的K距离并且可淘汰的，多个为正无穷则淘汰最旧的，返回淘汰帧的帧id，没有则返回空
  auto Evict() -> std::optional<frame_id_t>;
  //把当前时间戳存到给定帧id的history_中，若无记录则需要创建新的历史条目
  void RecordAccess(frame_id_t frame_id, AccessType access_type = AccessType::Unknown);
  //切换帧的可逐出状态并且改变curr_size
  void SetEvictable(frame_id_t frame_id, bool set_evictable);
  //直接删除该帧，从node_store中删除，避免干扰
  void Remove(frame_id_t frame_id);
  //返回可逐出帧的数量
  auto Size() -> size_t;

 private:
  // TODO(student): implement me! You can replace these member variables as you like.
  // Remove maybe_unused if you start using them.
  std::unordered_map<frame_id_t, LRUKNode> node_store_;  //帧id到LRUNode的映射，相当于页表
  size_t current_timestamp_{0};                          //时间戳
  std::atomic<size_t> curr_size_{0};                     //可被淘汰帧的数量
  size_t replacer_size_;                                 //缓冲池总帧数
  size_t k_;                                             // k值
  std::mutex latch_;                                     //锁
  struct HistoryCmp {
    auto operator()(const std::pair<size_t, frame_id_t> &a, const std::pair<size_t, frame_id_t> &b) const -> bool {
      return a.first > b.first;
    }
  };
  struct CacheCmp {
    auto operator()(const std::pair<size_t, frame_id_t> &a, const std::pair<size_t, frame_id_t> &b) const -> bool {
      return a.first < b.first;
    }
  };
  //用于无限距离帧，top是最早时间
  using HistoryQueue =
      std::priority_queue<std::pair<size_t, frame_id_t>, std::vector<std::pair<size_t, frame_id_t>>, HistoryCmp>;
  //用于有限距离帧，top是最大距离
  using CacheQueue =
      std::priority_queue<std::pair<size_t, frame_id_t>, std::vector<std::pair<size_t, frame_id_t>>, CacheCmp>;

  //计算反向k距离，为evict辅助函数
  auto ComputeBackwardDistance(const LRUKNode &node) -> size_t;
};

}  // namespace bustub
