//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <algorithm>
#include <cstddef>
#include <limits>
#include <mutex>
#include <optional>
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief a new LRUKReplacer.
 * @param num_frames the maximum number of frames the LRUReplacer will be required to store
 */
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

//计算反向k距离，为evict辅助函数
auto LRUKReplacer::ComputeBackwardDistance(const LRUKNode &node) -> size_t {
  if (node.history_.size() < k_) {
    return std::numeric_limits<size_t>::max();
  }
  return current_timestamp_ - node.history_.front();
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
 * that are marked as 'evictable' are candidates for eviction.
 *
 * A frame with less than k historical references is given +inf as its backward k-distance.
 * If multiple frames have inf backward k-distance, then evict frame whose oldest timestamp
 * is furthest in the past.
 *
 * Successful eviction of a frame should decrement the size of replacer and remove the frame's
 * access history.
 *
 * @return the frame ID if a frame is successfully evicted, or `std::nullopt` if no frames can be evicted.
 */

//找出最大的K距离并且可淘汰的，多个为正无穷则淘汰最旧的，返回淘汰帧的帧id，没有则返回空
auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  std::lock_guard<std::mutex> lock(latch_);
  if (curr_size_ == 0) {
    return std::nullopt;
  }
  HistoryQueue history_queue;
  CacheQueue cache_queue;
  //遍历node_store_
  for (const auto &[fid, node] : node_store_) {
    if (!node.is_evictable_) {
      continue;
    }
    if (node.history_.size() < node.k_) {
      size_t earlist_time = node.history_.empty() ? std::numeric_limits<size_t>::max() : node.history_.front();
      history_queue.push({earlist_time, fid});
    } else {
      size_t distance_k = ComputeBackwardDistance(node);
      cache_queue.push({distance_k, fid});
    }
  }
  if (!history_queue.empty()) {
    frame_id_t candidate = history_queue.top().second;
    node_store_.erase(candidate);
    curr_size_--;
    return candidate;
  }
  if (!cache_queue.empty()) {
    frame_id_t candidate = cache_queue.top().second;
    node_store_.erase(candidate);
    curr_size_--;
    return candidate;
  }
  return std::nullopt;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Record the event that the given frame id is accessed at current timestamp.
 * Create a new entry for access history if frame id has not been seen before.
 *
 * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
 * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
 *
 * @param frame_id id of frame that received a new access.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
//把当前时间戳存到给定帧id的history_中，若无记录则需要创建新的历史条目
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);
  //验证帧id是否有效
  BUSTUB_ASSERT(static_cast<frame_id_t>(frame_id) >= 0 && static_cast<size_t>(frame_id) < replacer_size_,
                "Invalid frame_id");
  //节点不存在
  if (node_store_.find(frame_id) == node_store_.end()) {
    node_store_.emplace(frame_id, LRUKNode(k_));
  }
  auto &node = node_store_[frame_id];
  node.history_.push_back(current_timestamp_);
  if (node.history_.size() > k_) {
    node.history_.pop_front();
  }
  current_timestamp_++;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
//切换帧的可逐出状态并且改变curr_size
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  //验证帧id是否有效
  BUSTUB_ASSERT(static_cast<frame_id_t>(frame_id) >= 0 && static_cast<size_t>(frame_id) < replacer_size_,
                "Invalid frame_id");
  //验证帧id是否在node_store里
  if (node_store_.find(frame_id) == node_store_.end()) {
    return;
  }
  auto &node = node_store_[frame_id];
  bool old_evictable = node.is_evictable_;
  if (old_evictable && (!set_evictable)) {
    node.is_evictable_ = set_evictable;
    curr_size_--;
  } else if ((!old_evictable) && set_evictable) {
    node.is_evictable_ = set_evictable;
    curr_size_++;
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Remove an evictable frame from replacer, along with its access history.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * with largest backward k-distance. This function removes specified frame id,
 * no matter what its backward k-distance is.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
//直接删除该帧，从node_store中删除，避免干扰
void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  BUSTUB_ASSERT(static_cast<frame_id_t>(frame_id) >= 0 && static_cast<size_t>(frame_id) < replacer_size_,
                "Invalid frame_id");
  if (node_store_.find(frame_id) == node_store_.end()) {
    return;
  }
  auto &node = node_store_[frame_id];
  BUSTUB_ASSERT(node.is_evictable_, "Invalid frame_id");
  node_store_.erase(frame_id);
  curr_size_--;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
