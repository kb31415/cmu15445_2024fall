//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

struct HashJoinKey {
  std::vector<Value> keys_;

  explicit HashJoinKey(std::vector<Value> key) : keys_(std::move(key)) {}

  auto operator==(const HashJoinKey &other) const -> bool {
    if (keys_.size() != other.keys_.size()) {
      return false;
    }
    for (size_t i = 0; i < keys_.size(); ++i) {
      if (keys_[i].CompareEquals(other.keys_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};
}  // namespace bustub

namespace std {
template <>
struct hash<bustub::HashJoinKey> {
  auto operator()(const bustub::HashJoinKey &key) const -> size_t {
    size_t hash_val = 0;
    for (const auto &val : key.keys_) {
      hash_val = bustub::HashUtil::CombineHashes(hash_val, bustub::HashUtil::HashValue(&val));
    }
    return hash_val;
  }
};
}  // namespace std

namespace bustub {
/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;

  /** keft child executor */
  std::unique_ptr<AbstractExecutor> left_executor_;

  /** right child executor */
  std::unique_ptr<AbstractExecutor> right_executor_;

  /** 左表元组下标，按连接键分组。 */
  std::unordered_map<HashJoinKey, std::vector<uint32_t>> hash_table_;
  std::vector<Tuple> left_tuples_;
  const std::vector<uint32_t> *current_bucket_{nullptr};
  size_t current_match_idx_{0};

  Tuple current_right_tuple_{};
  RID current_right_rid_{};
  bool right_tuple_available_{false};

  std::vector<bool> left_matched_flags_;
  size_t current_left_idx_{0};
};

}  // namespace bustub
