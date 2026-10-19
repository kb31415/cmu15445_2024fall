//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "common/macros.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Construct a new HashJoinExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The HashJoin join plan to be executed
 * @param left_child The child executor that produces tuples for the left side of join
 * @param right_child The child executor that produces tuples for the right side of join
 */
HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for Spring 2025: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

/** Initialize the join */
void HashJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();

  current_bucket_ = nullptr;
  hash_table_.clear();
  left_tuples_.clear();

  Tuple left_tuple{};
  RID left_rid{};
  while (left_executor_->Next(&left_tuple, &left_rid)) {
    std::vector<Value> left_key_values;
    left_key_values.reserve(plan_->LeftJoinKeyExpressions().size());
    for (const auto &expr : plan_->LeftJoinKeyExpressions()) {
      left_key_values.emplace_back(expr->Evaluate(&left_tuple, left_executor_->GetOutputSchema()));
    }
    auto idx = static_cast<uint32_t>(left_tuples_.size());
    left_tuples_.emplace_back(left_tuple);
    hash_table_[HashJoinKey{std::move(left_key_values)}].emplace_back(idx);
  }

  current_bucket_ = nullptr;
  current_match_idx_ = 0;
  right_tuple_available_ = false;
  current_left_idx_ = 0;
  if (plan_->GetJoinType() == JoinType::LEFT) {
    left_matched_flags_.assign(left_tuples_.size(), false);
  } else {
    left_matched_flags_.clear();
  }
}

/**
 * Yield the next tuple from the join.
 * @param[out] tuple The next tuple produced by the join.
 * @param[out] rid The next tuple RID, not used by hash join.
 * @return `true` if a tuple was produced, `false` if there are no more tuples.
 */
auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  const bool is_left = plan_->GetJoinType() == JoinType::LEFT;
  while (true) {
    if (current_bucket_ != nullptr && current_match_idx_ < current_bucket_->size()) {
      auto left_idx = (*current_bucket_)[current_match_idx_++];
      if (is_left) {
        left_matched_flags_[left_idx] = true;
      }
      const auto &left_tuple = left_tuples_[left_idx];
      std::vector<Value> values;
      values.reserve(GetOutputSchema().GetColumnCount());
      for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(left_tuple.GetValue(&left_executor_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(current_right_tuple_.GetValue(&right_executor_->GetOutputSchema(), i));
      }
      *tuple = Tuple{values, &GetOutputSchema()};
      *rid = RID{};
      return true;
    }

    if (!right_tuple_available_) {
      right_tuple_available_ = right_executor_->Next(&current_right_tuple_, &current_right_rid_);
    }
    if (!right_tuple_available_) {
      break;
    }

    std::vector<Value> right_key_values;
    right_key_values.reserve(plan_->RightJoinKeyExpressions().size());
    for (const auto &expr : plan_->RightJoinKeyExpressions()) {
      right_key_values.emplace_back(expr->Evaluate(&current_right_tuple_, right_executor_->GetOutputSchema()));
    }
    auto it = hash_table_.find(HashJoinKey{std::move(right_key_values)});
    if (it != hash_table_.end()) {
      current_bucket_ = &it->second;
      current_match_idx_ = 0;
    } else {
      current_bucket_ = nullptr;
      current_match_idx_ = 0;
    }
    right_tuple_available_ = false;
  }

  if (!is_left) {
    return false;
  }
  while (current_left_idx_ < left_tuples_.size()) {
    auto idx = current_left_idx_++;
    if (left_matched_flags_[idx]) {
      continue;
    }
    std::vector<Value> values;
    values.reserve(GetOutputSchema().GetColumnCount());
    for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
      values.emplace_back(left_tuples_[idx].GetValue(&left_executor_->GetOutputSchema(), i));
    }
    for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
      auto &column = right_executor_->GetOutputSchema().GetColumn(i);
      values.emplace_back(ValueFactory::GetNullValueByType(column.GetType()));
    }
    *tuple = Tuple{values, &GetOutputSchema()};
    *rid = RID{};
    return true;
  }
  return false;
}

}  // namespace bustub
