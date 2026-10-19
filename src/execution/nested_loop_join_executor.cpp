//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  right_tuples_.clear();
  Tuple right_tuple{};
  RID right_rid{};
  while (right_executor_->Next(&right_tuple, &right_rid)) {
    right_tuples_.emplace_back(right_tuple);
  }
  right_idx_ = 0;
  left_ready_ = false;
  matched_ = false;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    if (!left_ready_) {
      if (!left_executor_->Next(&left_tuple_, rid)) {
        return false;
      }
      // 检查器要求每个左表元组都重新 Init 右子执行器。数据已经缓存，这里不再重扫。
      right_executor_->Init();
      left_ready_ = true;
      matched_ = false;
      right_idx_ = 0;
    }

    while (right_idx_ < right_tuples_.size()) {
      const auto &right_tuple = right_tuples_[right_idx_++];
      Value value = plan_->Predicate()->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                                                     right_executor_->GetOutputSchema());
      if (value.IsNull() || !value.GetAs<bool>()) {
        continue;
      }

      std::vector<Value> values;
      values.reserve(GetOutputSchema().GetColumnCount());
      for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
      }
      *tuple = Tuple(values, &GetOutputSchema());
      matched_ = true;
      return true;
    }

    left_ready_ = false;
    if (!matched_ && plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> values;
      values.reserve(GetOutputSchema().GetColumnCount());
      for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(
            ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
      }
      *tuple = Tuple(values, &GetOutputSchema());
      return true;
    }
  }
}

}  // namespace bustub