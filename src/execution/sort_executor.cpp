//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// sort_executor.cpp
//
// Identification: src/execution/sort_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/sort_executor.h"

#include <algorithm>
#include <utility>
#include <vector>

#include "execution/execution_common.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  sorted_tuples_.clear();
  cursor_ = 0;

  std::vector<SortEntry> entries;
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    entries.push_back({GenerateSortKey(tuple, plan_->GetOrderBy(), child_executor_->GetOutputSchema()), tuple});
  }
  std::sort(entries.begin(), entries.end(), TupleComparator(plan_->GetOrderBy()));
  sorted_tuples_.reserve(entries.size());
  for (auto &entry : entries) {
    sorted_tuples_.push_back(std::move(entry.second));
  }
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cursor_ >= sorted_tuples_.size()) {
    return false;
  }
  *tuple = sorted_tuples_[cursor_++];
  *rid = tuple->GetRid();
  return true;
}

}  // namespace bustub
