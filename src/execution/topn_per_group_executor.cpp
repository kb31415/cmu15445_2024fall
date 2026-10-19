//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_per_group_executor.cpp
//
// Identification: src/execution/topn_per_group_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/topn_per_group_executor.h"

#include <algorithm>
#include <utility>

namespace bustub {

TopNPerGroupExecutor::TopNPerGroupExecutor(ExecutorContext *exec_ctx, const TopNPerGroupPlanNode *plan,
                                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), cmp_(plan->GetOrderBy()) {}

void TopNPerGroupExecutor::Init() {
  child_executor_->Init();
  heaps_.clear();
  sorted_tuples_.clear();
  cursor_ = 0;

  const size_t limit = plan_->GetN();
  Tuple tuple;
  RID rid;
  const auto &schema = child_executor_->GetOutputSchema();
  while (child_executor_->Next(&tuple, &rid)) {
    if (limit == 0) {
      continue;
    }
    AggregateKey key;
    key.group_bys_.reserve(plan_->GetGroupBy().size());
    for (const auto &expr : plan_->GetGroupBy()) {
      key.group_bys_.push_back(expr->Evaluate(&tuple, schema));
    }
    auto &heap = heaps_[key];
    SortEntry entry{GenerateSortKey(tuple, plan_->GetOrderBy(), schema), tuple};
    if (heap.size() < limit) {
      heap.push_back(std::move(entry));
      std::push_heap(heap.begin(), heap.end(), cmp_);
    } else if (cmp_(entry, heap.front())) {
      std::pop_heap(heap.begin(), heap.end(), cmp_);
      heap.back() = std::move(entry);
      std::push_heap(heap.begin(), heap.end(), cmp_);
    }
  }

  for (auto &[key, heap] : heaps_) {
    (void)key;
    std::sort(heap.begin(), heap.end(), cmp_);
    for (auto &entry : heap) {
      sorted_tuples_.push_back(std::move(entry.second));
    }
  }
}

auto TopNPerGroupExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cursor_ >= sorted_tuples_.size()) {
    return false;
  }
  *tuple = sorted_tuples_[cursor_++];
  *rid = tuple->GetRid();
  return true;
}

}  // namespace bustub
