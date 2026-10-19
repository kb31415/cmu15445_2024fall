//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.cpp
//
// Identification: src/execution/topn_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/topn_executor.h"

#include <algorithm>
#include <utility>

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), cmp_(plan->GetOrderBy()) {}

void TopNExecutor::Init() {
  if (child_executor_ == nullptr) {
    return;
  }
  child_executor_->Init();
  heap_.clear();
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
    SortEntry entry{GenerateSortKey(tuple, plan_->GetOrderBy(), schema), tuple};
    if (heap_.size() < limit) {
      heap_.push_back(std::move(entry));
      std::push_heap(heap_.begin(), heap_.end(), cmp_);
    } else if (cmp_(entry, heap_.front())) {
      std::pop_heap(heap_.begin(), heap_.end(), cmp_);
      heap_.back() = std::move(entry);
      std::push_heap(heap_.begin(), heap_.end(), cmp_);
    }
  }

  std::sort(heap_.begin(), heap_.end(), cmp_);
  sorted_tuples_.reserve(heap_.size());
  for (auto &entry : heap_) {
    sorted_tuples_.push_back(std::move(entry.second));
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cursor_ >= sorted_tuples_.size()) {
    return false;
  }
  *tuple = sorted_tuples_[cursor_++];
  *rid = tuple->GetRid();
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return heap_.size(); }

}  // namespace bustub
