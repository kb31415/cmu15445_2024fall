//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// window_function_executor.cpp
//
// Identification: src/execution/window_function_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/window_function_executor.h"

#include <algorithm>
#include <numeric>
#include <utility>
#include <vector>

#include "execution/execution_common.h"
#include "type/value_factory.h"

namespace bustub {

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

auto WindowFunctionExecutor::MakePartitionKey(const Tuple &tuple, const std::vector<AbstractExpressionRef> &exprs) const
    -> AggregateKey {
  AggregateKey key;
  key.group_bys_.reserve(exprs.size());
  for (const auto &expr : exprs) {
    key.group_bys_.push_back(expr->Evaluate(&tuple, child_executor_->GetOutputSchema()));
  }
  return key;
}

namespace {

auto KeysEqual(const SortKey &a, const SortKey &b) -> bool {
  if (a.size() != b.size()) {
    return false;
  }
  for (size_t i = 0; i < a.size(); i++) {
    if (a[i].CompareEquals(b[i]) != CmpBool::CmpTrue) {
      return false;
    }
  }
  return true;
}

auto ComesBefore(const SortKey &a, const SortKey &b, const std::vector<OrderBy> &order_bys) -> bool {
  for (size_t i = 0; i < order_bys.size(); i++) {
    const auto order = order_bys[i].first;
    if (a[i].CompareEquals(b[i]) == CmpBool::CmpTrue) {
      continue;
    }
    if (order == OrderByType::DESC) {
      return a[i].CompareGreaterThan(b[i]) == CmpBool::CmpTrue;
    }
    return a[i].CompareLessThan(b[i]) == CmpBool::CmpTrue;
  }
  return false;
}

auto CombineWindow(WindowFunctionType type, Value current, const Value &input) -> Value {
  switch (type) {
    case WindowFunctionType::CountStarAggregate:
      if (current.IsNull()) {
        return ValueFactory::GetIntegerValue(1);
      }
      return current.Add(ValueFactory::GetIntegerValue(1));
    case WindowFunctionType::CountAggregate:
      if (input.IsNull()) {
        return current;
      }
      if (current.IsNull()) {
        return ValueFactory::GetIntegerValue(1);
      }
      return current.Add(ValueFactory::GetIntegerValue(1));
    case WindowFunctionType::SumAggregate:
      if (input.IsNull()) {
        return current;
      }
      if (current.IsNull()) {
        return input;
      }
      return current.Add(input);
    case WindowFunctionType::MinAggregate:
      if (input.IsNull()) {
        return current;
      }
      if (current.IsNull() || input.CompareLessThan(current) == CmpBool::CmpTrue) {
        return input;
      }
      return current;
    case WindowFunctionType::MaxAggregate:
      if (input.IsNull()) {
        return current;
      }
      if (current.IsNull() || input.CompareGreaterThan(current) == CmpBool::CmpTrue) {
        return input;
      }
      return current;
    case WindowFunctionType::Rank:
      return current;
  }
  return current;
}

}  // namespace

auto WindowFunctionExecutor::ComputeWindowValues(const WindowFunctionPlanNode::WindowFunction &window,
                                                 const Schema &child_schema) const -> std::vector<Value> {
  const auto row_count = rows_.size();
  std::vector<Value> values(row_count);
  std::unordered_map<AggregateKey, std::vector<size_t>> partitions;
  for (size_t i = 0; i < row_count; i++) {
    partitions[MakePartitionKey(rows_[i], window.partition_by_)].push_back(i);
  }

  for (auto &[key, indexes] : partitions) {
    (void)key;
    std::vector<SortKey> order_keys(indexes.size());
    for (size_t i = 0; i < indexes.size(); i++) {
      order_keys[i] = GenerateSortKey(rows_[indexes[i]], window.order_by_, child_schema);
    }
    std::vector<size_t> order(indexes.size());
    std::iota(order.begin(), order.end(), 0);
    if (!window.order_by_.empty()) {
      std::sort(order.begin(), order.end(),
                [&](size_t a, size_t b) { return ComesBefore(order_keys[a], order_keys[b], window.order_by_); });
    }

    if (window.type_ == WindowFunctionType::Rank) {
      for (size_t pos = 0; pos < order.size(); pos++) {
        size_t rank = pos + 1;
        if (pos > 0 && KeysEqual(order_keys[order[pos]], order_keys[order[pos - 1]])) {
          rank = values[indexes[order[pos - 1]]].GetAs<int32_t>();
        }
        values[indexes[order[pos]]] = ValueFactory::GetIntegerValue(static_cast<int32_t>(rank));
      }
      continue;
    }

    if (window.order_by_.empty()) {
      Value acc = ValueFactory::GetNullValueByType(TypeId::INTEGER);
      for (auto idx : indexes) {
        auto input = window.function_->Evaluate(&rows_[idx], child_schema);
        acc = CombineWindow(window.type_, acc, input);
      }
      for (auto idx : indexes) {
        values[idx] = acc;
      }
      continue;
    }

    Value acc = ValueFactory::GetNullValueByType(TypeId::INTEGER);
    for (auto pos : order) {
      auto input = window.function_->Evaluate(&rows_[indexes[pos]], child_schema);
      acc = CombineWindow(window.type_, acc, input);
      values[indexes[pos]] = acc;
    }
  }
  return values;
}

void WindowFunctionExecutor::Init() {
  child_executor_->Init();
  rows_.clear();
  output_.clear();
  cursor_ = 0;

  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    rows_.push_back(std::move(tuple));
  }

  const auto &child_schema = child_executor_->GetOutputSchema();
  std::unordered_map<uint32_t, std::vector<Value>> window_columns;
  for (const auto &[idx, window] : plan_->window_functions_) {
    window_columns.emplace(idx, ComputeWindowValues(window, child_schema));
  }

  const auto &out_schema = plan_->OutputSchema();
  output_.reserve(rows_.size());
  for (size_t row = 0; row < rows_.size(); row++) {
    std::vector<Value> values;
    values.reserve(plan_->columns_.size());
    for (uint32_t col = 0; col < plan_->columns_.size(); col++) {
      auto found = window_columns.find(col);
      if (found != window_columns.end()) {
        values.push_back(found->second[row]);
      } else {
        values.push_back(plan_->columns_[col]->Evaluate(&rows_[row], child_schema));
      }
    }
    output_.emplace_back(values, &out_schema);
  }

  const std::vector<OrderBy> *order = nullptr;
  for (const auto &[idx, window] : plan_->window_functions_) {
    (void)idx;
    if (!window.order_by_.empty()) {
      order = &window.order_by_;
      break;
    }
  }
  if (order != nullptr) {
    std::vector<size_t> positions(output_.size());
    std::iota(positions.begin(), positions.end(), 0);
    std::vector<SortKey> keys;
    keys.reserve(rows_.size());
    for (const auto &row_tuple : rows_) {
      keys.push_back(GenerateSortKey(row_tuple, *order, child_schema));
    }
    std::stable_sort(positions.begin(), positions.end(),
                     [&](size_t a, size_t b) { return ComesBefore(keys[a], keys[b], *order); });
    std::vector<Tuple> ordered;
    ordered.reserve(output_.size());
    for (auto pos : positions) {
      ordered.push_back(std::move(output_[pos]));
    }
    output_ = std::move(ordered);
  }
}

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cursor_ >= output_.size()) {
    return false;
  }
  *tuple = output_[cursor_++];
  *rid = tuple->GetRid();
  return true;
}

}  // namespace bustub
