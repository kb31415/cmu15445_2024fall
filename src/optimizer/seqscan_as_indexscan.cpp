//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seqscan_as_indexscan.cpp
//
// Identification: src/optimizer/seqscan_as_indexscan.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <vector>

#include "catalog/catalog.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

namespace {

auto ExtractPointKeys(const AbstractExpression *predicate, uint32_t index_col, std::vector<AbstractExpressionRef> *keys)
    -> bool {
  if (predicate == nullptr) {
    return false;
  }
  if (const auto *logic = dynamic_cast<const LogicExpression *>(predicate); logic != nullptr) {
    if (logic->logic_type_ != LogicType::Or) {
      return false;
    }
    return ExtractPointKeys(logic->GetChildAt(0).get(), index_col, keys) &&
           ExtractPointKeys(logic->GetChildAt(1).get(), index_col, keys);
  }

  const auto *comparison = dynamic_cast<const ComparisonExpression *>(predicate);
  if (comparison == nullptr || comparison->comp_type_ != ComparisonType::Equal) {
    return false;
  }
  auto left = comparison->GetChildAt(0);
  auto right = comparison->GetChildAt(1);
  const ColumnValueExpression *column = nullptr;
  AbstractExpressionRef constant;
  if (dynamic_cast<const ColumnValueExpression *>(left.get()) != nullptr &&
      dynamic_cast<const ConstantValueExpression *>(right.get()) != nullptr) {
    column = dynamic_cast<const ColumnValueExpression *>(left.get());
    constant = right;
  } else if (dynamic_cast<const ColumnValueExpression *>(right.get()) != nullptr &&
             dynamic_cast<const ConstantValueExpression *>(left.get()) != nullptr) {
    column = dynamic_cast<const ColumnValueExpression *>(right.get());
    constant = left;
  } else {
    return false;
  }
  if (column->GetColIdx() != index_col) {
    return false;
  }
  keys->push_back(std::move(constant));
  return true;
}

}  // namespace

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.push_back(OptimizeSeqScanAsIndexScan(child));
  }
  auto optimized = plan->CloneWithChildren(std::move(children));

  if (optimized->GetType() != PlanType::SeqScan) {
    return optimized;
  }
  const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized);
  if (seq_scan_plan.filter_predicate_ == nullptr) {
    return optimized;
  }

  for (const auto &index_info : catalog_.GetTableIndexes(seq_scan_plan.table_name_)) {
    const auto &key_attrs = index_info->index_->GetKeyAttrs();
    if (key_attrs.size() != 1) {
      continue;
    }
    std::vector<AbstractExpressionRef> pred_keys;
    if (!ExtractPointKeys(seq_scan_plan.filter_predicate_.get(), key_attrs[0], &pred_keys) || pred_keys.empty()) {
      continue;
    }
    return std::make_shared<IndexScanPlanNode>(seq_scan_plan.output_schema_, seq_scan_plan.table_oid_,
                                               index_info->index_oid_, seq_scan_plan.filter_predicate_,
                                               std::move(pred_keys));
  }
  return optimized;
}

auto Optimizer::CanUseIndexForPredicate(const AbstractExpression *predicate, const IndexInfo *index_info) -> bool {
  if (predicate == nullptr || index_info == nullptr || index_info->index_ == nullptr) {
    return false;
  }
  const auto &key_attrs = index_info->index_->GetKeyAttrs();
  if (key_attrs.size() != 1) {
    return false;
  }
  std::vector<AbstractExpressionRef> keys;
  return ExtractPointKeys(predicate, key_attrs[0], &keys) && !keys.empty();
}

}  // namespace bustub
