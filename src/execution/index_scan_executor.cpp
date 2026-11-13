//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/index_scan_executor.h"

#include <algorithm>

#include "common/macros.h"
#include "execution/execution_common.h"
#include "execution/expressions/constant_value_expression.h"
#include "storage/table/table_heap.h"
#include "type/value_factory.h"

namespace bustub {

IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  tree_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info->index_.get());
  BUSTUB_ASSERT(tree_ != nullptr, "Index is not a B+ tree index");
}

void IndexScanExecutor::Init() {
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  BUSTUB_ASSERT(index_info != nullptr, "Index not found");
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  BUSTUB_ASSERT(table_info != nullptr, "Table not found");

  auto *txn = exec_ctx_->GetTransaction();
  if (txn != nullptr && txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (plan_->filter_predicate_ != nullptr) {
      txn->AppendScanPredicate(plan_->table_oid_, plan_->filter_predicate_);
    } else {
      txn->AppendScanPredicate(plan_->table_oid_,
                               std::make_shared<ConstantValueExpression>(ValueFactory::GetBooleanValue(true)));
    }
  }

  if (!plan_->pred_keys_.empty()) {
    use_iterator_ = false;
    result_rids_.clear();
    const auto key_col_count = index_info->index_->GetIndexColumnCount();
    const auto &keys = plan_->pred_keys_;
    for (size_t start = 0; start < keys.size(); start += key_col_count) {
      std::vector<Value> key_values;
      key_values.reserve(key_col_count);
      for (uint32_t col = 0; col < key_col_count && start + col < keys.size(); col++) {
        key_values.push_back(keys[start + col]->Evaluate(nullptr, index_info->key_schema_));
      }
      if (key_values.size() != key_col_count) {
        break;
      }
      Tuple search_key{key_values, &index_info->key_schema_};
      index_info->index_->ScanKey(search_key, &result_rids_, exec_ctx_->GetTransaction());
    }
    // A primary-key update replaces the index entry. An older snapshot still needs the
    // previous key, so consider every slot and let the predicate keep the visible version.
    auto iter = table_info->table_->MakeIterator();
    while (!iter.IsEnd()) {
      auto heap_rid = iter.GetRID();
      ++iter;
      if (std::find(result_rids_.begin(), result_rids_.end(), heap_rid) == result_rids_.end()) {
        result_rids_.push_back(heap_rid);
      }
    }
  } else {
    use_iterator_ = true;
    it_ = tree_->GetBeginIterator();
    end_it_ = tree_->GetEndIterator();
    result_rids_.clear();
  }
  current_idx_ = 0;
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  BUSTUB_ASSERT(table_info != nullptr, "Table not found");
  auto *txn = exec_ctx_->GetTransaction();
  auto *txn_mgr = exec_ctx_->GetTransactionManager();

  auto emit = [&](RID current_rid, const Tuple &visible) -> bool {
    if (plan_->filter_predicate_ != nullptr) {
      auto value = plan_->filter_predicate_->Evaluate(&visible, table_info->schema_);
      if (value.IsNull() || !value.GetAs<bool>()) {
        return false;
      }
    }
    std::vector<Value> values;
    values.reserve(GetOutputSchema().GetColumnCount());
    for (uint32_t i = 0; i < GetOutputSchema().GetColumnCount(); i++) {
      values.push_back(visible.GetValue(&table_info->schema_, i));
    }
    *tuple = Tuple{values, &GetOutputSchema()};
    *rid = current_rid;
    return true;
  };

  if (use_iterator_) {
    while (!it_.IsEnd()) {
      auto [key, value] = *it_;
      (void)key;
      RID current_rid = value;
      ++it_;
      auto visible = MvccRead(txn, txn_mgr, table_info->schema_, table_info->table_.get(), current_rid);
      if (!visible.has_value()) {
        continue;
      }
      if (emit(current_rid, *visible)) {
        return true;
      }
    }
  } else {
    while (current_idx_ < result_rids_.size()) {
      auto current_rid = result_rids_[current_idx_++];
      auto visible = MvccRead(txn, txn_mgr, table_info->schema_, table_info->table_.get(), current_rid);
      if (!visible.has_value()) {
        continue;
      }
      if (emit(current_rid, *visible)) {
        return true;
      }
    }
  }
  return false;
}

}  // namespace bustub
