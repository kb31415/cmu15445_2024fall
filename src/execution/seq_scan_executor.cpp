//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

#include "common/macros.h"
#include "execution/execution_common.h"
#include "execution/expressions/constant_value_expression.h"
#include "type/value_factory.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  BUSTUB_ASSERT(table_info != nullptr, "Table not found");
  table_iter_ = std::make_unique<TableIterator>(table_info->table_->MakeIterator());

  auto *txn = exec_ctx_->GetTransaction();
  if (txn != nullptr && txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (plan_->filter_predicate_ != nullptr) {
      txn->AppendScanPredicate(plan_->GetTableOid(), plan_->filter_predicate_);
    } else {
      txn->AppendScanPredicate(plan_->GetTableOid(),
                               std::make_shared<ConstantValueExpression>(ValueFactory::GetBooleanValue(true)));
    }
  }
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  BUSTUB_ASSERT(table_info != nullptr, "Table not found");
  auto *txn = exec_ctx_->GetTransaction();
  auto *txn_mgr = exec_ctx_->GetTransactionManager();

  while (!table_iter_->IsEnd()) {
    *rid = table_iter_->GetRID();
    ++(*table_iter_);

    auto visible = MvccRead(txn, txn_mgr, table_info->schema_, table_info->table_.get(), *rid);
    if (!visible.has_value()) {
      continue;
    }
    if (plan_->filter_predicate_ != nullptr) {
      auto value = plan_->filter_predicate_->Evaluate(&(*visible), table_info->schema_);
      if (value.IsNull() || !value.GetAs<bool>()) {
        continue;
      }
    }

    std::vector<Value> values;
    values.reserve(GetOutputSchema().GetColumnCount());
    for (uint32_t i = 0; i < GetOutputSchema().GetColumnCount(); i++) {
      values.push_back(visible->GetValue(&table_info->schema_, i));
    }
    *tuple = Tuple{values, &GetOutputSchema()};
    return true;
  }
  return false;
}

}  // namespace bustub
