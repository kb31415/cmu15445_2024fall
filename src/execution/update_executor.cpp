//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/update_executor.h"

#include <memory>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "type/value_factory.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  BUSTUB_ASSERT(table_info_ != nullptr, "Table not found");
}

void UpdateExecutor::Init() {
  child_executor_->Init();
  updated_count_ = 0;
  finished_ = false;
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (finished_) {
    return false;
  }

  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  auto *txn = exec_ctx_->GetTransaction();
  auto *txn_mgr = exec_ctx_->GetTransactionManager();

  struct KeyChange {
    Index *index_{nullptr};
    Tuple key_;
    RID rid_{};
  };
  std::vector<KeyChange> removed_keys;
  std::vector<KeyChange> inserted_keys;

  Tuple child_tuple{};
  RID child_rid{};
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    std::vector<Value> new_values;
    new_values.reserve(plan_->target_expressions_.size());
    for (const auto &expr : plan_->target_expressions_) {
      new_values.push_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
    }
    Tuple new_tuple{new_values, &table_info_->schema_};

    auto [old_meta, old_tuple, link] = GetTupleAndUndoLink(txn_mgr, table_info_->table_.get(), child_rid);
    (void)old_meta;
    (void)link;
    for (auto &index_info : indexes) {
      auto old_key =
          old_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      auto new_key =
          new_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      if (!IsTupleContentEqual(old_key, new_key)) {
        removed_keys.push_back(KeyChange{index_info->index_.get(), old_key, child_rid});
        inserted_keys.push_back(KeyChange{index_info->index_.get(), new_key, child_rid});
      }
    }

    if (txn != nullptr) {
      MvccModify(txn, txn_mgr, plan_->GetTableOid(), table_info_->table_.get(), &table_info_->schema_, child_rid,
                 &new_tuple, false);
    } else {
      table_info_->table_->UpdateTupleInPlace({0, false}, new_tuple, child_rid);
    }
    updated_count_++;
  }

  for (const auto &change : removed_keys) {
    change.index_->DeleteEntry(change.key_, change.rid_, txn);
  }
  for (const auto &change : inserted_keys) {
    if (!change.index_->InsertEntry(change.key_, change.rid_, txn)) {
      if (txn != nullptr && txn->GetTransactionState() == TransactionState::RUNNING) {
        txn->SetTainted();
      }
      throw ExecutionException("primary key conflict");
    }
  }

  std::vector<Value> values;
  values.emplace_back(ValueFactory::GetIntegerValue(updated_count_));
  *tuple = Tuple{values, &GetOutputSchema()};
  (void)rid;
  finished_ = true;
  return true;
}

}  // namespace bustub
