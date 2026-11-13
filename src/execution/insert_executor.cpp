//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/insert_executor.h"

#include <memory>
#include <vector>

#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "type/value_factory.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  inserted_count_ = 0;
  finished_ = false;
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (finished_) {
    return false;
  }

  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  BUSTUB_ASSERT(table_info != nullptr, "Table not found");
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  std::shared_ptr<IndexInfo> primary;
  for (auto &index_info : indexes) {
    if (index_info->is_primary_key_) {
      primary = index_info;
    }
  }

  auto *txn = exec_ctx_->GetTransaction();
  auto *txn_mgr = exec_ctx_->GetTransactionManager();
  Tuple child_tuple{};
  RID child_rid{};

  while (child_executor_->Next(&child_tuple, &child_rid)) {
    if (primary != nullptr && txn != nullptr) {
      auto key = child_tuple.KeyFromTuple(table_info->schema_, primary->key_schema_, primary->index_->GetKeyAttrs());
      std::vector<RID> found;
      primary->index_->ScanKey(key, &found, txn);
      if (!found.empty()) {
        auto [meta, old_tuple, link] = GetTupleAndUndoLink(txn_mgr, table_info->table_.get(), found[0]);
        (void)old_tuple;
        (void)link;
        bool conflict = meta.ts_ > txn->GetReadTs() && meta.ts_ != txn->GetTransactionId();
        if (conflict || !meta.is_deleted_) {
          if (txn->GetTransactionState() == TransactionState::RUNNING) {
            txn->SetTainted();
          }
          throw ExecutionException("write-write conflict");
        }
        MvccModify(txn, txn_mgr, plan_->GetTableOid(), table_info->table_.get(), &table_info->schema_, found[0],
                   &child_tuple, false);
        inserted_count_++;
        continue;
      }
    }

    timestamp_t ts = txn == nullptr ? 0 : txn->GetTransactionTempTs();
    TupleMeta tuple_meta{ts, false};
    auto insert_rid = table_info->table_->InsertTuple(tuple_meta, child_tuple, exec_ctx_->GetLockManager(), txn,
                                                      plan_->GetTableOid());
    if (!insert_rid.has_value()) {
      continue;
    }
    std::vector<std::pair<IndexInfo *, Tuple>> indexed;
    bool index_conflict = false;
    for (auto &index_info : indexes) {
      auto key_tuple =
          child_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      if (!index_info->index_->InsertEntry(key_tuple, *insert_rid, txn)) {
        index_conflict = true;
        break;
      }
      indexed.emplace_back(index_info.get(), key_tuple);
    }
    if (index_conflict) {
      for (const auto &[index_info, key_tuple] : indexed) {
        index_info->index_->DeleteEntry(key_tuple, *insert_rid, txn);
      }
      table_info->table_->UpdateTupleMeta({0, true}, *insert_rid);
      if (txn != nullptr && txn->GetTransactionState() == TransactionState::RUNNING) {
        txn->SetTainted();
      }
      throw ExecutionException("primary key conflict");
    }
    inserted_count_++;
    if (txn != nullptr) {
      txn->AppendWriteSet(plan_->GetTableOid(), *insert_rid);
    }
  }

  std::vector<Value> values;
  values.emplace_back(ValueFactory::GetIntegerValue(inserted_count_));
  *tuple = Tuple{values, &GetOutputSchema()};
  (void)rid;
  finished_ = true;
  return true;
}

}  // namespace bustub
