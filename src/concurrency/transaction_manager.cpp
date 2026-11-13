//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Begins a new transaction.
 * @param isolation_level an optional isolation level of the transaction.
 * @return an initialized transaction
 */
auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  txn_ref->read_ts_ = last_commit_ts_.load();
  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

/** @brief Verify if a txn satisfies serializability. We will not test this function and you can change / remove it as
 * you want. */
auto TransactionManager::VerifyTxn(Transaction *txn) -> bool {
  if (catalog_ == nullptr || txn->GetScanPredicates().empty() || txn->GetWriteSets().empty()) {
    return true;
  }
  const auto read_ts = txn->GetReadTs();
  struct ConflictTxn {
    timestamp_t commit_ts_{0};
    std::unordered_map<table_oid_t, std::unordered_set<RID>> writes_;
  };
  std::vector<ConflictTxn> conflicts;
  {
    std::shared_lock<std::shared_mutex> lck(txn_map_mutex_);
    for (const auto &[other_id, other] : txn_map_) {
      (void)other_id;
      if (other.get() == txn || other->GetTransactionState() != TransactionState::COMMITTED) {
        continue;
      }
      if (other->GetCommitTs() <= read_ts) {
        continue;
      }
      conflicts.push_back(ConflictTxn{other->GetCommitTs(), other->GetWriteSets()});
    }
  }

  const auto &predicates = txn->GetScanPredicates();
  for (const auto &conflict : conflicts) {
    for (const auto &[table_oid, rids] : conflict.writes_) {
      auto pred_iter = predicates.find(table_oid);
      if (pred_iter == predicates.end()) {
        continue;
      }
      auto table_info = catalog_->GetTable(table_oid);
      for (auto rid : rids) {
        auto [meta, tuple, link] = GetTupleAndUndoLink(this, table_info->table_.get(), rid);
        std::vector<UndoLog> logs;
        timestamp_t version_ts = meta.ts_;
        auto cur = link.value_or(UndoLink{});
        while (version_ts > conflict.commit_ts_ && cur.IsValid()) {
          auto log = GetUndoLog(cur);
          logs.push_back(log);
          version_ts = log.ts_;
          cur = log.prev_version_;
        }
        if (version_ts != conflict.commit_ts_) {
          continue;
        }
        auto visible = ReconstructTuple(&table_info->schema_, tuple, meta, logs);
        if (!visible.has_value()) {
          continue;
        }
        for (const auto &pred : pred_iter->second) {
          auto value = pred->Evaluate(&(*visible), table_info->schema_);
          if (!value.IsNull() && value.GetAs<bool>()) {
            return false;
          }
        }
      }
    }
  }
  return true;
}

/**
 * Commits a transaction.
 * @param txn the transaction to commit, the txn will be managed by the txn manager so no need to delete it by
 * yourself
 */
auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  if (txn->state_ == TransactionState::TAINTED) {
    return false;
  }
  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  auto commit_ts = last_commit_ts_.load() + 1;
  if (catalog_ != nullptr) {
    for (const auto &[table_oid, rids] : txn->GetWriteSets()) {
      auto table_info = catalog_->GetTable(table_oid);
      for (auto rid : rids) {
        auto [meta, tuple] = table_info->table_->GetTuple(rid);
        meta.ts_ = commit_ts;
        table_info->table_->UpdateTupleMeta(meta, rid);
      }
    }
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->commit_ts_ = commit_ts;
  last_commit_ts_.store(commit_ts);
  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(commit_ts);
  running_txns_.RemoveTxn(txn->read_ts_);
  return true;
}

/**
 * Aborts a transaction
 * @param txn the transaction to abort, the txn will be managed by the txn manager so no need to delete it by yourself
 */
void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  if (catalog_ != nullptr) {
    for (const auto &[table_oid, rids] : txn->GetWriteSets()) {
      auto table_info = catalog_->GetTable(table_oid);
      auto indexes = catalog_->GetTableIndexes(table_info->name_);
      for (auto rid : rids) {
        auto [meta, tuple, link] = GetTupleAndUndoLink(this, table_info->table_.get(), rid);
        bool has_undo = link.has_value() && link->IsValid();
        Tuple restored_tuple = tuple;
        TupleMeta restored_meta{txn->GetReadTs(), true};
        std::optional<UndoLink> restored_link = std::nullopt;
        if (has_undo) {
          auto log = GetUndoLog(*link);
          auto restored = ReconstructTuple(&table_info->schema_, tuple, meta, {log});
          if (restored.has_value()) {
            restored_meta = {log.ts_, false};
            restored_tuple = *restored;
          } else {
            restored_meta = {log.ts_, true};
          }
          if (log.prev_version_.IsValid()) {
            restored_link = log.prev_version_;
          }

          for (const auto &index_info : indexes) {
            auto aborted_key =
                tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
            auto restored_key = restored_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_,
                                                            index_info->index_->GetKeyAttrs());
            bool key_changed = !IsTupleContentEqual(aborted_key, restored_key);
            // A deleted slot keeps its index entry so a later insert of the same key can reuse it.
            // Only drop the entry when this transaction changed the key.
            if (!meta.is_deleted_ && key_changed) {
              index_info->index_->DeleteEntry(aborted_key, rid, txn);
            }
            if (!restored_meta.is_deleted_ && (meta.is_deleted_ || key_changed)) {
              index_info->index_->InsertEntry(restored_key, rid, txn);
            }
          }
          UpdateTupleAndUndoLink(this, rid, restored_link, table_info->table_.get(), txn, restored_meta, restored_tuple,
                                 nullptr);
        } else {
          // A fresh insert has no undo log. Leave the index entry so a later insert of the same key reuses this slot.
          table_info->table_->UpdateTupleMeta(restored_meta, rid);
        }
      }
    }
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

/** @brief Stop-the-world garbage collection. Will be called only when all transactions are not accessing the table
 * heap. */
void TransactionManager::GarbageCollection() {
  const auto watermark = GetWatermark();
  std::unordered_set<txn_id_t> needed;
  if (catalog_ != nullptr) {
    for (const auto &name : catalog_->GetTableNames()) {
      auto table_info = catalog_->GetTable(name);
      auto iter = table_info->table_->MakeEagerIterator();
      while (!iter.IsEnd()) {
        auto rid = iter.GetRID();
        auto [meta, tuple] = iter.GetTuple();
        ++iter;
        (void)tuple;
        if (meta.ts_ <= watermark) {
          continue;
        }
        auto link = GetUndoLink(rid).value_or(UndoLink{});
        while (link.IsValid()) {
          needed.insert(link.prev_txn_);
          auto log = GetUndoLogOptional(link);
          if (!log.has_value() || log->ts_ <= watermark) {
            break;
          }
          link = log->prev_version_;
        }
      }
    }
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  std::vector<txn_id_t> drop;
  for (const auto &[id, txn] : txn_map_) {
    auto state = txn->GetTransactionState();
    if (state != TransactionState::COMMITTED && state != TransactionState::ABORTED) {
      continue;
    }
    if (needed.find(id) == needed.end()) {
      drop.push_back(id);
    }
  }
  for (auto id : drop) {
    txn_map_.erase(id);
  }
}

}  // namespace bustub
