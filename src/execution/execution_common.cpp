//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// execution_common.cpp
//
// Identification: src/execution/execution_common.cpp
//
// Copyright (c) 2024-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/execution_common.h"

#include <optional>
#include <string>
#include <vector>

#include "catalog/catalog.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "type/value_factory.h"

namespace bustub {

TupleComparator::TupleComparator(std::vector<OrderBy> order_bys) : order_bys_(std::move(order_bys)) {}

auto TupleComparator::operator()(const SortEntry &entry_a, const SortEntry &entry_b) const -> bool {
  for (size_t i = 0; i < entry_a.first.size(); i++) {
    if (order_bys_[i].first == OrderByType::INVALID) {
      return false;
    }

    const auto &key_a = entry_a.first[i];
    const auto &key_b = entry_b.first[i];

    // 升序比较
    if (order_bys_[i].first == OrderByType::DEFAULT || order_bys_[i].first == OrderByType::ASC) {
      if (key_a.CompareLessThan(key_b) == CmpBool::CmpTrue) {
        return true;
      }
      if (key_a.CompareGreaterThan(key_b) == CmpBool::CmpTrue) {
        return false;
      }
    }

    // 降序比较
    if (order_bys_[i].first == OrderByType::DESC) {
      if (key_a.CompareGreaterThan(key_b) == CmpBool::CmpTrue) {
        return true;
      }
      if (key_a.CompareLessThan(key_b) == CmpBool::CmpTrue) {
        return false;
      }
    }
  }
  // 所有排序键都相等。严格弱序要求相等时返回 false。
  return false;
}

auto GenerateSortKey(const Tuple &tuple, const std::vector<OrderBy> &order_bys, const Schema &schema) -> SortKey {
  SortKey sort_key{};
  for (const auto &order_by : order_bys) {
    sort_key.emplace_back(order_by.second->Evaluate(&tuple, schema));
  }

  return sort_key;
}

/**
 * Above are all you need for P3.
 * You can ignore the remaining part of this file until P4.
 */

namespace {

auto ValuesEqual(const Value &lhs, const Value &rhs) -> bool {
  if (lhs.IsNull() && rhs.IsNull()) {
    return true;
  }
  return lhs.CompareEquals(rhs) == CmpBool::CmpTrue;
}

auto PartialTuple(const Schema *schema, const std::vector<bool> &modified, const std::vector<Value> &values) -> Tuple {
  std::vector<uint32_t> attrs;
  attrs.reserve(values.size());
  for (uint32_t i = 0; i < modified.size(); i++) {
    if (modified[i]) {
      attrs.push_back(i);
    }
  }
  auto partial = Schema::CopySchema(schema, attrs);
  if (attrs.empty()) {
    return Tuple{};
  }
  return Tuple{values, &partial};
}

auto UndoValues(const Schema *schema, const UndoLog &log) -> std::vector<Value> {
  std::vector<uint32_t> attrs;
  for (uint32_t i = 0; i < log.modified_fields_.size(); i++) {
    if (log.modified_fields_[i]) {
      attrs.push_back(i);
    }
  }
  if (attrs.empty()) {
    return {};
  }
  auto partial = Schema::CopySchema(schema, attrs);
  std::vector<Value> values;
  values.reserve(attrs.size());
  for (uint32_t i = 0; i < attrs.size(); i++) {
    values.push_back(log.tuple_.GetValue(&partial, i));
  }
  return values;
}

void Taint(Transaction *txn) {
  if (txn->GetTransactionState() == TransactionState::RUNNING) {
    txn->SetTainted();
  }
  throw ExecutionException("write-write conflict");
}

}  // namespace

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  const auto col_cnt = schema->GetColumnCount();
  bool deleted = base_meta.is_deleted_;
  std::vector<Value> values;
  values.reserve(col_cnt);
  if (!deleted) {
    for (uint32_t i = 0; i < col_cnt; i++) {
      values.push_back(base_tuple.GetValue(schema, i));
    }
  } else {
    for (uint32_t i = 0; i < col_cnt; i++) {
      values.push_back(ValueFactory::GetNullValueByType(schema->GetColumn(i).GetType()));
    }
  }

  for (const auto &log : undo_logs) {
    if (log.is_deleted_) {
      deleted = true;
      continue;
    }
    deleted = false;
    auto stored = UndoValues(schema, log);
    uint32_t stored_idx = 0;
    for (uint32_t i = 0; i < col_cnt && i < log.modified_fields_.size(); i++) {
      if (log.modified_fields_[i]) {
        values[i] = stored[stored_idx++];
      }
    }
  }

  if (deleted) {
    return std::nullopt;
  }
  return Tuple{values, schema};
}

/**
 * @brief Collects the undo logs sufficient to reconstruct the tuple w.r.t. the txn.
 *
 * @param rid The RID of the tuple.
 * @param base_meta The metadata of the base tuple.
 * @param base_tuple The base tuple.
 * @param undo_link The undo link to the latest undo log.
 * @param txn The transaction.
 * @param txn_mgr The transaction manager.
 * @return An optional vector of undo logs to pass to ReconstructTuple(). std::nullopt if the tuple did not exist at the
 * time.
 */
auto CollectUndoLogs(RID rid, const TupleMeta &base_meta, const Tuple &base_tuple, std::optional<UndoLink> undo_link,
                     Transaction *txn, TransactionManager *txn_mgr) -> std::optional<std::vector<UndoLog>> {
  (void)rid;
  (void)base_tuple;
  if (base_meta.ts_ <= txn->GetReadTs() || base_meta.ts_ == txn->GetTransactionId()) {
    return std::vector<UndoLog>{};
  }

  std::vector<UndoLog> logs;
  UndoLink link = undo_link.value_or(UndoLink{});
  while (link.IsValid()) {
    auto log = txn_mgr->GetUndoLog(link);
    logs.push_back(log);
    if (log.ts_ <= txn->GetReadTs()) {
      return logs;
    }
    link = log.prev_version_;
  }
  return std::nullopt;
}

/**
 * @brief Generates a new undo log as the transaction tries to modify this tuple at the first time.
 *
 * @param schema The schema of the table.
 * @param base_tuple The base tuple before the update, the one retrieved from the table heap. nullptr if the tuple is
 * deleted.
 * @param target_tuple The target tuple after the update. nullptr if this is a deletion.
 * @param ts The timestamp of the base tuple.
 * @param prev_version The undo link to the latest undo log of this tuple.
 * @return The generated undo log.
 */
auto GenerateNewUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple, timestamp_t ts,
                        UndoLink prev_version) -> UndoLog {
  UndoLog log;
  log.ts_ = ts;
  log.prev_version_ = prev_version;
  log.is_deleted_ = base_tuple == nullptr;
  log.modified_fields_.assign(schema->GetColumnCount(), false);

  if (base_tuple == nullptr || target_tuple == nullptr) {
    if (base_tuple != nullptr) {
      std::vector<Value> values;
      values.reserve(schema->GetColumnCount());
      for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
        log.modified_fields_[i] = true;
        values.push_back(base_tuple->GetValue(schema, i));
      }
      log.tuple_ = PartialTuple(schema, log.modified_fields_, values);
    }
    return log;
  }

  std::vector<Value> values;
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    auto old_val = base_tuple->GetValue(schema, i);
    auto new_val = target_tuple->GetValue(schema, i);
    if (!ValuesEqual(old_val, new_val)) {
      log.modified_fields_[i] = true;
      values.push_back(old_val);
    }
  }
  log.tuple_ = PartialTuple(schema, log.modified_fields_, values);
  return log;
}

/**
 * @brief Generate the updated undo log to replace the old one, whereas the tuple is already modified by this txn once.
 *
 * @param schema The schema of the table.
 * @param base_tuple The base tuple before the update, the one retrieved from the table heap. nullptr if the tuple is
 * deleted.
 * @param target_tuple The target tuple after the update. nullptr if this is a deletion.
 * @param log The original undo log.
 * @return The updated undo log.
 */
auto GenerateUpdatedUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple,
                            const UndoLog &log) -> UndoLog {
  Tuple placeholder = base_tuple == nullptr ? Tuple{} : *base_tuple;
  TupleMeta meta{log.ts_, base_tuple == nullptr};
  auto original = ReconstructTuple(schema, placeholder, meta, {log});
  const Tuple *original_ptr = original.has_value() ? &(*original) : nullptr;
  auto updated = GenerateNewUndoLog(schema, original_ptr, target_tuple, log.ts_, log.prev_version_);
  if (original_ptr == nullptr || updated.is_deleted_) {
    return updated;
  }
  bool grew = false;
  for (uint32_t i = 0; i < schema->GetColumnCount() && i < log.modified_fields_.size(); i++) {
    if (log.modified_fields_[i] && !updated.modified_fields_[i]) {
      updated.modified_fields_[i] = true;
      grew = true;
    }
  }
  if (!grew) {
    return updated;
  }
  std::vector<Value> values;
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    if (updated.modified_fields_[i]) {
      values.push_back(original_ptr->GetValue(schema, i));
    }
  }
  updated.tuple_ = PartialTuple(schema, updated.modified_fields_, values);
  return updated;
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  fmt::println(stderr, "debug_hook: {}", info);
  if (table_info == nullptr || table_heap == nullptr || txn_mgr == nullptr) {
    return;
  }
  const auto &schema = table_info->schema_;
  auto iter = table_heap->MakeEagerIterator();
  while (!iter.IsEnd()) {
    auto rid = iter.GetRID();
    auto [meta, tuple] = iter.GetTuple();
    ++iter;
    std::string ts_str =
        meta.ts_ >= TXN_START_ID ? fmt::format("txn{}", meta.ts_ ^ TXN_START_ID) : fmt::format("{}", meta.ts_);
    if (meta.is_deleted_) {
      fmt::println(stderr, "RID={}/{} ts={} <del marker> tuple={}", rid.GetPageId(), rid.GetSlotNum(), ts_str,
                   tuple.ToString(&schema));
    } else {
      fmt::println(stderr, "RID={}/{} ts={} tuple={}", rid.GetPageId(), rid.GetSlotNum(), ts_str,
                   tuple.ToString(&schema));
    }
    auto link = txn_mgr->GetUndoLink(rid).value_or(UndoLink{});
    while (link.IsValid()) {
      auto log_opt = txn_mgr->GetUndoLogOptional(link);
      if (!log_opt.has_value()) {
        break;
      }
      const auto &log = *log_opt;
      auto txn_readable = link.prev_txn_ ^ TXN_START_ID;
      fmt::println(stderr, "  txn{}@{} del={} ts={}", txn_readable, link.prev_log_idx_, log.is_deleted_, log.ts_);
      link = log.prev_version_;
    }
  }
}

void MvccModify(Transaction *txn, TransactionManager *txn_mgr, table_oid_t oid, TableHeap *heap, const Schema *schema,
                RID rid, const Tuple *target, bool deleted) {
  auto [meta, base, link] = GetTupleAndUndoLink(txn_mgr, heap, rid);
  if (meta.ts_ > txn->GetReadTs() && meta.ts_ != txn->GetTransactionId()) {
    Taint(txn);
  }

  Tuple stored = target == nullptr ? base : *target;
  TupleMeta new_meta{txn->GetTransactionTempTs(), deleted};
  auto expect = meta;
  auto check = [expect](const TupleMeta &seen, const Tuple &, RID, std::optional<UndoLink>) {
    return seen.ts_ == expect.ts_ && seen.is_deleted_ == expect.is_deleted_;
  };

  bool owns_base = meta.ts_ == txn->GetTransactionId();
  bool owns_log = owns_base && link.has_value() && link->IsValid() && link->prev_txn_ == txn->GetTransactionId();

  if (owns_base && !owns_log) {
    if (!UpdateTupleAndUndoLink(txn_mgr, rid, link, heap, txn, new_meta, stored, check)) {
      Taint(txn);
    }
    txn->AppendWriteSet(oid, rid);
    return;
  }

  if (owns_log) {
    auto old_log = txn_mgr->GetUndoLog(*link);
    const Tuple *base_ptr = meta.is_deleted_ ? nullptr : &base;
    auto updated = GenerateUpdatedUndoLog(schema, base_ptr, deleted ? nullptr : target, old_log);
    txn->ModifyUndoLog(link->prev_log_idx_, std::move(updated));
    if (!UpdateTupleAndUndoLink(txn_mgr, rid, link, heap, txn, new_meta, stored, check)) {
      Taint(txn);
    }
    txn->AppendWriteSet(oid, rid);
    return;
  }

  const Tuple *base_ptr = meta.is_deleted_ ? nullptr : &base;
  UndoLink prev = link.value_or(UndoLink{});
  auto new_log = GenerateNewUndoLog(schema, base_ptr, deleted ? nullptr : target, meta.ts_, prev);
  auto new_link = txn->AppendUndoLog(std::move(new_log));
  if (!UpdateTupleAndUndoLink(txn_mgr, rid, new_link, heap, txn, new_meta, stored, check)) {
    Taint(txn);
  }
  txn->AppendWriteSet(oid, rid);
}

auto MvccRead(Transaction *txn, TransactionManager *txn_mgr, const Schema &schema, TableHeap *heap, RID rid)
    -> std::optional<Tuple> {
  auto [meta, tuple, link] = GetTupleAndUndoLink(txn_mgr, heap, rid);
  if (txn == nullptr || meta.ts_ <= txn->GetReadTs() || meta.ts_ == txn->GetTransactionId()) {
    if (meta.is_deleted_) {
      return std::nullopt;
    }
    return tuple;
  }
  auto logs = CollectUndoLogs(rid, meta, tuple, link, txn, txn_mgr);
  if (!logs.has_value()) {
    return std::nullopt;
  }
  return ReconstructTuple(&schema, tuple, meta, *logs);
}

}  // namespace bustub