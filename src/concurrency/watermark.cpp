//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// watermark.cpp
//
// Identification: src/concurrency/watermark.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/watermark.h"
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }
  auto &count = current_reads_[read_ts];
  count += 1;
  if (count == 1) {
    active_reads_.insert(read_ts);
  }
  watermark_ = active_reads_.empty() ? commit_ts_ : *active_reads_.begin();
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  auto iter = current_reads_.find(read_ts);
  if (iter == current_reads_.end()) {
    return;
  }
  iter->second -= 1;
  if (iter->second == 0) {
    current_reads_.erase(iter);
    active_reads_.erase(read_ts);
  }
  watermark_ = active_reads_.empty() ? commit_ts_ : *active_reads_.begin();
}

}  // namespace bustub
