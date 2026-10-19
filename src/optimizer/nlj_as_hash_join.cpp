//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nlj_as_hash_join.cpp
//
// Identification: src/optimizer/nlj_as_hash_join.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

/** 检查表达式是否为等值比较 */
auto IsEquiComparison(const AbstractExpressionRef &expr) -> bool {
  const auto *comp_expr = dynamic_cast<const ComparisonExpression *>(expr.get());
  return comp_expr != nullptr && comp_expr->comp_type_ == ComparisonType::Equal;
}

/** 检查表达式是否为列值表达式 */
auto IsColumnExpression(const AbstractExpressionRef &expr) -> bool {
  return dynamic_cast<const ColumnValueExpression *>(expr.get()) != nullptr;
}

/** 从等值比较表达式中提取连接键 */
auto ExtractJoinKeys(const AbstractExpressionRef &predicate, std::vector<AbstractExpressionRef> &left_keys,
                     std::vector<AbstractExpressionRef> &right_keys) -> bool {
  if (predicate == nullptr) {
    return false;
  }

  // AND 递归拆分
  if (const LogicExpression *logic_expr = dynamic_cast<const LogicExpression *>(predicate.get());
      logic_expr != nullptr) {
    if (logic_expr->logic_type_ == LogicType::And) {
      // 递归处理AND的两个子表达式
      return ExtractJoinKeys(logic_expr->GetChildAt(0), left_keys, right_keys) &&
             ExtractJoinKeys(logic_expr->GetChildAt(1), left_keys, right_keys);
    }
    return false;
  }

  // 识别 列=列，且一边来自左表，一边来自右表
  if (IsEquiComparison(predicate)) {
    const auto &comp_expr = dynamic_cast<const ComparisonExpression &>(*predicate);
    auto left_expr = comp_expr.GetChildAt(0);
    auto right_expr = comp_expr.GetChildAt(1);

    // 检查两边都是列表达式
    if (IsColumnExpression(left_expr) && IsColumnExpression(right_expr)) {
      const auto &left_col = dynamic_cast<const ColumnValueExpression &>(*left_expr);
      const auto &right_col = dynamic_cast<const ColumnValueExpression &>(*right_expr);

      // 确保一个来自左表（tuple_idex=0），一个来自右表（tuple_idex=1）
      if (left_col.GetTupleIdx() == 0 && right_col.GetTupleIdx() == 1) {
        left_keys.emplace_back(left_expr);
        right_keys.emplace_back(right_expr);
        return true;
      } else if (left_col.GetTupleIdx() == 1 && right_col.GetTupleIdx() == 0) {
        left_keys.emplace_back(right_expr);
        right_keys.emplace_back(left_expr);
        return true;
      }
    }
  }
  return false;
}

void CollectTupleSides(const AbstractExpressionRef &expr, bool &has_left, bool &has_right) {
  if (const auto *column = dynamic_cast<const ColumnValueExpression *>(expr.get()); column != nullptr) {
    if (column->GetTupleIdx() == 0) {
      has_left = true;
    } else if (column->GetTupleIdx() == 1) {
      has_right = true;
    }
  }
  for (const auto &child : expr->GetChildren()) {
    CollectTupleSides(child, has_left, has_right);
  }
}

auto ForceTupleZero(const AbstractExpressionRef &expr) -> AbstractExpressionRef {
  if (const auto *column = dynamic_cast<const ColumnValueExpression *>(expr.get()); column != nullptr) {
    return std::make_shared<ColumnValueExpression>(0, column->GetColIdx(), column->GetReturnType());
  }
  std::vector<AbstractExpressionRef> children;
  children.reserve(expr->GetChildren().size());
  for (const auto &child : expr->GetChildren()) {
    children.emplace_back(ForceTupleZero(child));
  }
  return expr->CloneWithChildren(std::move(children));
}

/** 把连接谓词里的列改写成单表过滤器使用的列号。right 侧列会加上左表列数。 */
auto RewriteColumnIdx(const AbstractExpressionRef &expr, uint32_t left_column_cnt) -> AbstractExpressionRef {
  if (const auto *column = dynamic_cast<const ColumnValueExpression *>(expr.get()); column != nullptr) {
    auto col_idx = column->GetColIdx();
    if (column->GetTupleIdx() == 1) {
      col_idx += left_column_cnt;
    }
    return std::make_shared<ColumnValueExpression>(0, col_idx, column->GetReturnType());
  }
  std::vector<AbstractExpressionRef> children;
  children.reserve(expr->GetChildren().size());
  for (const auto &child : expr->GetChildren()) {
    children.emplace_back(RewriteColumnIdx(child, left_column_cnt));
  }
  return expr->CloneWithChildren(std::move(children));
}

auto CombineAnd(const std::vector<AbstractExpressionRef> &exprs) -> AbstractExpressionRef {
  auto combined = exprs.front();
  for (size_t i = 1; i < exprs.size(); i++) {
    combined = std::make_shared<LogicExpression>(combined, exprs[i], LogicType::And);
  }
  return combined;
}

/**
 * 把合取谓词拆成等值连接键、可以下推的单表谓词，以及必须留在连接之上的剩余谓词。
 * 遇到 OR 时无法安全拆分，返回 false。
 */
auto SplitJoinPredicate(const AbstractExpressionRef &predicate, std::vector<AbstractExpressionRef> &left_keys,
                        std::vector<AbstractExpressionRef> &right_keys,
                        std::vector<AbstractExpressionRef> &left_filters,
                        std::vector<AbstractExpressionRef> &right_filters,
                        std::vector<AbstractExpressionRef> &residuals) -> bool {
  if (predicate == nullptr) {
    return false;
  }
  if (const auto *logic_expr = dynamic_cast<const LogicExpression *>(predicate.get()); logic_expr != nullptr) {
    if (logic_expr->logic_type_ != LogicType::And) {
      return false;
    }
    return SplitJoinPredicate(logic_expr->GetChildAt(0), left_keys, right_keys, left_filters, right_filters,
                              residuals) &&
           SplitJoinPredicate(logic_expr->GetChildAt(1), left_keys, right_keys, left_filters, right_filters, residuals);
  }

  if (IsEquiComparison(predicate)) {
    const auto &comp_expr = dynamic_cast<const ComparisonExpression &>(*predicate);
    auto lhs = comp_expr.GetChildAt(0);
    auto rhs = comp_expr.GetChildAt(1);
    if (IsColumnExpression(lhs) && IsColumnExpression(rhs)) {
      const auto &left_col = dynamic_cast<const ColumnValueExpression &>(*lhs);
      const auto &right_col = dynamic_cast<const ColumnValueExpression &>(*rhs);
      if (left_col.GetTupleIdx() == 0 && right_col.GetTupleIdx() == 1) {
        left_keys.emplace_back(lhs);
        right_keys.emplace_back(rhs);
        return true;
      }
      if (left_col.GetTupleIdx() == 1 && right_col.GetTupleIdx() == 0) {
        left_keys.emplace_back(rhs);
        right_keys.emplace_back(lhs);
        return true;
      }
    }
  }

  bool has_left = false;
  bool has_right = false;
  CollectTupleSides(predicate, has_left, has_right);
  if (has_left && !has_right) {
    left_filters.emplace_back(predicate);
    return true;
  }
  if (has_right && !has_left) {
    right_filters.emplace_back(predicate);
    return true;
  }
  if (!has_left && !has_right) {
    if (const auto *constant = dynamic_cast<const ConstantValueExpression *>(predicate.get());
        constant != nullptr && constant->val_.CastAs(TypeId::BOOLEAN).GetAs<bool>()) {
      return true;
    }
  }
  residuals.emplace_back(predicate);
  return true;
}

/**
 * @brief optimize nested loop join into hash join.
 * In the starter code, we will check NLJs with exactly one equal condition. You can further support optimizing joins
 * with multiple eq conditions.
 */
auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for Spring 2025: You should support join keys of any number of conjunction of equi-conditions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  // 递归优化子节点
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  // 检查是否为 NLJ
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);

    if (nlj_plan.GetJoinType() == JoinType::INNER) {
      std::vector<AbstractExpressionRef> left_keys;
      std::vector<AbstractExpressionRef> right_keys;
      std::vector<AbstractExpressionRef> left_filters;
      std::vector<AbstractExpressionRef> right_filters;
      std::vector<AbstractExpressionRef> residuals;
      if (SplitJoinPredicate(nlj_plan.Predicate(), left_keys, right_keys, left_filters, right_filters, residuals) &&
          !left_keys.empty()) {
        auto left = nlj_plan.GetLeftPlan();
        auto right = nlj_plan.GetRightPlan();
        if (!left_filters.empty()) {
          left = std::make_shared<FilterPlanNode>(left->output_schema_, CombineAnd(left_filters), left);
        }
        if (!right_filters.empty()) {
          std::vector<AbstractExpressionRef> rewritten;
          rewritten.reserve(right_filters.size());
          for (const auto &expr : right_filters) {
            rewritten.emplace_back(ForceTupleZero(expr));
          }
          right = std::make_shared<FilterPlanNode>(right->output_schema_, CombineAnd(rewritten), right);
        }
        AbstractPlanNodeRef joined =
            std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, std::move(left), std::move(right),
                                               std::move(left_keys), std::move(right_keys), nlj_plan.GetJoinType());
        if (!residuals.empty()) {
          auto left_column_cnt = nlj_plan.GetLeftPlan()->OutputSchema().GetColumnCount();
          std::vector<AbstractExpressionRef> rewritten;
          rewritten.reserve(residuals.size());
          for (const auto &expr : residuals) {
            rewritten.emplace_back(RewriteColumnIdx(expr, left_column_cnt));
          }
          joined = std::make_shared<FilterPlanNode>(joined->output_schema_, CombineAnd(rewritten), joined);
        }
        return joined;
      }
    } else if (nlj_plan.GetJoinType() == JoinType::LEFT) {
      std::vector<AbstractExpressionRef> left_keys;
      std::vector<AbstractExpressionRef> right_keys;
      if (ExtractJoinKeys(nlj_plan.Predicate(), left_keys, right_keys) && !left_keys.empty()) {
        return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                  nlj_plan.GetRightPlan(), std::move(left_keys), std::move(right_keys),
                                                  nlj_plan.GetJoinType());
      }
    }
  }
  return optimized_plan;
}

}  // namespace bustub
