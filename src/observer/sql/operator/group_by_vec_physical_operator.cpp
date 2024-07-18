/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sql/operator/group_by_vec_physical_operator.h"
#include "common/log/log.h"

GroupByVecPhysicalOperator::GroupByVecPhysicalOperator(
    std::vector<std::unique_ptr<Expression>> &&group_by_exprs, std::vector<Expression *> &&expressions)
    : group_by_exprs_(std::move(group_by_exprs)), ht_(expressions){
  call_ = false;
  aggregate_exprs_ = expressions;
  value_expressions_.reserve(aggregate_exprs_.size());
  for(auto expr : aggregate_exprs_) {
    auto       *aggregate_expr = static_cast<AggregateExpr *>(expr);
    Expression *child_expr     = aggregate_expr->child().get();
    ASSERT(child_expr != nullptr, "aggregate expression must have a child expression");
    value_expressions_.emplace_back(child_expr);
  }

}

RC GroupByVecPhysicalOperator::open(Trx *trx)
{
  // 打开下层算子
  ASSERT(children_.size() == 1, "group by operator only support one child, but got %d", children_.size());
  PhysicalOperator &child = *children_[0];
  RC rc = child.open(trx);
  if (OB_FAIL(rc)) {
    LOG_INFO("failed to open child operator. rc=%s", strrc(rc));
    return rc;
  }
  while ((rc = child.next(chunk_)) == RC::SUCCESS) {
    Chunk group_chunk;
    Chunk aggregate_chunk;
    // 从 chunk 中获取 group by 列和聚合列
    int col_id = 0;
    for (auto &expr : group_by_exprs_) {
      Column column;
      expr->get_column(chunk_, column);
      group_chunk.add_column(make_unique<Column>(column.attr_type(), column.attr_len()), col_id);
      auto col_data = column.data();
      group_chunk.column_ptr(col_id)->append(col_data, column.count());
      col_id++;
    }
    col_id = 0;
    for (auto &expr : value_expressions_) {
      Column column;
      expr->get_column(chunk_, column);
      aggregate_chunk.add_column(make_unique<Column>(column.attr_type(), column.attr_len()), col_id);
      auto col_data = column.data();
      aggregate_chunk.column_ptr(col_id)->append(col_data, column.count());
      col_id++;
    }

    // 将 group by 和聚合列添加到哈希表中
    rc = ht_.add_chunk(group_chunk, aggregate_chunk);
    if (OB_FAIL(rc)) {
      LOG_INFO("failed to add chunks. rc=%s", strrc(rc));
      return rc;
    }
  }
  if (rc == RC::RECORD_EOF) {
    rc = RC::SUCCESS;
  }
  return rc;
}

RC GroupByVecPhysicalOperator::next(Chunk &chunk)
{
  if (call_) {
    return RC::RECORD_EOF;
  }
  call_ = true;
  int col_id = 0;
  for(auto& group_expr : group_by_exprs_) {
    Column col;
    group_expr->get_column(chunk_, col);
    chunk.add_column(make_unique<Column>(col.attr_type(), col.attr_len()), col_id);
    col_id ++;
  }
  for(auto& aggrs_expr : value_expressions_) {
    Column col;
    aggrs_expr->get_column(chunk_, col);
    chunk.add_column(make_unique<Column>(col.attr_type(), col.attr_len()), col_id);
    col_id ++;
  }
  // 创建哈希表扫描器
  StandardAggregateHashTable::Scanner scanner_(&ht_);
  scanner_.open_scan();
  RC rc = scanner_.next(chunk);
  if (rc == RC::RECORD_EOF) {
    return RC::RECORD_EOF;
  }
  return RC::SUCCESS;
}

RC GroupByVecPhysicalOperator::close()
{
  RC rc = children_[0]->close();
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to close child operator: %s", strrc(rc));
    return rc;
  }
  call_ = false;
  return RC::SUCCESS;
}
