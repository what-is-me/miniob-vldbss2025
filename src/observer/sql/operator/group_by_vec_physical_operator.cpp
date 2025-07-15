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
#include "common/sys/rc.h"
#include "common/type/attr_type.h"
#include "sql/expr/expression.h"
#include "storage/common/column.h"
#include <memory>
GroupByVecPhysicalOperator::GroupByVecPhysicalOperator(
    vector<unique_ptr<Expression>> &&group_by_exprs, vector<Expression *> &&expressions)
    : group_by_exprs_(std::move(group_by_exprs)), aggregate_expressions_(std::move(expressions))
{
#ifdef USE_SIMD
  if (aggregate_expressions_.size() == 1 &&
      reinterpret_cast<AggregateExpr *>(aggregate_expressions_.front())->aggregate_type() == AggregateExpr::Type::SUM &&
      group_by_exprs_.size() == 1 &&
      (group_by_exprs_.front()->value_type() == AttrType::INTS ||
          (group_by_exprs_.front()->value_type() == AttrType::CHARS && group_by_exprs_.front()->value_length() <= 4))) {
    need_encode_ =
        group_by_exprs_.front()->value_type() == AttrType::CHARS && group_by_exprs_.front()->value_length() <= 4;
    switch (aggregate_expressions_.front()->value_type()) {
      case AttrType::INTS: {
        hash_table_         = std::make_unique<LinearProbingAggregateHashTable<int>>(AggregateExpr::Type::SUM);
        hash_table_scanner_ = std::make_unique<LinearProbingAggregateHashTable<int>::Scanner>(hash_table_.get());
      } break;
      case AttrType::FLOATS: {
        hash_table_         = std::make_unique<LinearProbingAggregateHashTable<float>>(AggregateExpr::Type::SUM);
        hash_table_scanner_ = std::make_unique<LinearProbingAggregateHashTable<float>::Scanner>(hash_table_.get());
      } break;
      default: {
        hash_table_         = std::make_unique<StandardAggregateHashTable>(aggregate_expressions_);
        hash_table_scanner_ = std::make_unique<StandardAggregateHashTable::Scanner>(hash_table_.get());
      }
    }
    output_chunk_.add_column(
        std::make_unique<Column>(group_by_exprs_.front()->value_type(), group_by_exprs_.front()->value_length()), 0);
    output_chunk_.add_column(std::make_unique<Column>(aggregate_expressions_.front()->value_type(),
                                 aggregate_expressions_.front()->value_length()),
        1);
  } else {
    hash_table_         = std::make_unique<StandardAggregateHashTable>(aggregate_expressions_);
    hash_table_scanner_ = std::make_unique<StandardAggregateHashTable::Scanner>(hash_table_.get());
    for (int i = 0; i < group_by_exprs_.size(); ++i) {
      output_chunk_.add_column(
          std::make_unique<Column>(group_by_exprs_[i]->value_type(), group_by_exprs_[i]->value_length()), i);
    }
    for (int i = 0; i < aggregate_expressions_.size(); ++i) {
      output_chunk_.add_column(
          std::make_unique<Column>(aggregate_expressions_[i]->value_type(), aggregate_expressions_[i]->value_length()),
          group_by_exprs_.size() + i);
    }
  }
#else
  hash_table_         = std::make_unique<StandardAggregateHashTable>(aggregate_expressions_);
  hash_table_scanner_ = std::make_unique<StandardAggregateHashTable::Scanner>(hash_table_.get());
  for (int i = 0; i < group_by_exprs_.size(); ++i) {
    output_chunk_.add_column(
        std::make_unique<Column>(group_by_exprs_[i]->value_type(), group_by_exprs_[i]->value_length()), i);
  }
  for (int i = 0; i < aggregate_expressions_.size(); ++i) {
    output_chunk_.add_column(
        std::make_unique<Column>(aggregate_expressions_[i]->value_type(), aggregate_expressions_[i]->value_length()),
        group_by_exprs_.size() + i);
  }
#endif
}

static std::unique_ptr<Column> encode(const Column &column)
{
  std::unique_ptr<Column> res      = std::make_unique<Column>(AttrType::INTS, sizeof(int));
  const int               rows     = column.count();
  const char             *data     = column.data();
  const int               attr_len = column.attr_len();
  for (int i = 0; i < rows; ++i) {
    const char *str    = data + i * attr_len;
    char        val[4] = {0, 0, 0, 0};
    memcpy(val, str, attr_len);
    res->append_one(val);
  }
  return res;
}

// for debug
[[maybe_unused]] static void output_column(const Column &column)
{
  for (int i = 0; i < column.count(); ++i) {
    std::cout << column.get_value(i).to_string() << ", ";
  }
  std::cout << std::endl;
}

RC GroupByVecPhysicalOperator::open(Trx *trx)
{
  ASSERT(children_.size() == 1, "group by operator only support one child, but got %d", children_.size());
  PhysicalOperator &child = *children_[0];
  RC                rc    = child.open(trx);
  if (OB_FAIL(rc)) {
    LOG_INFO("failed to open child operator. rc=%s", strrc(rc));
    return rc;
  }
  Chunk chunk;
  while (OB_SUCC(rc = child.next(chunk))) {
    Chunk groups_chunk, aggrs_chunk;
    for (int i = 0; i < group_by_exprs_.size(); ++i) {
      std::unique_ptr<Column> column = std::make_unique<Column>();
      group_by_exprs_[i]->get_column(chunk, *column);
      // output_column(*column);
      if (need_encode_) {
        column = encode(*column);
        // output_column(*column);
      }
      groups_chunk.add_column(std::move(column), i);
    }
    for (int i = 0; i < aggregate_expressions_.size(); ++i) {
      std::unique_ptr<Column> column = std::make_unique<Column>();
      static_cast<AggregateExpr *>(aggregate_expressions_[i])->child()->get_column(chunk, *column);
      // output_column(*column);
      aggrs_chunk.add_column(std::move(column), i);
    }
    rc = hash_table_->add_chunk(groups_chunk, aggrs_chunk);
    if (OB_FAIL(rc)) {
      LOG_INFO("failed to update aggregate state. rc=%s", strrc(rc));
      return rc;
    }
  }
  if (rc != RC::RECORD_EOF) {
    LOG_INFO("failed to update aggregate state. rc=%s", strrc(rc));
    return rc;
  }
  hash_table_scanner_->open_scan();
  return RC::SUCCESS;
}

RC GroupByVecPhysicalOperator::next(Chunk &chunk)
{
  chunk.reset();
  if (need_encode_) {
    chunk.add_column(std::make_unique<Column>(AttrType::INTS, sizeof(int)), 0);
    chunk.add_column(
        std::make_unique<Column>(output_chunk_.column(1).attr_type(), output_chunk_.column(1).attr_len()), 1);
    RC rc = hash_table_scanner_->next(chunk);
    chunk.column(0).set_attr_type(AttrType::CHARS);
    return rc;
  }
  for (int i = 0; i < output_chunk_.column_num(); ++i) {
    chunk.add_column(std::make_unique<Column>(output_chunk_.column(i).attr_type(), output_chunk_.column(i).attr_len()),
        output_chunk_.column_ids(i));
  }
  return hash_table_scanner_->next(chunk);
}

RC GroupByVecPhysicalOperator::close()
{
  hash_table_scanner_->close_scan();
  return RC::SUCCESS;
}