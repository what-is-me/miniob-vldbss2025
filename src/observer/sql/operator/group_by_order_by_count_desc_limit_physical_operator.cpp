#include "sql/operator/group_by_order_by_count_desc_limit_physical_operator.h"
#include "common/sys/rc.h"
#include "common/type/attr_type.h"
#include "sql/expr/aggregate_hash_table_specialized_v2.hpp"
#include "sql/expr/expression.h"
#include <cstdint>
#include <memory>
#include <string>
GroupByOrderByCountDescVecPhysicalOperator::GroupByOrderByCountDescVecPhysicalOperator(
    vector<unique_ptr<Expression>> &&group_by_exprs, vector<Expression *> &&expressions, int n)
    : group_by_exprs_(std::move(group_by_exprs)), aggregate_expressions_(std::move(expressions)), n_(n)
{
  for (int i = 0; i < aggregate_expressions_.size(); ++i) {
    if (aggregate_expressions_.at(i)->type() == ExprType::AGGREGATION &&
        static_cast<AggregateExpr *>(aggregate_expressions_.at(i))->aggregate_type() == AggregateExpr::Type::COUNT) {
      count_pos_ = i;
    }
  }
  for (int i = 0; i < group_by_exprs_.size(); ++i) {
    output_chunk_.add_column(
        std::make_unique<Column>(group_by_exprs_[i]->value_type(), group_by_exprs_[i]->value_length()), i);
  }
  for (int i = 0; i < aggregate_expressions_.size(); ++i) {
    output_chunk_.add_column(
        std::make_unique<Column>(aggregate_expressions_[i]->value_type(), aggregate_expressions_[i]->value_length()),
        group_by_exprs_.size() + i);
  }
  // TEXT, 1
  // BIGINT, 1
  if (group_by_exprs_.size() == 1 && aggregate_expressions_.size() == 1) {
    if (group_by_exprs_[0]->value_type() == AttrType::TEXTS) {
      hash_table_ = make_unique<clickbench::ClickAggregateHashTable<1, std::string>>(aggregate_expressions_);
    } else if (group_by_exprs_[0]->value_type() == AttrType::BIGINTS) {
      hash_table_ = make_unique<clickbench::ClickAggregateHashTable<1, int64_t>>(aggregate_expressions_);
    }
  }
  // INT, TEXT ,1
  // BIGINT, TEXT, 1
  // INT, BIGINT, 3
  // BIGINT, BIGINT, 3
  else if (group_by_exprs_.size() == 2) {
    if (aggregate_expressions_.size() == 1) {
      if (group_by_exprs_[0]->value_type() == AttrType::INTS && group_by_exprs_[1]->value_type() == AttrType::TEXTS) {
        hash_table_ = make_unique<clickbench::ClickAggregateHashTable<1, int, std::string>>(aggregate_expressions_);
      } else if (group_by_exprs_[0]->value_type() == AttrType::BIGINTS &&
                 group_by_exprs_[1]->value_type() == AttrType::TEXTS) {
        hash_table_ = make_unique<clickbench::ClickAggregateHashTable<1, int64_t, std::string>>(aggregate_expressions_);
      }
    } else if (aggregate_expressions_.size() == 3) {
      if (group_by_exprs_[0]->value_type() == AttrType::INTS && group_by_exprs_[1]->value_type() == AttrType::BIGINTS) {
        hash_table_ = make_unique<clickbench::ClickAggregateHashTable<3, int, int64_t>>(aggregate_expressions_);
      } else if (group_by_exprs_[0]->value_type() == AttrType::BIGINTS &&
                 group_by_exprs_[1]->value_type() == AttrType::BIGINTS) {
        hash_table_ = make_unique<clickbench::ClickAggregateHashTable<3, int64_t, int64_t>>(aggregate_expressions_);
      }
    }
  }
  // BIGINT,BIGINT,BIGINT,BIGINT, 1
  else if (group_by_exprs_.size() == 4 && aggregate_expressions_.size() == 1) {
    if (std::all_of(group_by_exprs_.begin(), group_by_exprs_.end(), [](const unique_ptr<Expression> &expr) {
          return expr->value_type() == AttrType::BIGINTS;
        })) {
      hash_table_ = make_unique<clickbench::ClickAggregateHashTable<1, int64_t, int64_t, int64_t, int64_t>>(
          aggregate_expressions_);
    }
  }
}

RC GroupByOrderByCountDescVecPhysicalOperator::open(Trx *trx)
{
  if (hash_table_ == nullptr) {
    return RC::UNIMPLEMENTED;
  }
  ASSERT(children_.size() == 1, "group by operator only support one child, but got %d", children_.size());
  PhysicalOperator &child = *children_[0];
  RC                rc    = child.open(trx);
  if (OB_FAIL(rc)) {
    LOG_INFO("failed to open child operator. rc=%s", strrc(rc));
    return rc;
  }
  Chunk chunk;
  while (OB_SUCC(rc = child.next(chunk))) {
    if (chunk.rows() == 0) {
      continue;
    }
    Chunk groups_chunk, aggrs_chunk;
    for (int i = 0; i < group_by_exprs_.size(); ++i) {
      std::unique_ptr<Column> column = std::make_unique<Column>();
      group_by_exprs_[i]->get_column(chunk, *column);
      groups_chunk.add_column(std::move(column), i);
    }
    for (int i = 0; i < aggregate_expressions_.size(); ++i) {
      std::unique_ptr<Column> column = std::make_unique<Column>();
      static_cast<AggregateExpr *>(aggregate_expressions_[i])->child()->get_column(chunk, *column);
      aggrs_chunk.add_column(std::move(column), i);
    }
    if (groups_chunk.rows() > 0) {
      rc = hash_table_->add_chunk(groups_chunk, aggrs_chunk);
      if (OB_FAIL(rc)) {
        LOG_INFO("failed to update aggregate state. rc=%s", strrc(rc));
        return rc;
      }
    }
    chunk.reset();
  }
  if (rc != RC::RECORD_EOF) {
    LOG_INFO("failed to update aggregate state. rc=%s", strrc(rc));
    return rc;
  }
  hash_table_->next(output_chunk_, count_pos_, n_);
  return RC::SUCCESS;
}
RC GroupByOrderByCountDescVecPhysicalOperator::next(Chunk &chunk)
{
  if (output_chunk_.rows() == 0 || outputed_) {
    return RC::RECORD_EOF;
  }
  outputed_ = true;
  return chunk.reference(output_chunk_);
}
RC GroupByOrderByCountDescVecPhysicalOperator::close() { return children().front()->close(); }