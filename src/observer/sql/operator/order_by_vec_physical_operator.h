#pragma once

#include "common/order_by_key.hpp"
#include "sql/operator/physical_operator.h"
#include "storage/common/chunk.h"

class OrderByVecPhysicalOperator : public PhysicalOperator
{
public:
  OrderByVecPhysicalOperator(vector<unique_ptr<Expression>> &&order_by_exprs, vector<bool> &&asc);
  virtual ~OrderByVecPhysicalOperator() = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::ORDER_BY_VEC; }

  RC open(Trx *trx) override;
  RC next(Chunk &chunk) override;
  RC close() override;

  RC tuple_schema(TupleSchema &schema) const override { return children_.front()->tuple_schema(schema); }

private:
  vector<unique_ptr<Expression>> order_by_exprs_;
  vector<bool>                   asc_;
  __order_by::Rows               rows_;
  Chunk chunk_;
};