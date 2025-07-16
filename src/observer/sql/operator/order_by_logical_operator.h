#pragma once
#include "sql/operator/logical_operator.h"

class OrderByLogicalOperator : public LogicalOperator
{
public:
  OrderByLogicalOperator(vector<unique_ptr<Expression>> &&order_by_exprs, vector<bool> &&asc)
      : order_by_exprs_(std::move(order_by_exprs)), asc_(std::move(asc))
  {}
  virtual ~OrderByLogicalOperator() = default;

  LogicalOperatorType type() const override { return LogicalOperatorType::ORDER_BY; }
  OpType              get_op_type() const override { return OpType::LOGICALORDERBY; }
  auto               &order_by_exprs() { return order_by_exprs_; }
  auto               &asc() { return asc_; }

private:
  vector<unique_ptr<Expression>> order_by_exprs_;
  vector<bool>                   asc_;
};