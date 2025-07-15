#pragma once
#include "sql/operator/logical_operator.h"
#include "sql/operator/operator_node.h"
class LimitLogicalOperator : public LogicalOperator
{
public:
  LimitLogicalOperator(int n) : n_(n) {}
  virtual ~LimitLogicalOperator() = default;
  LogicalOperatorType type() const override { return LogicalOperatorType::LIMIT; }
  OpType              get_op_type() const override { return OpType::LOGICALLIMIT; }
  int                 n() const { return n_; }

private:
  int n_;
};