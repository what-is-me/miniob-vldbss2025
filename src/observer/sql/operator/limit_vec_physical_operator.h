#pragma once

#include "sql/operator/physical_operator.h"

class LimitVecPhysicalOperator : public PhysicalOperator
{
public:
  LimitVecPhysicalOperator(int n) : n_(n) {}
  virtual ~LimitVecPhysicalOperator() = default;
  PhysicalOperatorType type() const override { return PhysicalOperatorType::LIMIT_VEC; }

  RC open(Trx *trx) override;
  RC next(Chunk &chunk) override;
  RC close() override;

private:
  int               n_;
  PhysicalOperator *child{nullptr};
};