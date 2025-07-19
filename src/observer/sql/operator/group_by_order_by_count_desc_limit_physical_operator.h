#pragma once
#include "sql/expr/aggregate_hash_table.h"
#include "sql/operator/physical_operator.h"
#include "storage/common/chunk.h"
class GroupByOrderByCountDescVecPhysicalOperator : public PhysicalOperator
{
public:
  GroupByOrderByCountDescVecPhysicalOperator(
      vector<unique_ptr<Expression>> &&group_by_exprs, vector<Expression *> &&expressions, int n_);

  virtual ~GroupByOrderByCountDescVecPhysicalOperator() = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::GROUP_BY_VEC; }

  RC open(Trx *trx) override;
  RC next(Chunk &chunk) override;
  RC close() override;

private:
  vector<unique_ptr<Expression>>               group_by_exprs_;
  vector<Expression *>                         aggregate_expressions_;
  unique_ptr<ClickAggregateHashTableInretface> hash_table_;
  Chunk                                        output_chunk_;
  int                                          count_pos_;
  int                                          n_;
  bool                                         outputed_{false};
};