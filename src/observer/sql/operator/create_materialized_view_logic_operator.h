#pragma once

#include "common/log/log.h"
#include "sql/operator/logical_operator.h"
#include "sql/stmt/create_materialized_view_stmt.h"

class CreateMaterializedViewLogicalOperator : public LogicalOperator
{
public:
  RC create_plan(
      CreateMaterializedViewStmt *create_materialized_view_stmt, unique_ptr<LogicalOperator> &logical_operator)
  {
    return RC::UNIMPLEMENTED;
  }
  CreateMaterializedViewLogicalOperator(string view_name) : view_name_(view_name) {}

  ~CreateMaterializedViewLogicalOperator() override = default;

  LogicalOperatorType type() const override { return LogicalOperatorType::MATERIALIZED_VIEW_CREATE; }

  const string &view_name() const { return view_name_; }

private:
  string view_name_;
};