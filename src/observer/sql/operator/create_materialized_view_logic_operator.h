#pragma once

#include "sql/operator/logical_operator.h"

class CreateMaterializedViewLogicalOperator : public LogicalOperator
{
public:
  CreateMaterializedViewLogicalOperator(string view_name) : view_name_(view_name) {}

  virtual ~CreateMaterializedViewLogicalOperator() override = default;

  LogicalOperatorType type() const override { return LogicalOperatorType::MATERIALIZED_VIEW_CREATE; }

  const string &view_name() const { return view_name_; }

private:
  string view_name_;
};