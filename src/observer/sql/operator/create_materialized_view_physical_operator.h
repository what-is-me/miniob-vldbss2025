#pragma once

#include "common/log/log.h"
#include "sql/operator/physical_operator.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include <memory>

class CreateMaterializedViewPhysicalOperator : public PhysicalOperator
{
public:
  CreateMaterializedViewPhysicalOperator(string view, Db *db) : view_name_(std::move(view)), db_(db) {}

  virtual ~CreateMaterializedViewPhysicalOperator() = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::CREATE_MATERIALIZED_VIEW; };

  RC open(Trx *trx) override;
  RC next(Chunk &chunk) override;
  RC close() override;

private:
  string view_name_;
  Db    *db_;
  Chunk  chunk_;
};