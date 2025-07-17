#pragma once

#include "common/log/log.h"
#include "sql/operator/physical_operator.h"
#include "storage/table/table.h"
#include <memory>

class CreateMaterializedViewPhysicalOperator : public PhysicalOperator
{
public:
  CreateMaterializedViewPhysicalOperator(string view, string original_table_name) { view_name_ = view; 
  original_table_name_ = original_table_name;};

  ~CreateMaterializedViewPhysicalOperator() = default;

  bool is_physical() const override { return true; }
  bool is_logical() const override { return false; }

  PhysicalOperatorType type() const override { return PhysicalOperatorType::CREATE_MATERIALIZED_VIEW; };

  RC open(Trx *trx) override
  {
    ASSERT(children_.size() == 1, "CreateMaterializedViewPhysicalOperator children size must be 1");
    PhysicalOperator &child = *children_[0];
    ASSERT(child.type() == PhysicalOperatorType::PROJECT_VEC, "CreateMaterializedViewPhysicalOperator children must be PROJECT_VEC");
    RC rc = child.open(trx);
    if (OB_FAIL(rc)) {
      LOG_INFO("failed to open child operator. rc=%s", strrc(rc));
      return rc;
    }
    return RC::SUCCESS;
  }
  RC next() override
  {
    LOG_ERROR("PAX format should be used!");
    return RC::UNIMPLEMENTED;
  }
  RC next(Chunk &chunk) override
  {
    if (children_.empty()) {
      return RC::RECORD_EOF;
    }
    chunk_.reset();
    RC rc = children_[0]->next(chunk_);
    if (rc == RC::RECORD_EOF) {
      return rc;
    } else if (rc == RC::SUCCESS) {
      rc = chunk.reference(chunk_);
    } else {
      LOG_WARN("failed to get next tuple: %s", strrc(rc));
      return rc;
    }
    LOG_TRACE("column: %d", chunk.column_num());
    chunk.table_name = view_name_;
    chunk.original_table_name = original_table_name_;
    return rc;
  }
  RC close() override
  {
    if (!children_.empty()) {
      children_[0]->close();
    }
    return RC::SUCCESS;
  };

  // virtual Tuple *current_tuple() { return nullptr; }

  RC tuple_schema(TupleSchema &schema) const override
  {
    LOG_INFO("create materialized view physical operator tuple schema");
    RC rc = children_[0]->tuple_schema(schema);
    return rc;
  }

  vector<unique_ptr<PhysicalOperator>> &children() { return children_; }

private:
  string view_name_;
  string original_table_name_;
  Chunk  chunk_;
  std::unique_ptr<Table> table;
};