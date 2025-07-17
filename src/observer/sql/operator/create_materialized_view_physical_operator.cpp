#include "sql/operator/create_materialized_view_physical_operator.h"
#include "common/sys/rc.h"
#include "common/type/attr_type.h"
#include "common/types.h"
#include "sql/expr/tuple.h"
#include "sql/parser/parse_defs.h"
#include "sql/stmt/create_table_stmt.h"
#include "storage/common/chunk.h"
#include "storage/table/table.h"

RC CreateMaterializedViewPhysicalOperator::open(Trx *trx)
{
  ASSERT(children_.size() == 1, "CreateMaterializedViewPhysicalOperator children size must be 1");
  PhysicalOperator &child = *children_[0];
  TupleSchema       schema;
  child.tuple_schema(schema);

  RC rc = child.open(trx);
  if (OB_FAIL(rc)) {
    return rc;
  }
  rc = child.next(chunk_);
  vector<AttrInfoSqlNode> attr_infos;
  switch (rc) {
    case RC::SUCCESS: {
      for (int i = 0; i < schema.cell_num(); ++i) {
        AttrInfoSqlNode attr_info;
        attr_info.name   = schema.cell_at(i).field_name();
        attr_info.type   = chunk_.column(i).attr_type();
        attr_info.length = chunk_.column(i).attr_len();
        attr_infos.emplace_back(attr_info);
      }
    } break;
    case RC::RECORD_EOF: {
      for (int i = 0; i < schema.cell_num(); ++i) {
        AttrInfoSqlNode attr_info;
        attr_info.name   = schema.cell_at(i).field_name();
        attr_info.type   = AttrType::INTS;
        attr_info.length = 4;
        attr_infos.emplace_back(attr_info);
      }
    } break;
    default: return rc;
  }
  db_->create_table(view_name_.c_str(), attr_infos, {}, StorageFormat::PAX_FORMAT);
  if(rc == RC::RECORD_EOF) {
    return RC::SUCCESS;
  }
  Table * table  = db_->find_table(view_name_.c_str());
  table->insert_chunk(chunk_);
  chunk_.reset();
  while(OB_SUCC(rc = child.next(chunk_))) {
    table->insert_chunk(chunk_);
    chunk_.reset();
  }
  if(rc == RC::RECORD_EOF) {
    return RC::SUCCESS;
  }
  return rc;
}
RC CreateMaterializedViewPhysicalOperator::next(Chunk &chunk) { return RC::RECORD_EOF; }
RC CreateMaterializedViewPhysicalOperator::close()
{
  if (!children_.empty()) {
    children_.front()->close();
  }
  return RC::SUCCESS;
};
