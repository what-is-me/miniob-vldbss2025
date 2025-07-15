#include "sql/operator/limit_vec_physical_operator.h"
#include "common/sys/rc.h"

using namespace common;

RC LimitVecPhysicalOperator::open(Trx *trx)
{
  ASSERT(children_.size() == 1, "limit operator only support one child, but got %d", children_.size());

  child = children_[0].get();
  RC rc = child->open(trx);

  if (OB_FAIL(rc)) {
    LOG_INFO("failed to open child operator. rc=%s", strrc(rc));
    return rc;
  }

  return RC::SUCCESS;
}
RC LimitVecPhysicalOperator::next(Chunk &chunk)
{
  if (n_ == 0) {
    return RC::RECORD_EOF;
  }
  RC rc = child->next(chunk);
  if (OB_FAIL(rc)) {
    return rc;
  }
  if (n_ >= chunk.rows()) {
    n_ -= chunk.rows();
    return RC::SUCCESS;
  }
  for (int i = 0; i < chunk.column_num(); ++i) {
    Column &column = chunk.column(i);
    column.limit(n_);
  }
  n_ = 0;
  return RC::SUCCESS;
}
RC LimitVecPhysicalOperator::close() { return child->close(); }