#include "sql/operator/order_by_vec_physical_operator.h"
#include "common/order_by_key.hpp"
#include "common/sys/rc.h"
#include "storage/common/column.h"
#include <memory>
#include <string>
#include <variant>
#include <vector>

OrderByVecPhysicalOperator::OrderByVecPhysicalOperator(
    vector<unique_ptr<Expression>> &&order_by_exprs, vector<bool> &&asc)
    : order_by_exprs_(std::move(order_by_exprs)), asc_(std::move(asc))
{}

RC OrderByVecPhysicalOperator::open(Trx *trx)
{
  ASSERT(children_.size() == 1, "order by operator only support one child, but got %d", children_.size());

  PhysicalOperator &child = *children_[0];
  RC                rc    = child.open(trx);
  if (OB_FAIL(rc)) {
    LOG_INFO("failed to open child operator. rc=%s", strrc(rc));
    return rc;
  }
  __order_by::OrderKeyComp comp(asc_);
  while (OB_SUCC(rc = child.next(chunk_))) {
    std::vector<std::unique_ptr<Column>> order_key_cols(order_by_exprs_.size());
    for (int i = 0; i < order_by_exprs_.size(); ++i) {
      order_key_cols.at(i) = std::make_unique<Column>();
      RC rc                = order_by_exprs_.at(i)->get_column(chunk_, *order_key_cols.at(i));
      if (OB_FAIL(rc)) {
        return rc;
      }
    }
    __order_by::append_rows(rows_, chunk_, order_key_cols);
  }
  if (rc != RC::RECORD_EOF) {
    LOG_INFO("failed to update aggregate state. rc=%s", strrc(rc));
    return rc;
  }
  __order_by::sort_rows(rows_, comp);
  return RC::SUCCESS;
}
RC OrderByVecPhysicalOperator::next(Chunk &chunk)
{
  chunk.reset();
  if (rows_.empty() || chunk_.capacity() == 0) {
    return RC::RECORD_EOF;
  }
  int total_rows = rows_.size();
  for (int i = 0; i < chunk_.column_num(); ++i) {
    Column &col = chunk_.column(i);
    chunk.add_column(make_unique<Column>(col.attr_type(), col.attr_len(), total_rows), chunk_.column_ids(i));
  }
  for (auto &&row : std::move(rows_)) {
    RC rc = __order_by::append_chunk(chunk, std::move(row.first));
    if (OB_FAIL(rc)) {
      return rc;
    }
  }
  rows_.clear();
  return RC::SUCCESS;
}
RC OrderByVecPhysicalOperator::close() { return children().at(0)->close(); }