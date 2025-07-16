#pragma once
#include "common/sys/rc.h"
#include "common/value.h"
#include "storage/common/chunk.h"
#include "storage/common/column.h"
#include <cstdint>
#include <queue>
#include <string>
#include <variant>
#include <vector>
namespace __order_by {
using OrderKey = std::vector<Value>;
class OrderKeyComp
{
public:
  OrderKeyComp(const std::vector<bool> &ascs)
  {
    for (bool asc : ascs) {
      if (asc) {
        comps_.push_back(+[](const Value &a, const Value &b) { return a.compare(b); });
      } else {
        comps_.push_back(+[](const Value &a, const Value &b) { return b.compare(a); });
      }
    }
  }
  bool operator()(const OrderKey &a, const OrderKey &b) const
  {
    for (int i = 0; i < comps_.size(); ++i) {
      int comp = comps_.at(i)(a.at(i), b.at(i));
      if (comp < 0) {
        return true;
      }
      if (comp > 0) {
        return false;
      }
    }
    return false;
  }

  template <typename T>
  bool operator()(const std::pair<T, OrderKey> &a, const std::pair<T, OrderKey> &b) const
  {
    return operator()(a.second, b.second);
  }

private:
  std::vector<int (*)(const Value &, const Value &)> comps_;
};

using Row  = std::pair<std::vector<Value>, OrderKey>;
using Rows = std::vector<std::pair<std::vector<Value>, OrderKey>>;
using RowHeap =
    std::priority_queue<std::pair<std::vector<Value>, OrderKey>, Rows, OrderKeyComp>;  // 大根堆，从大到小pop

inline Row fetch_row(int rid, Chunk &chunk, std::vector<std::unique_ptr<Column>> &order_key_cols)
{
  Row row;
  row.first.reserve(chunk.column_num());
  row.second.reserve(order_key_cols.size());
  for (int i = 0; i < chunk.column_num(); ++i) {
    const Value value = chunk.get_value(i, rid);
    row.first.emplace_back(value);
  }
  for (int i = 0; i < order_key_cols.size(); ++i) {
    const Value value = order_key_cols.at(i)->get_value(rid);
    row.second.emplace_back(value);
  }
  return row;
}

inline void append_rows(Rows &rows, Chunk &chunk, std::vector<std::unique_ptr<Column>> &order_key_cols)
{
  for (int rid = 0; rid < chunk.rows(); ++rid) {
    rows.emplace_back(fetch_row(rid, chunk, order_key_cols));
  }
}

inline void sort_rows(Rows &rows, const OrderKeyComp &cmp) { std::sort(rows.begin(), rows.end(), cmp); }

inline void append_rows_limit(
    RowHeap &rows, Chunk &chunk, std::vector<std::unique_ptr<Column>> &order_key_cols, int limitation)
{
  for (int rid = 0; rid < chunk.rows(); ++rid) {
    rows.push(fetch_row(rid, chunk, order_key_cols));
    if (rows.size() > limitation) {
      rows.pop();
    }
  }
}

inline RC append_chunk(Chunk &chunk, std::vector<Value> &&row)
{
  for (int i = 0; i < chunk.column_num(); ++i) {
    RC rc = chunk.column(i).append_value(std::move(row.at(i)));
    if (OB_FAIL(rc)) {
      return rc;
    }
  }
  return RC::SUCCESS;
}

}  // namespace __order_by