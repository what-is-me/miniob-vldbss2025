#pragma once
#include "common/sys/rc.h"
#include "common/value.h"
#include "storage/common/chunk.h"
#include "storage/common/column.h"
#include <queue>
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
    row.first.emplace_back(chunk.get_value(i, rid));
  }
  for (int i = 0; i < order_key_cols.size(); ++i) {
    row.second.emplace_back(order_key_cols.at(i)->get_value(rid));
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

namespace __order_by_count {
using Row = std::pair<int, std::vector<Value>>;
struct Comp
{
  bool operator()(const Row &a, const Row &b) const { return a.first > b.first; }
};
using RowHeap = std::priority_queue<Row, std::vector<Row>, Comp>;
inline void append_rows_limit(RowHeap &rows, Chunk &chunk, Column &count_star, int limitation)
{
  for (int rid = 0; rid < chunk.rows(); ++rid) {
    Row row;
    row.second.reserve(chunk.rows());
    for (int i = 0; i < chunk.column_num(); ++i) {
      row.second.emplace_back(chunk.get_value(i, rid));
      row.first = *count_star.data_at<int>(rid);
    }
    rows.push(std::move(row));
    if (rows.size() > limitation) {
      rows.pop();
    }
  }
}
inline void append_chunk(Chunk &chunk, RowHeap &row_heap)
{
  std::vector<std::vector<Value>> stk;
  stk.reserve(row_heap.size());
  while (!row_heap.empty()) {
    stk.emplace_back(std::move(row_heap.top().second));
    row_heap.pop();
  }
  for (auto iter = stk.rbegin(); iter != stk.rend(); ++iter) {
    for (int i = 0; i < chunk.column_num(); ++i) {
      chunk.column(i).append_value(std::move(iter->at(i)));
    }
  }
}
}  // namespace __order_by_count