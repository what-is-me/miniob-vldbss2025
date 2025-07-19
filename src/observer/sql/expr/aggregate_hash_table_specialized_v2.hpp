#pragma once
#include "common/type/string_t.h"
#include "sql/expr/aggregate_hash_table.h"
#include "sql/expr/aggregate_state.h"
#include "storage/common/chunk.h"
#include "storage/common/column.h"
#include <algorithm>
#include <array>
#include <cstdint>
#include <functional>
#include <queue>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>
namespace clickbench {
struct CommonState
{
public:
  void       *get() { return reinterpret_cast<void *>(data_.data()); }
  const void *get() const { return reinterpret_cast<const void *>(data_.data()); }
  int         get_count() { return reinterpret_cast<CountState<int> *>(data_.data())->finalize<int>(); }

private:
  inline static constexpr size_t MAX_STATE_SIZE =
      std::max(std::max(sizeof(SumState<int64_t>), sizeof(CountState<int64_t>)), sizeof(AvgState<int64_t>));
  std::array<char, MAX_STATE_SIZE> data_{};
};
template <typename... KeyTypes>
class TupleHash
{
public:
  size_t operator()(const std::tuple<KeyTypes...> &tp) const
  {
    size_t hash_val = 0;
    if constexpr (sizeof...(KeyTypes) > 0) {
      hash_val = hash_func(std::get<0>(tp));
    }
    if constexpr (sizeof...(KeyTypes) > 1) {
      hash_val *= static_cast<size_t>(1099511628211ULL);
      hash_val ^= hash_func(std::get<1>(tp));
    }
    if constexpr (sizeof...(KeyTypes) > 2) {
      hash_val *= static_cast<size_t>(1099511628211ULL);
      hash_val ^= hash_func(std::get<2>(tp));
    }
    if constexpr (sizeof...(KeyTypes) > 3) {
      hash_val *= static_cast<size_t>(1099511628211ULL);
      hash_val ^= hash_func(std::get<3>(tp));
    }
    return hash_val;
  }

private:
  size_t hash_func(const int &value) const { return static_cast<size_t>(value); }
  size_t hash_func(const int64_t &value) const { return static_cast<size_t>(value); }
  size_t hash_func(const std::string &value) const { return std::hash<std::string>()(value); }
};

template <size_t NAggr /*1,3*/, typename... KeyTypes>
class ClickAggregateHashTable : public ClickAggregateHashTableInretface
{
public:
  using ClickHashTable = unordered_map<std::tuple<KeyTypes...>, std::array<CommonState, NAggr>, TupleHash<KeyTypes...>>;
  ClickAggregateHashTable(const vector<Expression *> aggregations)
  {
    for (auto &expr : aggregations) {
      ASSERT(expr->type() == ExprType::AGGREGATION, "expect aggregate expression");
      auto *aggregation_expr = static_cast<AggregateExpr *>(expr);
      aggr_types_.push_back(aggregation_expr->aggregate_type());
      aggr_child_types_.push_back(aggregation_expr->value_type());
    }
  }
  virtual ~ClickAggregateHashTable() = default;

  RC add_chunk(Chunk &groups_chunk, Chunk &aggrs_chunk) override
  {
    if (groups_chunk.rows() != aggrs_chunk.rows()) {
      [[unlikely]] LOG_WARN("groups_chunk and aggrs_chunk have different rows: %d, %d", groups_chunk.rows(), aggrs_chunk.rows());
      return RC::INVALID_ARGUMENT;
    }
    std::tuple<KeyTypes...> group_by_values;
    for (int i = 0; i < groups_chunk.rows(); i++) {
      if constexpr (sizeof...(KeyTypes) > 0) {
        std::get<0>(group_by_values) = get_ceil<0>(groups_chunk, i);
      }
      if constexpr (sizeof...(KeyTypes) > 1) {
        std::get<1>(group_by_values) = get_ceil<1>(groups_chunk, i);
      }
      if constexpr (sizeof...(KeyTypes) > 2) {
        std::get<2>(group_by_values) = get_ceil<2>(groups_chunk, i);
      }
      if constexpr (sizeof...(KeyTypes) > 3) {
        std::get<3>(group_by_values) = get_ceil<3>(groups_chunk, i);
      }
      auto &aggr = aggr_values_[group_by_values];
#pragma unroll
      for (size_t aggr_idx = 0; aggr_idx < NAggr; aggr_idx++) {
        RC rc = aggregate_state_update_by_value(aggr[aggr_idx].get(),
            aggr_types_[aggr_idx],
            aggr_child_types_[aggr_idx],
            aggrs_chunk.get_value(aggr_idx, i));
        if (rc != RC::SUCCESS) {
          LOG_WARN("update aggregate state failed");
          return rc;
        }
      }
    }
    return RC::SUCCESS;
  }

private:
  template <size_t I>
  std::tuple_element_t<I, std::tuple<KeyTypes...>> get_ceil(Chunk &groups_chunk, int rid)
  {
    using ret_t    = std::tuple_element_t<I, std::tuple<KeyTypes...>>;
    Column &column = groups_chunk.column(I);
    if constexpr (std::is_same_v<ret_t, int>) {
      return *column.data_at<int>(rid);
    }
    if constexpr (std::is_same_v<ret_t, int64_t>) {
      return *column.data_at<int64_t>(rid);
    }
    if constexpr (std::is_same_v<ret_t, std::string>) {
      const auto *str = column.data_at<string_t>(rid);
      return std::string(str->data(), str->data() + str->size());
    }
    return {};
  }

  template <size_t I>
  void put_ceil(Chunk &chunk, int rid, const int &value)
  {
    *chunk.column(I).data_at<int>(rid) = value;
  }

  template <size_t I>
  void put_ceil(Chunk &chunk, int rid, const int64_t &value)
  {
    *chunk.column(I).data_at<int64_t>(rid) = value;
  }

  template <size_t I>
  void put_ceil(Chunk &chunk, int rid, const std::string &value)
  {
    Column &col                 = chunk.column(I);
    *col.data_at<string_t>(rid) = col.add_text(value.data(), value.length());
  }

  typename ClickHashTable::iterator begin() { return aggr_values_.begin(); }
  typename ClickHashTable::iterator end() { return aggr_values_.end(); }

private:
  using Row = std::pair<int, std::pair<std::tuple<KeyTypes...>, std::array<CommonState, NAggr>>>;
  struct RowCmp
  {
    bool operator()(const Row &a, const Row &b) const { return a.first > b.first; }
  };

public:
  RC next(Chunk &out_put_chunk, int count_pos, int n) override
  {
    std::priority_queue<Row, std::vector<Row>, RowCmp> row_heap;  // 小根堆
    for (auto iter = begin(); iter != end();) {
      int count_star = iter->second.at(count_pos).get_count();
      if (row_heap.size() < n) {
        row_heap.push(std::make_pair(count_star, std::make_pair(std::move(iter->first), std::move(iter->second))));
        iter = aggr_values_.erase(iter);
        continue;
      }
      if (count_star > row_heap.top().first) {
        row_heap.pop();
        row_heap.push(std::make_pair(count_star, std::make_pair(std::move(iter->first), std::move(iter->second))));
        iter = aggr_values_.erase(iter);
        continue;
      }
      ++iter;
    }
    for (int i = 0; i < sizeof...(KeyTypes); ++i) {
      out_put_chunk.column(i).resize(row_heap.size());
    }
    for (int rid = ((int)row_heap.size()) - 1; rid >= 0; rid--) {
      add_row(out_put_chunk, rid, row_heap.top());
      row_heap.pop();
    }
    for (int i = sizeof...(KeyTypes); i < out_put_chunk.column_num(); ++i) {
      Column &column = out_put_chunk.column(i);
      switch (column.attr_len()) {
        case 4: {
          auto *arr = reinterpret_cast<int *>(column.data());
          std::reverse(arr, arr + column.count());
        } break;
        case 8: {
          auto *arr = reinterpret_cast<int64_t *>(column.data());
          std::reverse(arr, arr + column.count());
        } break;
        default: return RC::UNIMPLEMENTED;
      }
    }
    return RC::SUCCESS;
  }

private:
  void add_row(Chunk &out_put_chunk, int rid, Row row)
  {
    if constexpr (sizeof...(KeyTypes) > 0) {
      put_ceil<0>(out_put_chunk, rid, std::get<0>(row.second.first));
    }
    if constexpr (sizeof...(KeyTypes) > 1) {
      put_ceil<1>(out_put_chunk, rid, std::get<1>(row.second.first));
    }
    if constexpr (sizeof...(KeyTypes) > 2) {
      put_ceil<2>(out_put_chunk, rid, std::get<2>(row.second.first));
    }
    if constexpr (sizeof...(KeyTypes) > 3) {
      put_ceil<3>(out_put_chunk, rid, std::get<3>(row.second.first));
    }
    const int aggr_begin_i = sizeof...(KeyTypes);
    for (int i = 0; i < NAggr; ++i) {
      finialize_aggregate_state(row.second.second[i].get(),
          aggr_types_[i],
          aggr_child_types_[i],
          out_put_chunk.column(i + aggr_begin_i));  // need reverse
    }
  }

private:
  /// group by values -> aggregate values
  ClickHashTable aggr_values_;
};
}  // namespace clickbench