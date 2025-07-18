#pragma once

#include "common/lang/vector.h"
#include "common/lang/unordered_map.h"
#include "common/sys/rc.h"
#include "sql/expr/aggregate_hash_table.h"
#include "sql/expr/expression.h"
#include <cstring>
#include "sql/expr/aggregate_state.h"
template <int NKey, int NAggr>
class Int64KeyAggregateHashTable : public AggregateHashTable
{
private:
  struct ArrayHash
  {
    size_t operator()(const std::array<int64_t, NKey> &arr) const
    {
      size_t hash_val = 0;
#pragma unroll
      for (int i = 0; i < NKey; ++i) {
        hash_val ^= static_cast<size_t>(arr[i]);
        hash_val *= static_cast<size_t>(1099511628211ULL);  // copy from libstdc++
      }
      return hash_val;
    }
  };
  struct CommonState
  {
  public:
    void       *get() { return reinterpret_cast<void *>(data_.data()); }
    const void *get() const { return reinterpret_cast<const void *>(data_.data()); }

  private:
    inline static constexpr size_t MAX_STATE_SIZE =
        std::max(std::max(sizeof(SumState<int64_t>), sizeof(CountState<int64_t>)), sizeof(AvgState<int64_t>));
    std::array<char, MAX_STATE_SIZE> data_{};
  };

public:
  using Int64KeyHashTable = unordered_map<std::array<int64_t, NKey>, std::array<CommonState, NAggr>, ArrayHash>;
  class Scanner : public AggregateHashTable::Scanner
  {
  public:
    explicit Scanner(AggregateHashTable *hash_table) : AggregateHashTable::Scanner(hash_table) {}
    ~Scanner() = default;

    void open_scan() override
    {
      it_  = static_cast<Int64KeyAggregateHashTable *>(hash_table_)->begin();
      end_ = static_cast<Int64KeyAggregateHashTable *>(hash_table_)->end();
    }

    RC next(Chunk &output_chunk) override
    {
      RC rc = RC::SUCCESS;
      if (it_ == end_) {
        return RC::RECORD_EOF;
      }
      while (it_ != end_ && output_chunk.rows() < output_chunk.capacity()) {
        const std::array<int64_t, NKey> &group_by_values = it_->first;
        std::array<CommonState, NAggr>  &aggrs           = it_->second;
        for (int i = 0; i < output_chunk.column_num(); i++) {
          auto col_idx = output_chunk.column_ids(i);
          if (col_idx >= NKey) {
            int aggr_real_idx = col_idx - NKey;
            rc                = finialize_aggregate_state(aggrs[aggr_real_idx].get(),
                hash_table_->aggr_types_[aggr_real_idx],
                hash_table_->aggr_child_types_[aggr_real_idx],
                output_chunk.column(i));
            if (OB_FAIL(rc)) {
              LOG_WARN("finialize aggregate state failed");
              return rc;
            }
          } else {
            if (OB_FAIL(output_chunk.column(i).append_value(Value(group_by_values[col_idx])))) {
              LOG_WARN("append value failed");
              return rc;
            }
          }
        }
        it_++;
      }
      if (it_ == end_) {
        return RC::SUCCESS;
      }
      return RC::SUCCESS;
    }

  private:
    typename Int64KeyHashTable::iterator end_;
    typename Int64KeyHashTable::iterator it_;
  };
  Int64KeyAggregateHashTable(const vector<Expression *> aggregations)
  {
    for (auto &expr : aggregations) {
      ASSERT(expr->type() == ExprType::AGGREGATION, "expect aggregate expression");
      auto *aggregation_expr = static_cast<AggregateExpr *>(expr);
      aggr_types_.push_back(aggregation_expr->aggregate_type());
      aggr_child_types_.push_back(aggregation_expr->value_type());
    }
  }
  virtual ~Int64KeyAggregateHashTable() = default;
  RC add_chunk(Chunk &groups_chunk, Chunk &aggrs_chunk) override
  {
    if (groups_chunk.rows() != aggrs_chunk.rows()) {
      [[unlikely]] LOG_WARN("groups_chunk and aggrs_chunk have different rows: %d, %d", groups_chunk.rows(), aggrs_chunk.rows());
      return RC::INVALID_ARGUMENT;
    }
    for (int i = 0; i < groups_chunk.rows(); i++) {
      std::array<int64_t, NKey> group_by_values;
      for (int j = 0; j < groups_chunk.column_num(); ++j) {
        group_by_values[j] = groups_chunk.get_value(j, i).get_bigint();
      }
      auto &aggr = aggr_values_[group_by_values];
      for (size_t aggr_idx = 0; aggr_idx < aggr.size(); aggr_idx++) {
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

  typename Int64KeyHashTable::iterator begin() { return aggr_values_.begin(); }
  typename Int64KeyHashTable::iterator end() { return aggr_values_.end(); }
  Int64KeyHashTable           aggr_values_;
};
template class Int64KeyAggregateHashTable<2, 3>;