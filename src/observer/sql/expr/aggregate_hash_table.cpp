/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sql/expr/aggregate_hash_table.h"
#include "common/math/simd_util.h"
#include "sql/expr/aggregate_state.h"
#include <immintrin.h>

// ----------------------------------StandardAggregateHashTable------------------

RC StandardAggregateHashTable::add_chunk(Chunk &groups_chunk, Chunk &aggrs_chunk)
{
  if (groups_chunk.rows() != aggrs_chunk.rows()) {
    LOG_WARN("groups_chunk and aggrs_chunk have different rows: %d, %d", groups_chunk.rows(), aggrs_chunk.rows());
    return RC::INVALID_ARGUMENT;
  }
  for (int i = 0; i < groups_chunk.rows(); i++) {
    vector<Value>  group_by_values;
    vector<void *> aggr_values;

    for (int j = 0; j < groups_chunk.column_num(); j++) {
      group_by_values.emplace_back(groups_chunk.get_value(j, i));
    }

    auto it = aggr_values_.find(group_by_values);
    if (it == aggr_values_.end()) {
      for (size_t j = 0; j < aggr_types_.size(); j++) {
        void *state_ptr = create_aggregate_state(aggr_types_[j], aggr_child_types_[j]);
        if (state_ptr == nullptr) {
          LOG_WARN("create aggregate state failed");
          return RC::INTERNAL;
        }
        aggr_values.emplace_back(state_ptr);
      }
      aggr_values_.emplace(group_by_values, aggr_values);
    }
    auto &aggr = aggr_values_.find(group_by_values)->second;
    for (size_t aggr_idx = 0; aggr_idx < aggr.size(); aggr_idx++) {
      RC rc = aggregate_state_update_by_value(
          aggr[aggr_idx], aggr_types_[aggr_idx], aggr_child_types_[aggr_idx], aggrs_chunk.get_value(aggr_idx, i));
      if (rc != RC::SUCCESS) {
        LOG_WARN("update aggregate state failed");
        return rc;
      }
    }
  }
  return RC::SUCCESS;
}

void StandardAggregateHashTable::Scanner::open_scan()
{
  it_  = static_cast<StandardAggregateHashTable *>(hash_table_)->begin();
  end_ = static_cast<StandardAggregateHashTable *>(hash_table_)->end();
}

RC StandardAggregateHashTable::Scanner::next(Chunk &output_chunk)
{
  RC rc = RC::SUCCESS;
  if (it_ == end_) {
    return RC::RECORD_EOF;
  }
  while (it_ != end_ && output_chunk.rows() < output_chunk.capacity()) {
    auto &group_by_values = it_->first;
    auto &aggrs           = it_->second;
    for (int i = 0; i < output_chunk.column_num(); i++) {
      auto col_idx = output_chunk.column_ids(i);
      if (col_idx >= static_cast<int>(group_by_values.size())) {
        int aggr_real_idx = col_idx - group_by_values.size();
        rc                = finialize_aggregate_state(aggrs[aggr_real_idx],
            hash_table_->aggr_types_[aggr_real_idx],
            hash_table_->aggr_child_types_[aggr_real_idx],
            output_chunk.column(i));
        if (rc != RC::SUCCESS) {
          LOG_WARN("finialize aggregate state failed");
          return rc;
        }
      } else {
        if (OB_FAIL(output_chunk.column(i).append_value(group_by_values[col_idx]))) {
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

size_t StandardAggregateHashTable::VectorHash::operator()(const vector<Value> &vec) const
{
  size_t hash_val = 0;
  for (const auto &elem : vec) {
    hash_val ^= hash<string>()(elem.to_string());
  }
  return hash_val;
}

bool StandardAggregateHashTable::VectorEqual::operator()(const vector<Value> &lhs, const vector<Value> &rhs) const
{
  if (lhs.size() != rhs.size()) {
    return false;
  }
  for (size_t i = 0; i < lhs.size(); ++i) {
    if (rhs[i].compare(lhs[i]) != 0) {
      return false;
    }
  }
  return true;
}

// ----------------------------------LinearProbingAggregateHashTable------------------
#ifdef USE_SIMD
template <typename V>
RC LinearProbingAggregateHashTable<V>::add_chunk(Chunk &group_chunk, Chunk &aggr_chunk)
{
  if (group_chunk.column_num() != 1 || aggr_chunk.column_num() != 1) {
    LOG_WARN("group_chunk and aggr_chunk size must be 1.");
    return RC::INVALID_ARGUMENT;
  }
  if (group_chunk.rows() != aggr_chunk.rows()) {
    LOG_WARN("group_chunk and aggr _chunk rows must be equal.");
    return RC::INVALID_ARGUMENT;
  }
  add_batch((int *)group_chunk.column(0).data(), (V *)aggr_chunk.column(0).data(), group_chunk.rows());
  return RC::SUCCESS;
}

template <typename V>
void LinearProbingAggregateHashTable<V>::Scanner::open_scan()
{
  capacity_   = static_cast<LinearProbingAggregateHashTable *>(hash_table_)->capacity();
  size_       = static_cast<LinearProbingAggregateHashTable *>(hash_table_)->size();
  scan_pos_   = 0;
  scan_count_ = 0;
}

template <typename V>
RC LinearProbingAggregateHashTable<V>::Scanner::next(Chunk &output_chunk)
{
  if (scan_pos_ >= capacity_ || scan_count_ >= size_) {
    return RC::RECORD_EOF;
  }
  auto linear_probing_hash_table = static_cast<LinearProbingAggregateHashTable *>(hash_table_);
  while (scan_pos_ < capacity_ && scan_count_ < size_ && output_chunk.rows() <= output_chunk.capacity()) {
    int key;
    V   value;
    RC  rc = linear_probing_hash_table->iter_get(scan_pos_, key, value);
    if (rc == RC::SUCCESS) {
      output_chunk.column(0).append_one((char *)&key);
      output_chunk.column(1).append_one((char *)&value);
      scan_count_++;
    }
    scan_pos_++;
  }
  return RC::SUCCESS;
}

template <typename V>
void LinearProbingAggregateHashTable<V>::Scanner::close_scan()
{
  capacity_   = -1;
  size_       = -1;
  scan_pos_   = -1;
  scan_count_ = 0;
}

template <typename V>
RC LinearProbingAggregateHashTable<V>::get(int key, V &value)
{
  RC  rc          = RC::SUCCESS;
  int index       = (key % capacity_ + capacity_) % capacity_;
  int iterate_cnt = 0;
  while (true) {
    if (keys_[index] == EMPTY_KEY) {
      rc = RC::NOT_EXIST;
      break;
    } else if (keys_[index] == key) {
      value = values_[index];
      break;
    } else {
      index += 1;
      index %= capacity_;
      iterate_cnt++;
      if (iterate_cnt > capacity_) {
        rc = RC::NOT_EXIST;
        break;
      }
    }
  }
  return rc;
}

template <typename V>
RC LinearProbingAggregateHashTable<V>::iter_get(int pos, int &key, V &value)
{
  RC rc = RC::SUCCESS;
  if (keys_[pos] == LinearProbingAggregateHashTable<V>::EMPTY_KEY) {
    rc = RC::NOT_EXIST;
  } else {
    key   = keys_[pos];
    value = values_[pos];
  }
  return rc;
}

template <typename V>
void LinearProbingAggregateHashTable<V>::aggregate(V *value, V value_to_aggregate)
{
  if (aggregate_type_ == AggregateExpr::Type::SUM) {
    *value += value_to_aggregate;
  } else {
    ASSERT(false, "unsupported aggregate type");
  }
}

template <typename V>
void LinearProbingAggregateHashTable<V>::resize()
{
  capacity_ *= 2;
  vector<int> new_keys(capacity_);
  vector<V>   new_values(capacity_);

  for (size_t i = 0; i < keys_.size(); i++) {
    auto &key   = keys_[i];
    auto &value = values_[i];
    if (key != EMPTY_KEY) {
      int index = (key % capacity_ + capacity_) % capacity_;
      while (new_keys[index] != EMPTY_KEY) {
        index = (index + 1) % capacity_;
      }
      new_keys[index]   = key;
      new_values[index] = value;
    }
  }

  keys_   = std::move(new_keys);
  values_ = std::move(new_values);
}

template <typename V>
void LinearProbingAggregateHashTable<V>::resize_if_need()
{
  if (size_ >= capacity_ / 2) {
    resize();
  }
}

namespace __simd_detail {
inline int       &vec_at(__m256i &vec, int idx) { return reinterpret_cast<int *>(&vec)[idx]; }
inline const int &vec_at(const __m256i &vec, int idx) { return reinterpret_cast<const int *>(&vec)[idx]; }
template <typename V>
inline void selective_load(V *memory, int offset, V *vec, __mmask8 inv)
{
#pragma unroll
  for (int i = 0; i < SIMD_WIDTH; i++) {
    if ((inv >> i) & 1) {
      vec[i] = memory[offset++];
    }
  }
}
inline int     count_mask_bits(uint mask) { return _mm_popcnt_u32(mask); }
inline __m256i calc_hash(const __m256i &key, int capacity)
{
  __m256i ret;
#pragma unroll
  for (int i = 0; i < SIMD_WIDTH; i++) {
    vec_at(ret, i) = (vec_at(key, i) % capacity + capacity) % capacity;
  }
  return ret;
}
}  // namespace __simd_detail

template <typename V>
void LinearProbingAggregateHashTable<V>::add_batch(int *input_keys, V *input_values, int len)
{
  // inv (invalid) 表示是否有效，inv[i] = -1 表示有效，inv[i] = 0 表示无效。
  // key[SIMD_WIDTH],value[SIMD_WIDTH] 表示当前循环中处理的键值对。
  // off (offset) 表示线性探测冲突时的偏移量，key[i] 每次遇到冲突键，则off[i]++，如果key[i] 已经完成聚合，则off[i] = 0，
  // i = 0 表示selective load 的起始位置。
  // inv 全部初始化为 -1
  // off 全部初始化为 0

  // for (; i + SIMD_WIDTH <= len;) {
  // 1: 根据 `inv` 变量的值，从 `input_keys` 中 `selective load` `SIMD_WIDTH` 个不同的输入键值对。
  // 2. 计算 i += |inv|, `|inv|` 表示 `inv` 中有效的个数
  // 3. 计算 hash 值，
  // 4. 根据聚合类型（目前只支持 sum），在哈希表中更新聚合结果。如果本次循环，没有找到key[i]
  // 在哈希表中的位置，则不更新聚合结果。
  // 5. gather 操作，根据 hash 值将 keys_ 的 gather 结果写入 table_key 中。
  // 6. 更新 inv 和 off。如果本次循环key[i] 聚合完成，则inv[i]=-1，表示该位置在下次循环中读取新的键值对。
  // 如果本次循环 key[i] 未在哈希表中聚合完成（table_key[i] != key[i]），则inv[i] =
  // 0，表示该位置在下次循环中不需要读取新的键值对。 如果本次循环中，key[i]聚合完成，则off[i] 更新为
  // 0，表示线性探测偏移量为 0，key[i] 未完成聚合，则off[i]++,表示线性探测偏移量加 1。
  // }
  // 7. 通过标量线性探测，处理剩余键值对

  // resize_if_need();

  __mmask8 inv  = 0xFF;
  __m256i  keys = _mm256_undefined_si256();
  __m256i  off  = _mm256_set1_epi32(0);
  V        values[SIMD_WIDTH];
  int      i = 0;
  for (; i + SIMD_WIDTH <= len;) {
    __simd_detail::selective_load(input_keys, i, reinterpret_cast<int *>(&keys), inv);
    __simd_detail::selective_load(input_values, i, values, inv);
    i += __simd_detail::count_mask_bits(inv);
    inv            = 0;
    __m256i offset = _mm256_add_epi32(__simd_detail::calc_hash(keys, capacity_), off);
#pragma unroll
    for (int j = 0; j < SIMD_WIDTH; ++j) {
      const int hash_val = __simd_detail::vec_at(offset, j);
      const int key      = __simd_detail::vec_at(keys, j);
      const V   value    = values[j];
      if (keys_[hash_val] == key) {
        values_[hash_val] += value;
        __simd_detail::vec_at(off, j) = 0;
        inv |= 1 << j;
      } else if (keys_[hash_val] == EMPTY_KEY) {
        keys_[hash_val]               = key;
        values_[hash_val]             = value;
        __simd_detail::vec_at(off, j) = 0;
        inv |= 1 << j;
        ++size_;
      } else {
        ++__simd_detail::vec_at(off, j);
      }
    }
  }
  auto add_up = [this](const int key, const V value) {
    for (int offset = (key % capacity_ + capacity_) % capacity_;; ++offset) {
      if (offset == capacity_) {
        offset = 0;
      }
      if (keys_[offset] == key) {
        values_[offset] += value;
        return;
      }
      if (keys_[offset] == EMPTY_KEY) {
        keys_[offset]   = key;
        values_[offset] = value;
        ++size_;
        return;
      }
    }
  };
  for (int j = 0; j < SIMD_WIDTH; ++j) {
    if (((inv >> j) & 1) == 0) {
      add_up(__simd_detail::vec_at(keys, j), values[j]);
    }
  }
  for (; i < len; ++i) {
    add_up(input_keys[i], input_values[i]);
  }
  resize_if_need();
}

template <typename V>
const int LinearProbingAggregateHashTable<V>::EMPTY_KEY = 0xffffffff;
template <typename V>
const int LinearProbingAggregateHashTable<V>::DEFAULT_CAPACITY = 16384;

template class LinearProbingAggregateHashTable<int>;
template class LinearProbingAggregateHashTable<float>;
#endif
