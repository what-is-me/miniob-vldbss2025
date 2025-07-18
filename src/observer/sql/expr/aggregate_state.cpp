/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sql/expr/aggregate_state.h"
#include "common/value.h"
#include <cstdint>
#include <stdint.h>
#include <immintrin.h>

#ifdef USE_SIMD
#include "common/math/simd_util.h"
#endif

static inline __m256i mm256_loadu_si256(const void *addr) { return _mm256_loadu_si256((const __m256i_u *)addr); }

template <typename T>
void SumState<T>::update(const T *values, int size)
{
  for (int i = 0; i < size; ++i) {
    value += values[i];
  }
}

template <>
void SumState<int>::update(const int *values, int size)
{
  int align_size = size / 8 * 8;
  if (align_size != 0) {
    __m256i tmp = _mm256_set1_epi32(0);
    for (int i = 0; i < align_size; i += 8) {
      tmp = _mm256_add_epi32(tmp, mm256_loadu_si256(values + i));
    }
    for (int i = 0; i < 8; ++i) {
      value += reinterpret_cast<int *>(&tmp)[i];
    }
  }
  for (int i = align_size; i < size; ++i) {
    value += values[i];
  }
}

template <>
void SumState<int64_t>::update(const int64_t *values, int size)
{
  int align_size = size / 4 * 4;
  if (align_size != 0) {
    __m256i tmp = _mm256_set1_epi64x(0);
    for (int i = 0; i < align_size; i += 4) {
      tmp = _mm256_add_epi64(tmp, mm256_loadu_si256(values + i));
    }
    for (int i = 0; i < 4; ++i) {
      value += reinterpret_cast<int *>(&tmp)[i];
    }
  }
  for (int i = align_size; i < size; ++i) {
    value += values[i];
  }
}

template <class T>
void AvgState<T>::update(const T *values, int size)
{
  for (int i = 0; i < size; ++i) {
    value += values[i];
  }
  count += size;
}

template <>
void AvgState<int>::update(const int *values, int size)
{
  int align_size = size / 8 * 8;
  if (align_size != 0) {
    __m256i tmp = _mm256_set1_epi32(0);
    for (int i = 0; i < align_size; i += 8) {
      tmp = _mm256_add_epi32(tmp, mm256_loadu_si256(values + i));
    }
    for (int i = 0; i < 8; ++i) {
      value += reinterpret_cast<int *>(&tmp)[i];
    }
  }
  for (int i = align_size; i < size; ++i) {
    value += values[i];
  }
  count += size;
}

template <>
void AvgState<int64_t>::update(const int64_t *values, int size)
{
  int align_size = size / 4 * 4;
  if (align_size != 0) {
    __m256i tmp = _mm256_set1_epi64x(0);
    for (int i = 0; i < align_size; i += 4) {
      tmp = _mm256_add_epi64(tmp, mm256_loadu_si256(values + i));
    }
    for (int i = 0; i < 4; ++i) {
      value += reinterpret_cast<int *>(&tmp)[i];
    }
  }
  for (int i = align_size; i < size; ++i) {
    value += values[i];
  }
  count += size;
}

template <typename T>
void CountState<T>::update(const T *values, int size)
{
  value += size;
}

void *create_aggregate_state(AggregateExpr::Type aggr_type, AttrType attr_type)
{
  void *state_ptr = nullptr;
  if (aggr_type == AggregateExpr::Type::SUM) {
    if (attr_type == AttrType::INTS) {
      state_ptr = malloc(sizeof(SumState<int>));
      new (state_ptr) SumState<int>();
    } else if (attr_type == AttrType::FLOATS) {
      state_ptr = malloc(sizeof(SumState<float>));
      new (state_ptr) SumState<float>();
    } else if (attr_type == AttrType::BIGINTS) {
      state_ptr = malloc(sizeof(SumState<int64_t>));
      new (state_ptr) SumState<int64_t>();
    } else {
      LOG_WARN("unsupported aggregate value type");
    }
  } else if (aggr_type == AggregateExpr::Type::COUNT) {
    state_ptr = malloc(sizeof(CountState<int>));
    new (state_ptr) CountState<int>();
  } else if (aggr_type == AggregateExpr::Type::AVG) {
    if (attr_type == AttrType::INTS) {
      state_ptr = malloc(sizeof(AvgState<int>));
      new (state_ptr) AvgState<int>();
    } else if (attr_type == AttrType::FLOATS) {
      state_ptr = malloc(sizeof(AvgState<float>));
      new (state_ptr) AvgState<float>();
    } else if (attr_type == AttrType::BIGINTS) {
      state_ptr = malloc(sizeof(AvgState<int64_t>));
      new (state_ptr) AvgState<int64_t>();
    } else {
      LOG_WARN("unsupported aggregate value type");
    }
  } else {
    LOG_WARN("unsupported aggregator type");
  }
  return state_ptr;
}

RC aggregate_state_update_by_value(void *state, AggregateExpr::Type aggr_type, AttrType attr_type, const Value &val)
{
  RC rc = RC::SUCCESS;
  if (aggr_type == AggregateExpr::Type::SUM) {
    if (attr_type == AttrType::INTS) {
      static_cast<SumState<int> *>(state)->update(val.get_int());
    } else if (attr_type == AttrType::FLOATS) {
      static_cast<SumState<float> *>(state)->update(val.get_float());
    } else if (attr_type == AttrType::BIGINTS) {
      static_cast<SumState<int64_t> *>(state)->update(val.get_bigint());
    } else {
      LOG_WARN("unsupported aggregate value type");
      return RC::UNIMPLEMENTED;
    }
  } else if (aggr_type == AggregateExpr::Type::COUNT) {
    static_cast<CountState<int> *>(state)->update(1);
  } else if (aggr_type == AggregateExpr::Type::AVG) {
    if (attr_type == AttrType::INTS) {
      static_cast<AvgState<int> *>(state)->update(val.get_int());
    } else if (attr_type == AttrType::FLOATS) {
      static_cast<AvgState<float> *>(state)->update(val.get_float());
    } else if (attr_type == AttrType::BIGINTS) {
      static_cast<AvgState<int64_t> *>(state)->update(val.get_bigint());
    } else {
      LOG_WARN("unsupported aggregate value type");
      return RC::UNIMPLEMENTED;
    }
  } else {
    LOG_WARN("unsupported aggregator type");
    return RC::UNIMPLEMENTED;
  }
  return rc;
}

template <class STATE, typename T>
void append_to_column(void *state, Column &column)
{
  STATE *state_ptr = reinterpret_cast<STATE *>(state);
  T      res       = state_ptr->template finalize<T>();
  column.append_value(Value(res));
}

RC finialize_aggregate_state(void *state, AggregateExpr::Type aggr_type, AttrType attr_type, Column &col)
{
  RC rc = RC::SUCCESS;
  if (aggr_type == AggregateExpr::Type::SUM) {
    if (attr_type == AttrType::INTS) {
      append_to_column<SumState<int>, int>(state, col);
    } else if (attr_type == AttrType::FLOATS) {
      append_to_column<SumState<float>, float>(state, col);
    } else if (attr_type == AttrType::BIGINTS) {
      append_to_column<SumState<int64_t>, int64_t>(state, col);
    } else {
      rc = RC::UNIMPLEMENTED;
      LOG_WARN("unsupported aggregate value type");
    }
  } else if (aggr_type == AggregateExpr::Type::COUNT) {
    append_to_column<CountState<int>, int>(state, col);
  } else if (aggr_type == AggregateExpr::Type::AVG) {
    if (attr_type == AttrType::INTS) {
      append_to_column<AvgState<int>, float>(state, col);
    } else if (attr_type == AttrType::FLOATS) {
      append_to_column<AvgState<float>, float>(state, col);
    } else if (attr_type == AttrType::BIGINTS) {
      append_to_column<AvgState<int64_t>, float>(state, col);
    } else {
      rc = RC::UNIMPLEMENTED;
      LOG_WARN("unsupported aggregate value type");
    }  //
  } else {
    rc = RC::UNIMPLEMENTED;
    LOG_WARN("unsupported aggregator type");
  }
  return rc;
}

template <class STATE, typename T>
void update_aggregate_state(void *state, const Column &column)
{
  STATE *state_ptr = reinterpret_cast<STATE *>(state);
  T     *data      = (T *)column.data();
  state_ptr->update(data, column.count());
}

RC aggregate_state_update_by_column(void *state, AggregateExpr::Type aggr_type, AttrType attr_type, Column &col)
{
  RC rc = RC::SUCCESS;
  if (aggr_type == AggregateExpr::Type::SUM) {
    if (attr_type == AttrType::INTS) {
      update_aggregate_state<SumState<int>, int>(state, col);
    } else if (attr_type == AttrType::FLOATS) {
      update_aggregate_state<SumState<float>, float>(state, col);
    } else if (attr_type == AttrType::BIGINTS) {
      update_aggregate_state<SumState<int64_t>, int64_t>(state, col);
    } else {
      LOG_WARN("unsupported aggregate value type");
      rc = RC::UNIMPLEMENTED;
    }
  } else if (aggr_type == AggregateExpr::Type::COUNT) {
    update_aggregate_state<CountState<int>, int>(state, col);
  } else if (aggr_type == AggregateExpr::Type::AVG) {
    if (attr_type == AttrType::INTS) {
      update_aggregate_state<AvgState<int>, int>(state, col);
    } else if (attr_type == AttrType::FLOATS) {
      update_aggregate_state<AvgState<float>, float>(state, col);
    } else if (attr_type == AttrType::BIGINTS) {
      update_aggregate_state<AvgState<int64_t>, int64_t>(state, col);
    } else {
      LOG_WARN("unsupported aggregate value type");
      rc = RC::UNIMPLEMENTED;
    }
  } else {
    LOG_WARN("unsupported aggregator type");
    rc = RC::UNIMPLEMENTED;
  }
  return rc;
}

template class SumState<int>;
template class SumState<float>;

template class CountState<int>;

template class AvgState<int>;
template class AvgState<float>;
