/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "common/lang/comparator.h"
#include "common/lang/sstream.h"
#include "common/log/log.h"
#include "common/type/big_integer_type.h"
#include "common/value.h"
#include "storage/common/column.h"

int BigIntegerType::compare(const Value &left, const Value &right) const
{
  ASSERT(left.attr_type() == AttrType::BIGINTS, "left type is not biginteger");
  ASSERT(right.attr_type() == AttrType::BIGINTS || right.attr_type() == AttrType::FLOATS, "right type is not numeric");
  if (right.attr_type() == AttrType::BIGINTS) {
    return common::compare_int64((void *)&left.value_.int_value_, (void *)&right.value_.int_value_);
  } else if (right.attr_type() == AttrType::FLOATS) {
    float left_val  = left.get_float();
    float right_val = right.get_float();
    return common::compare_float((void *)&left_val, (void *)&right_val);
  }
  return INT32_MAX;
}

int BigIntegerType::compare(const Column &left, const Column &right, int left_idx, int right_idx) const
{
  ASSERT(left.attr_type() == AttrType::BIGINTS, "left type is not integer");
  ASSERT(right.attr_type() == AttrType::BIGINTS, "right type is not integer");
  return common::compare_int64((void *)&((int*)left.data())[left_idx],
      (void *)&((int*)right.data())[right_idx]);
}

RC BigIntegerType::cast_to(const Value &val, AttrType type, Value &result) const
{
  switch (type) {
  case AttrType::FLOATS: {
    float float_value = val.get_bigint();
    result.set_float(float_value);
    return RC::SUCCESS;
  }
  default:
    LOG_WARN("unsupported type %d", type);
    return RC::SCHEMA_FIELD_TYPE_MISMATCH;
  }
}

RC BigIntegerType::add(const Value &left, const Value &right, Value &result) const
{
  result.set_bigint(left.get_bigint() + right.get_bigint());
  return RC::SUCCESS;
}

RC BigIntegerType::subtract(const Value &left, const Value &right, Value &result) const
{
  result.set_bigint(left.get_bigint() - right.get_bigint());
  return RC::SUCCESS;
}

RC BigIntegerType::multiply(const Value &left, const Value &right, Value &result) const
{
  result.set_bigint(left.get_bigint() * right.get_bigint());
  return RC::SUCCESS;
}

RC BigIntegerType::negative(const Value &val, Value &result) const
{
  result.set_bigint(-val.get_bigint());
  return RC::SUCCESS;
}

RC BigIntegerType::set_value_from_str(Value &val, const string &data) const
{
  RC                rc = RC::SUCCESS;
  stringstream deserialize_stream;
  deserialize_stream.clear();  // 清理stream的状态，防止多次解析出现异常
  deserialize_stream.str(data);
  int int_value;
  deserialize_stream >> int_value;
  if (!deserialize_stream || !deserialize_stream.eof()) {
    rc = RC::SCHEMA_FIELD_TYPE_MISMATCH;
  } else {
    val.set_bigint(int_value);
  }
  return rc;
}

RC BigIntegerType::to_string(const Value &val, string &result) const
{
  stringstream ss;
  ss << val.value_.bigint_value_;
  result = ss.str();
  return RC::SUCCESS;
}