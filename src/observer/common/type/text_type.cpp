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
#include "common/log/log.h"
#include "common/type/attr_type.h"
#include "common/type/char_type.h"
#include "common/type/text_type.h"
#include "common/value.h"

int TextType::compare(const Value &left, const Value &right) const
{
  ASSERT(left.attr_type() == AttrType::TEXTS && right.attr_type() == AttrType::TEXTS, "invalid type");
  return common::compare_string(
      (void *)left.value_.pointer_value_, left.length_, (void *)right.value_.pointer_value_, right.length_);
}

RC TextType::set_value_from_str(Value &val, const string &data) const
{
  val.set_text(data.c_str(), data.size());
  return RC::SUCCESS;
}

RC TextType::cast_to(const Value &val, AttrType type, Value &result) const
{
  switch (type) {
    case AttrType::DATES: {
      result.attr_type_ = type;
      result.length_    = 4;
      const char *s     = val.value_.pointer_value_;

      int year, month, day;
      // 使用 sscanf 解析日期字符串
      if (sscanf(s, "%d-%d-%d", &year, &month, &day) == 3) {
        result.value_.int_value_ = year * 10000 + month * 100 + day;
      } else {
        // 如果解析失败，设置为非法值（比如0，或你可以定义错误处理）
        LOG_WARN("Invalid date string: %s", s);
        return RC::INVALID_ARGUMENT;
        result.value_.int_value_ = 0;
      }
    } break;
    case AttrType::CHARS: {
      result.set_string(val.value_.pointer_value_, val.length_);
    } break;
    default: return RC::UNIMPLEMENTED;
  }
  return RC::SUCCESS;
}

int TextType::cast_cost(AttrType type)
{
  if (type == AttrType::DATES) {
    return 0;
  }
  if (type == AttrType::CHARS) {
    return 50;
  }
  return INT32_MAX;
}

RC TextType::to_string(const Value &val, string &result) const
{
  result = string(val.value_.pointer_value_, val.value_.pointer_value_ + val.length_);
  return RC::SUCCESS;
}