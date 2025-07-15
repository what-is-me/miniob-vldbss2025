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
#include "common/type/date_type.h"
#include "common/value.h"
#include <stdexcept>
#include <iomanip>


int DateType::compare(const Value &left, const Value &right) const
{
  ASSERT(left.attr_type() == right.attr_type() && left.attr_type() == AttrType::DATES, "DateType::compare type error: not all dates");
  return common::compare_date((void*)&left.value_.int_value_, (void*)&right.value_.int_value_);
}

RC DateType::set_value_from_str(Value &val, const string &data) const
{
  // TODO: 实现
  
  const char* s = data.c_str();

  int year, month, day;
  // 使用 sscanf 解析日期字符串
  if (sscanf(s, "%d-%d-%d", &year, &month, &day) == 3) {
    val.set_date(year * 10000 + month * 100 + day);
  } else {
    // 如果解析失败，设置为非法值（比如0，或你可以定义错误处理）
    LOG_WARN("Invalid date string: %s", s);
    throw std::invalid_argument("Invalid date string");
    val.value_.int_value_ = 0;
  }

  return RC::SUCCESS;
}

RC DateType::cast_to(const Value &val, AttrType type, Value &result) const
{
  throw std::runtime_error("date_type DateType::cast_to not supported");
  return RC::UNSUPPORTED;
}

int DateType::cast_cost(AttrType type)
{
  if (type == AttrType::CHARS) {
    return 0;
  }
  return INT32_MAX;
}

RC DateType::to_string(const Value &val, string &result) const
{
  int date_numeric = val.value_.int_value_;

  int year = date_numeric / 10000;
  int month = (date_numeric / 100) % 100;
  int day = date_numeric % 100;

  stringstream ss;
  ss << std::setfill('0') << std::setw(4) << year << "-"
     << std::setw(2) << month << "-"
     << std::setw(2) << day;

  result = ss.str();
  return RC::SUCCESS;
}
