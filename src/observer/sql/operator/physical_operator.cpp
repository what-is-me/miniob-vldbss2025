/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by WangYunlai on 2022/11/18.
//

#include "sql/operator/physical_operator.h"

string physical_operator_type_name(PhysicalOperatorType type)
{
  switch (type) {
    case PhysicalOperatorType::TABLE_SCAN: return "TABLE_SCAN";
    case PhysicalOperatorType::INDEX_SCAN: return "INDEX_SCAN";
    case PhysicalOperatorType::NESTED_LOOP_JOIN: return "NESTED_LOOP_JOIN";
    case PhysicalOperatorType::HASH_JOIN: return "HASH_JOIN";
    case PhysicalOperatorType::EXPLAIN: return "EXPLAIN";
    case PhysicalOperatorType::PREDICATE: return "PREDICATE";
    case PhysicalOperatorType::INSERT: return "INSERT";
    case PhysicalOperatorType::DELETE: return "DELETE";
    case PhysicalOperatorType::PROJECT: return "PROJECT";
    case PhysicalOperatorType::STRING_LIST: return "STRING_LIST";
    case PhysicalOperatorType::HASH_GROUP_BY: return "HASH_GROUP_BY";
    case PhysicalOperatorType::SCALAR_GROUP_BY: return "SCALAR_GROUP_BY";
    case PhysicalOperatorType::AGGREGATE_VEC: return "AGGREGATE_VEC";
    case PhysicalOperatorType::GROUP_BY_VEC: return "GROUP_BY_VEC";
    case PhysicalOperatorType::PROJECT_VEC: return "PROJECT_VEC";
    case PhysicalOperatorType::TABLE_SCAN_VEC: return "TABLE_SCAN_VEC";
    case PhysicalOperatorType::EXPR_VEC: return "EXPR_VEC";
    case PhysicalOperatorType::PREDICATE_VEC: return "PREDICATE_VEC";
    case PhysicalOperatorType::CALC: return "CALC";
    case PhysicalOperatorType::ORDER_BY_VEC: return "ORDER_BY_VEC";
    case PhysicalOperatorType::LIMIT_VEC: return "LIMIT_VEC";
    case PhysicalOperatorType::ORDER_BY_LIMIT_VEC: return "ORDER_BY_LIMIT_VEC";
    case PhysicalOperatorType::CREATE_MATERIALIZED_VIEW: return "CREATE_MATERIALIZED_VIEW";
    default: return "UNKNOWN";    
  }
}

string PhysicalOperator::name() const { return physical_operator_type_name(type()); }

string PhysicalOperator::param() const { return ""; }
