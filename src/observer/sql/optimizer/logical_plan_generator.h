/* Copyright (c) 2023 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2023/08/16.
//

#pragma once

#include "common/lang/memory.h"
#include "common/sys/rc.h"
#include "common/type/attr_type.h"
#include "sql/stmt/create_materialized_view_stmt.h"

class Stmt;
class CalcStmt;
class SelectStmt;
class FilterStmt;
class InsertStmt;
class DeleteStmt;
class ExplainStmt;
class LogicalOperator;

class LogicalPlanGenerator
{
public:
  LogicalPlanGenerator()          = default;
  virtual ~LogicalPlanGenerator() = default;

  RC create(Stmt *stmt, unique_ptr<LogicalOperator> &logical_operator);

private:
  RC create_plan(CalcStmt *calc_stmt, unique_ptr<LogicalOperator> &logical_operator);
  RC create_plan(SelectStmt *select_stmt, unique_ptr<LogicalOperator> &logical_operator);
  RC create_plan(FilterStmt *filter_stmt, unique_ptr<LogicalOperator> &logical_operator);
  RC create_plan(InsertStmt *insert_stmt, unique_ptr<LogicalOperator> &logical_operator);
  RC create_plan(DeleteStmt *delete_stmt, unique_ptr<LogicalOperator> &logical_operator);
  RC create_plan(ExplainStmt *explain_stmt, unique_ptr<LogicalOperator> &logical_operator);
  RC create_plan(CreateMaterializedViewStmt *create_materialized_view_stmt, unique_ptr<LogicalOperator> &logical_operator);

  RC create_group_by_plan(SelectStmt *select_stmt, unique_ptr<LogicalOperator> &logical_operator);
  RC bind_order_by_plan(SelectStmt *select_stmt);

  int implicit_cast_cost(AttrType from, AttrType to);
};
