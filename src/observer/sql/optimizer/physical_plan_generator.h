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
// Created by Wangyunlai on 2022/12/14.
//

#pragma once

#include "common/sys/rc.h"
#include "sql/operator/logical_operator.h"
#include "sql/operator/physical_operator.h"
#include "sql/operator/create_materialized_view_logic_operator.h"

class Session;
class TableGetLogicalOperator;
class PredicateLogicalOperator;
class ProjectLogicalOperator;
class InsertLogicalOperator;
class DeleteLogicalOperator;
class ExplainLogicalOperator;
class JoinLogicalOperator;
class CalcLogicalOperator;
class GroupByLogicalOperator;
class OrderByLogicalOperator;
class LimitLogicalOperator;

/**
 * @brief 物理计划生成器
 * @ingroup PhysicalOperator
 * @details 根据逻辑计划生成物理计划。
 * 不会做任何优化，完全根据本意生成物理计划。
 */
class PhysicalPlanGenerator
{
public:
  PhysicalPlanGenerator()          = default;
  virtual ~PhysicalPlanGenerator() = default;

  RC create(LogicalOperator &logical_operator, unique_ptr<PhysicalOperator> &oper, Session *session);
  RC create_vec(LogicalOperator &logical_operator, unique_ptr<PhysicalOperator> &oper, Session *session);

private:
  RC create_plan(TableGetLogicalOperator &logical_oper, unique_ptr<PhysicalOperator> &oper, Session *session);
  RC create_plan(PredicateLogicalOperator &logical_oper, unique_ptr<PhysicalOperator> &oper, Session *session);
  RC create_plan(ProjectLogicalOperator &logical_oper, unique_ptr<PhysicalOperator> &oper, Session *session);
  RC create_plan(InsertLogicalOperator &logical_oper, unique_ptr<PhysicalOperator> &oper, Session *session);
  RC create_plan(DeleteLogicalOperator &logical_oper, unique_ptr<PhysicalOperator> &oper, Session *session);
  RC create_plan(ExplainLogicalOperator &logical_oper, unique_ptr<PhysicalOperator> &oper, Session *session);
  RC create_plan(JoinLogicalOperator &logical_oper, unique_ptr<PhysicalOperator> &oper, Session *session);
  RC create_plan(CalcLogicalOperator &logical_oper, unique_ptr<PhysicalOperator> &oper, Session *session);
  RC create_plan(GroupByLogicalOperator &logical_oper, unique_ptr<PhysicalOperator> &oper, Session *session);
  RC create_vec_plan(ProjectLogicalOperator &logical_oper, unique_ptr<PhysicalOperator> &oper, Session *session);
  RC create_vec_plan(GroupByLogicalOperator &logical_oper, unique_ptr<PhysicalOperator> &oper, Session *session);
  RC create_vec_plan(TableGetLogicalOperator &logical_oper, unique_ptr<PhysicalOperator> &oper, Session *session);
  RC create_vec_plan(ExplainLogicalOperator &logical_oper, unique_ptr<PhysicalOperator> &oper, Session *session);
  RC create_vec_plan(OrderByLogicalOperator &logical_oper, unique_ptr<PhysicalOperator> &oper, Session *session);
  RC create_vec_plan(LimitLogicalOperator &logical_oper, unique_ptr<PhysicalOperator> &oper, Session *session);
  RC create_vec_plan(CreateMaterializedViewLogicalOperator &logical_operator, unique_ptr<PhysicalOperator> &oper, Session *session);

  // TODO: remove this and add CBO rules
  bool can_use_hash_join(JoinLogicalOperator &logical_oper);
};
