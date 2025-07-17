#pragma once

#include "common/log/log.h"
#include "common/types.h"
#include "sql/parser/parse_defs.h"
#include "event/sql_debug.h"
#include "sql/stmt/select_stmt.h"

/**
 * @brief 表示创建物化视图的语句
 * @ingroup Statement
 */
class CreateMaterializedViewStmt : public Stmt
{
public:
CreateMaterializedViewStmt(std::string view_name, std::unique_ptr<SelectStmt> select_stmt)
    : view_name_(std::move(view_name)), select_stmt_(std::move(select_stmt)) {}

  virtual ~CreateMaterializedViewStmt() = default;

  StmtType type() const override { return StmtType::CREATE_MATERIALIZED_VIEW; }

  const string &view_name() const { return view_name_; }
  SelectStmt &select_stmt() { return *select_stmt_; }

  static RC create(Db *db, const CreateMaterializedViewSqlNode &create_materialized_view, Stmt *&stmt);

  std::string original_table_name() const { return select_stmt_->tables()[0]->name(); }

private:
  std::string view_name_;
  std::unique_ptr<SelectStmt> select_stmt_;
};
