#include "common/log/log.h"
#include "common/types.h"
#include "sql/parser/parse_defs.h"
#include "sql/stmt/create_materialized_view_stmt.h"
#include "sql/stmt/select_stmt.h"
#include "event/sql_debug.h"
#include "storage/db/db.h"
#include <memory>

RC CreateMaterializedViewStmt::create(
    Db *db, const CreateMaterializedViewSqlNode &create_materialized_view, Stmt *&stmt)
{
  // 1. 检查视图名是否重复
  if (db->find_table(create_materialized_view.view_name.c_str()) != nullptr) {
    LOG_WARN("materialized view name already exists as a table. view name=%s",
             create_materialized_view.view_name.c_str());
    return RC::SCHEMA_MATERIALIZED_VIEW_NAME_REPEAT;
  }

  // 2. 创建 SelectStmt 实例
  SelectStmt *selection_stmt = nullptr;
  RC          rc             = SelectStmt::create(db,
      create_materialized_view.select_sql_node->selection,
      reinterpret_cast<Stmt *&>(selection_stmt));  // 强制绑定引用

  if (rc != RC::SUCCESS) {
    return rc;
  }

  // 3. 用 selection_stmt 构造 CreateMaterializedViewStmt，并赋值给输出 stmt
  stmt = new CreateMaterializedViewStmt(create_materialized_view.view_name,
      std::unique_ptr<SelectStmt>(selection_stmt));  // 转为智能指针管理

  return RC::SUCCESS;
}
