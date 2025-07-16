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
// Created by Wangyunlai on 2023/7/12.
//

#include "sql/executor/load_data_executor.h"
#include "common/lang/string.h"
#include "event/session_event.h"
#include "event/sql_event.h"
#include "sql/executor/sql_result.h"
#include "sql/stmt/load_data_stmt.h"
#include "storage/common/chunk.h"

using namespace common;

RC LoadDataExecutor::execute(SQLStageEvent *sql_event)
{
  RC            rc         = RC::SUCCESS;
  SqlResult    *sql_result = sql_event->session_event()->sql_result();
  LoadDataStmt *stmt       = static_cast<LoadDataStmt *>(sql_event->stmt());
  Table        *table      = stmt->table();
  const char   *file_name  = stmt->filename();
  load_data(table, file_name, stmt->terminated(), stmt->enclosed(), sql_result);
  return rc;
}

/**
 * 从文件中导入数据时使用。尝试向表中插入解析后的一行数据。
 * @param table  要导入的表
 * @param file_values 从文件中读取到的一行数据，使用分隔符拆分后的几个字段值
 * @param record_values Table::insert_record使用的参数，为了防止频繁的申请内存
 * @param errmsg 如果出现错误，通过这个参数返回错误信息
 * @return 成功返回RC::SUCCESS
 */
RC insert_record_from_file(
    Table *table, vector<string> &file_values, vector<Value> &record_values, stringstream &errmsg)
{

  const int field_num     = record_values.size();
  const int sys_field_num = table->table_meta().sys_field_num();

  if (file_values.size() < record_values.size()) {
    return RC::SCHEMA_FIELD_MISSING;
  }

  RC rc = RC::SUCCESS;

  stringstream deserialize_stream;
  for (int i = 0; i < field_num && RC::SUCCESS == rc; i++) {
    const FieldMeta *field = table->table_meta().field(i + sys_field_num);

    string &file_value = file_values[i];
    if (field->type() != AttrType::CHARS) {
      common::strip(file_value);
    }
    rc = DataType::type_instance(field->type())->set_value_from_str(record_values[i], file_value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("Failed to deserialize value from string: %s, type=%d", file_value.c_str(), field->type());
      return rc;
    }
  }

  if (RC::SUCCESS == rc) {
    Record record;
    rc = table->make_record(field_num, record_values.data(), record);
    if (rc != RC::SUCCESS) {
      errmsg << "insert failed.";
    } else if (RC::SUCCESS != (rc = table->insert_record(record))) {
      errmsg << "insert failed.";
    }
  }
  return rc;
}

void split_csv_line(const string &str, char delim, char enclosed_c, vector<string> &results) {
  results.clear();
  string field;
  bool in_enclosure = false;

  for (size_t i = 0; i < str.size(); ++i) {
    char c = str[i];

    if (c == enclosed_c) {
      if (in_enclosure && i + 1 < str.size() && str[i + 1] == enclosed_c) {
        field += enclosed_c;
        ++i; // 跳过第二个引号
      } else {
        in_enclosure = !in_enclosure;
      }
    } else if (c == delim && !in_enclosure) {
      results.push_back(field);
      field.clear();
    } else {
      field += c;
    }
  }

  results.push_back(field);
}

// 检查引号是否闭合（数量为偶数）
bool is_enclosure_balanced(const string& line, char enclosed_c) {
  int count = 0;
  for (size_t i = 0; i < line.size(); ++i) {
    if (line[i] == enclosed_c) {
      if (i + 1 < line.size() && line[i + 1] == enclosed_c) {
        ++i; // 跳过转义引号
      } else {
        ++count;
      }
    }
  }
  return count % 2 == 0;
}



// TODO: pax format and row format
void LoadDataExecutor::load_data(Table *table, const char *file_name, char terminated, char enclosed, SqlResult *sql_result)
{
  // your code here
  stringstream result_string;

  fstream fs;
  fs.open(file_name, ios_base::in | ios_base::binary);
  if (!fs.is_open()) {
    result_string << "Failed to open file: " << file_name << ". system error=" << strerror(errno) << endl;
    sql_result->set_return_code(RC::FILE_NOT_EXIST);
    sql_result->set_state_string(result_string.str());
    return;
  }

  struct timespec begin_time;
  clock_gettime(CLOCK_MONOTONIC, &begin_time);
  const int sys_field_num = table->table_meta().sys_field_num();
  const int field_num     = table->table_meta().field_num() - sys_field_num;

  vector<Value>       record_values(field_num);
  string              line;
  vector<string> file_values;
  int                      line_num        = 0;
  [[maybe_unused]]int                      insertion_count = 0;
  RC                       rc              = RC::SUCCESS;
  // 构造columns数组，个数为table的field_num
  std::vector<std::unique_ptr<Column>> columns;
  for (int i = 0; i < field_num; i++) {
    const FieldMeta *field = table->table_meta().field(i);
    columns.emplace_back(make_unique<Column>(*field));
  }
  string multiline;
  while (!fs.eof() && RC::SUCCESS == rc) {
    getline(fs, line);
    if (!multiline.empty()) {
      multiline += "\n";
    }
    multiline += line;
    line_num++;
    if (common::is_blank(line.c_str())) {
      continue;
    }

    file_values.clear();
    if (!is_enclosure_balanced(multiline, enclosed)) {
      continue;
    }
    split_csv_line(multiline, terminated, enclosed, file_values);
    multiline.clear();
    stringstream errmsg;

    if (table->table_meta().storage_format() == StorageFormat::ROW_FORMAT) {
      rc = insert_record_from_file(table, file_values, record_values, errmsg);
      if (rc != RC::SUCCESS) {
        result_string << "Line:" << line_num << " insert record failed:" << errmsg.str() << ". error:" << strrc(rc)
                      << endl;
      } else {
        insertion_count++;
      }
    } else if (table->table_meta().storage_format() == StorageFormat::PAX_FORMAT) {
      // your code here
      // Todo: 参照insert_record_from_file实现
      if (file_values.size() < record_values.size()) {
        // return RC::SCHEMA_FIELD_MISSING;
        throw runtime_error("Insert record size too small");
      }

      stringstream deserialize_stream;
      for (int i = 0; i < field_num && RC::SUCCESS == rc; i++) {
        const FieldMeta *field = table->table_meta().field(i + sys_field_num);

        string &file_value = file_values[i];
        if (field->type() != AttrType::CHARS) {
          common::strip(file_value);
        }
        rc = DataType::type_instance(field->type())->set_value_from_str(record_values[i], file_value);
        if (rc != RC::SUCCESS) {
          LOG_WARN("Failed to deserialize value from string: %s, type=%d", file_value.c_str(), field->type());
          result_string << "Line:" << line_num << " insert record failed:" << errmsg.str() << ". error:" << strrc(rc)
                      << endl;
        }
      }

      if (RC::SUCCESS == rc) {
        // 插入对应的column
        for (int i = 0; i < record_values.size(); i++) {
          rc = columns[i]->append_value(record_values[i]);
          if (OB_FAIL(rc)) {
            LOG_WARN("Failed to append value to record, rc=%d", rc);
            rc = RC::INVALID_ARGUMENT;
            throw runtime_error("Failed to append value to record");
          }
        }

        // 当前column列满了，插入chunk
        if (columns[0]->count() == columns[0]->capacity()) {
          Chunk chunk;
          for (int i = 0; i < columns.size(); i++) {
            chunk.add_column(std::move(columns[i]), i);
            const FieldMeta *field = table->table_meta().field(i);
            columns[i] = std::make_unique<Column>(*field);
          }
          table->insert_chunk(chunk);
        }
      }
      // return rc;
      rc = RC::SUCCESS;
    } else {
      rc = RC::UNSUPPORTED;
      result_string << "Unsupported storage format: " << strrc(rc) << endl;
    }
  }

  if (table->table_meta().storage_format() == StorageFormat::PAX_FORMAT && columns[0]->count() != 0) {
    Chunk chunk;
    for (int i = 0; i < columns.size(); i++) {
      chunk.add_column(std::move(columns[i]), i);
      // 不用清空了，以后不用了
    }
    table->insert_chunk(chunk);
  }

  fs.close();

  struct timespec end_time;
  clock_gettime(CLOCK_MONOTONIC, &end_time);
  if (RC::SUCCESS == rc) {
    result_string << strrc(rc);
  }
  sql_result->set_return_code(RC::SUCCESS);
}
