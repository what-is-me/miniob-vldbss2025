/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Meiyi & Longda on 2021/4/13.
//
#include "storage/record/record_manager.h"
#include "common/log/log.h"
#include "common/sys/rc.h"
#include "common/type/attr_type.h"
#include "common/type/string_t.h"
#include "storage/common/chunk.h"
#include "storage/common/column.h"
#include "storage/common/condition_filter.h"
#include "storage/record/lob_handler.h"
#include "storage/trx/trx.h"
#include "storage/clog/log_handler.h"
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

using namespace common;

static constexpr int PAGE_HEADER_SIZE = (sizeof(PageHeader));
RecordPageHandler   *RecordPageHandler::create(StorageFormat format)
{
  if (format == StorageFormat::ROW_FORMAT) {
    return new RowRecordPageHandler();
  } else {
    return new PaxRecordPageHandler();
  }
}
/**
 * @brief 8字节对齐
 * 注: ceiling(a / b) = floor((a + b - 1) / b)
 *
 * @param size 待对齐的字节数
 */
int align8(int size) { return (size + 7) & ~7; }

/**
 * @brief 计算指定大小的页面，可以容纳多少个记录
 *
 * @param page_size   页面的大小
 * @param record_size 记录的大小
 * @param fixed_size  除 PAGE_HEADER 外，页面中其余固定长度占用，目前为PAX存储格式中的
 *                    列偏移索引大小（column index）。
 */
int page_record_capacity(int page_size, int record_size, int fixed_size)
{
  // (record_capacity * record_size) + record_capacity/8 + 1 <= (page_size - fix_size)
  // ==> record_capacity = ((page_size - fix_size) - 1) / (record_size + 0.125)
  return (int)((page_size - PAGE_HEADER_SIZE - fixed_size - 1) / (record_size + 0.125));
}

/**
 * @brief bitmap 记录了某个位置是否有有效的记录数据，这里给定记录个数时需要多少字节来存放bitmap数据
 * 注: ceiling(a / b) = floor((a + b - 1) / b)
 *
 * @param record_capacity 想要存放多少记录
 */
int page_bitmap_size(int record_capacity) { return (record_capacity + 7) / 8; }

string PageHeader::to_string() const
{
  stringstream ss;
  ss << "record_num:" << record_num << ",column_num:" << column_num << ",record_real_size:" << record_real_size
     << ",record_size:" << record_size << ",record_capacity:" << record_capacity << ",data_offset:" << data_offset;
  return ss.str();
}

////////////////////////////////////////////////////////////////////////////////
RecordPageIterator::RecordPageIterator() {}
RecordPageIterator::~RecordPageIterator() {}

void RecordPageIterator::init(RecordPageHandler *record_page_handler, SlotNum start_slot_num /*=0*/)
{
  record_page_handler_ = record_page_handler;
  page_num_            = record_page_handler->get_page_num();
  bitmap_.init(record_page_handler->bitmap_, record_page_handler->page_header_->record_capacity);
  next_slot_num_ = bitmap_.next_setted_bit(start_slot_num);
}

bool RecordPageIterator::has_next() { return -1 != next_slot_num_; }

RC RecordPageIterator::next(Record &record)
{
  record_page_handler_->get_record(RID(page_num_, next_slot_num_), record);

  if (next_slot_num_ >= 0) {
    next_slot_num_ = bitmap_.next_setted_bit(next_slot_num_ + 1);
  }
  return record.rid().slot_num != -1 ? RC::SUCCESS : RC::RECORD_EOF;
}

////////////////////////////////////////////////////////////////////////////////

RecordPageHandler::~RecordPageHandler() { cleanup(); }

RC RecordPageHandler::init(DiskBufferPool &buffer_pool, LogHandler &log_handler, PageNum page_num, ReadWriteMode mode,
    LobFileHandler *lob_handler)
{
  if (disk_buffer_pool_ != nullptr) {
    if (frame_->page_num() == page_num) {
      LOG_WARN("Disk buffer pool has been opened for page_num %d.", page_num);
      return RC::RECORD_OPENNED;
    } else {
      cleanup();
    }
  }
  lob_handler_ = lob_handler;

  RC ret = RC::SUCCESS;
  if ((ret = buffer_pool.get_this_page(page_num, &frame_)) != RC::SUCCESS) {
    LOG_ERROR("Failed to get page handle from disk buffer pool. ret=%d:%s", ret, strrc(ret));
    return ret;
  }

  char *data = frame_->data();

  if (mode == ReadWriteMode::READ_ONLY) {
    frame_->read_latch();
  } else {
    frame_->write_latch();
  }
  disk_buffer_pool_ = &buffer_pool;

  rw_mode_     = mode;
  page_header_ = (PageHeader *)(data);
  bitmap_      = data + PAGE_HEADER_SIZE;

  (void)log_handler_.init(log_handler, buffer_pool.id(), page_header_->record_real_size, storage_format_);

  LOG_TRACE("Successfully init page_num %d.", page_num);
  return ret;
}

RC RecordPageHandler::recover_init(DiskBufferPool &buffer_pool, PageNum page_num)
{
  if (disk_buffer_pool_ != nullptr) {
    LOG_WARN("Disk buffer pool has been opened for page_num %d.", page_num);
    return RC::RECORD_OPENNED;
  }

  RC ret = RC::SUCCESS;
  if ((ret = buffer_pool.get_this_page(page_num, &frame_)) != RC::SUCCESS) {
    LOG_ERROR("Failed to get page handle from disk buffer pool. ret=%d:%s", ret, strrc(ret));
    return ret;
  }

  char *data = frame_->data();

  frame_->write_latch();
  disk_buffer_pool_ = &buffer_pool;
  rw_mode_          = ReadWriteMode::READ_WRITE;
  page_header_      = (PageHeader *)(data);
  bitmap_           = data + PAGE_HEADER_SIZE;

  buffer_pool.recover_page(page_num);

  LOG_TRACE("Successfully init page_num %d.", page_num);
  return ret;
}

RC RecordPageHandler::init_empty_page(DiskBufferPool &buffer_pool, LogHandler &log_handler, PageNum page_num,
    int record_size, TableMeta *table_meta, LobFileHandler *lob_handler)
{
  RC rc        = init(buffer_pool, log_handler, page_num, ReadWriteMode::READ_WRITE);
  lob_handler_ = lob_handler;
  if (OB_FAIL(rc)) {
    LOG_ERROR("Failed to init empty page page_num:record_size %d:%d. rc=%s", page_num, record_size, strrc(rc));
    return rc;
  }

  (void)log_handler_.init(log_handler, buffer_pool.id(), record_size, storage_format_);

  int column_num = 0;
  // only pax format need column index
  if (table_meta != nullptr && storage_format_ == StorageFormat::PAX_FORMAT) {
    column_num = table_meta->field_num();
  }
  page_header_->record_num       = 0;
  page_header_->column_num       = column_num;
  page_header_->record_real_size = record_size;
  page_header_->record_size      = align8(record_size);
  page_header_->record_capacity  = page_record_capacity(
      BP_PAGE_DATA_SIZE, page_header_->record_size, column_num * sizeof(int) /* other fixed size*/);
  page_header_->col_idx_offset = align8(PAGE_HEADER_SIZE + page_bitmap_size(page_header_->record_capacity));
  page_header_->data_offset    = align8(PAGE_HEADER_SIZE + page_bitmap_size(page_header_->record_capacity)) +
                              column_num * sizeof(int) /* column index*/;
  this->fix_record_capacity();
  ASSERT(page_header_->data_offset + page_header_->record_capacity * page_header_->record_size 
              <= BP_PAGE_DATA_SIZE, 
         "Record overflow the page size");

  bitmap_ = frame_->data() + PAGE_HEADER_SIZE;
  memset(bitmap_, 0, page_bitmap_size(page_header_->record_capacity));
  // column_index[i] store the end offset of column `i` or the start offset of column `i+1`

  // 计算列偏移
  int *column_index = reinterpret_cast<int *>(frame_->data() + page_header_->col_idx_offset);
  for (int i = 0; i < column_num; ++i) {
    ASSERT(i == table_meta->field(i)->field_id(), "i should be the col_id of fields[i]");
    if (i == 0) {
      column_index[i] = table_meta->field(i)->len() * page_header_->record_capacity;
    } else {
      column_index[i] = table_meta->field(i)->len() * page_header_->record_capacity + column_index[i - 1];
    }
  }

  rc = log_handler_.init_new_page(frame_, page_num, span((const char *)column_index, column_num * sizeof(int)));
  if (OB_FAIL(rc)) {
    LOG_ERROR("Failed to init empty page: write log failed. page_num:record_size %d:%d. rc=%s", 
              page_num, record_size, strrc(rc));
    return rc;
  }

  return RC::SUCCESS;
}

RC RecordPageHandler::init_empty_page(DiskBufferPool &buffer_pool, LogHandler &log_handler, PageNum page_num,
    int record_size, int column_num, const char *col_idx_data, LobFileHandler *lob_handler)
{
  RC rc = init(buffer_pool, log_handler, page_num, ReadWriteMode::READ_WRITE);
  if (OB_FAIL(rc)) {
    LOG_ERROR("Failed to init empty page page_num:record_size %d:%d. rc=%s", page_num, record_size, strrc(rc));
    return rc;
  }
  lob_handler_ = lob_handler;

  (void)log_handler_.init(log_handler, buffer_pool.id(), record_size, storage_format_);

  page_header_->record_num       = 0;
  page_header_->column_num       = column_num;
  page_header_->record_real_size = record_size;
  page_header_->record_size      = align8(record_size);
  page_header_->record_capacity =
      page_record_capacity(BP_PAGE_DATA_SIZE, page_header_->record_size, page_header_->column_num * sizeof(int));
  page_header_->col_idx_offset = align8(PAGE_HEADER_SIZE + page_bitmap_size(page_header_->record_capacity));
  page_header_->data_offset    = align8(PAGE_HEADER_SIZE + page_bitmap_size(page_header_->record_capacity)) +
                              column_num * sizeof(int) /* column index*/;
  this->fix_record_capacity();
  ASSERT(page_header_->data_offset + page_header_->record_capacity * page_header_->record_size 
              <= BP_PAGE_DATA_SIZE, 
         "Record overflow the page size");

  bitmap_ = frame_->data() + PAGE_HEADER_SIZE;
  memset(bitmap_, 0, page_bitmap_size(page_header_->record_capacity));
  // column_index[i] store the end offset of column `i` the start offset of column `i+1`
  int *column_index = reinterpret_cast<int *>(frame_->data() + page_header_->col_idx_offset);
  memcpy(column_index, col_idx_data, column_num * sizeof(int));

  if (OB_FAIL(rc)) {
    LOG_ERROR("Failed to init empty page: write log failed. page_num:record_size %d:%d. rc=%s", 
              page_num, record_size, strrc(rc));
    return rc;
  }

  return RC::SUCCESS;
}

RC RecordPageHandler::cleanup()
{
  if (disk_buffer_pool_ != nullptr) {
    if (rw_mode_ == ReadWriteMode::READ_ONLY) {
      frame_->read_unlatch();
    } else {
      frame_->write_unlatch();
    }
    disk_buffer_pool_->unpin_page(frame_);
    disk_buffer_pool_ = nullptr;
  }

  return RC::SUCCESS;
}

RC RowRecordPageHandler::insert_record(const char *data, RID *rid)
{
  ASSERT(rw_mode_ != ReadWriteMode::READ_ONLY, 
         "cannot insert record into page while the page is readonly");

  if (page_header_->record_num == page_header_->record_capacity) {
    LOG_WARN("Page is full, page_num %d:%d.", disk_buffer_pool_->file_desc(), frame_->page_num());
    return RC::RECORD_NOMEM;
  }

  // 找到空闲位置
  Bitmap bitmap(bitmap_, page_header_->record_capacity);
  int    index = bitmap.next_unsetted_bit(0);
  bitmap.set_bit(index);
  page_header_->record_num++;

  // 记录日志，与数据库恢复相关
  RC rc = log_handler_.insert_record(frame_, RID(get_page_num(), index), data);
  if (OB_FAIL(rc)) {
    LOG_ERROR("Failed to insert record. page_num %d:%d. rc=%s", disk_buffer_pool_->file_desc(), frame_->page_num(), strrc(rc));
    // return rc; // ignore errors
  }

  // assert index < page_header_->record_capacity
  char *record_data = get_record_data(index);
  memcpy(record_data, data, page_header_->record_real_size);

  frame_->mark_dirty();

  if (rid) {
    rid->page_num = get_page_num();
    rid->slot_num = index;
  }

  // LOG_TRACE("Insert record. rid page_num=%d, slot num=%d", get_page_num(), index);
  return RC::SUCCESS;
}

RC RowRecordPageHandler::recover_insert_record(const char *data, const RID &rid)
{
  if (rid.slot_num >= page_header_->record_capacity) {
    LOG_WARN("slot_num illegal, slot_num(%d) > record_capacity(%d).", rid.slot_num, page_header_->record_capacity);
    return RC::RECORD_INVALID_RID;
  }

  // 更新位图
  Bitmap bitmap(bitmap_, page_header_->record_capacity);
  if (!bitmap.get_bit(rid.slot_num)) {
    bitmap.set_bit(rid.slot_num);
    page_header_->record_num++;
  }

  // 恢复数据
  char *record_data = get_record_data(rid.slot_num);
  memcpy(record_data, data, page_header_->record_real_size);

  frame_->mark_dirty();

  return RC::SUCCESS;
}

RC RowRecordPageHandler::delete_record(const RID *rid)
{
  ASSERT(rw_mode_ != ReadWriteMode::READ_ONLY, 
         "cannot delete record from page while the page is readonly");

  Bitmap bitmap(bitmap_, page_header_->record_capacity);
  if (bitmap.get_bit(rid->slot_num)) {
    bitmap.clear_bit(rid->slot_num);
    page_header_->record_num--;
    frame_->mark_dirty();

    RC rc = log_handler_.delete_record(frame_, *rid);
    if (OB_FAIL(rc)) {
      LOG_ERROR("Failed to delete record. page_num %d:%d. rc=%s", disk_buffer_pool_->file_desc(), frame_->page_num(), strrc(rc));
      // return rc; // ignore errors
    }

    return RC::SUCCESS;
  } else {
    LOG_DEBUG("Invalid slot_num %d, slot is empty, page_num %d.", rid->slot_num, frame_->page_num());
    return RC::RECORD_NOT_EXIST;
  }
}

RC RowRecordPageHandler::update_record(const RID &rid, const char *data)
{
  ASSERT(rw_mode_ != ReadWriteMode::READ_ONLY, "cannot delete record from page while the page is readonly");

  if (rid.slot_num >= page_header_->record_capacity) {
    LOG_ERROR("Invalid slot_num %d, exceed page's record capacity, frame=%s, page_header=%s",
              rid.slot_num, frame_->to_string().c_str(), page_header_->to_string().c_str());
    return RC::INVALID_ARGUMENT;
  }

  Bitmap bitmap(bitmap_, page_header_->record_capacity);
  if (bitmap.get_bit(rid.slot_num)) {
    frame_->mark_dirty();

    char *record_data = get_record_data(rid.slot_num);
    if (record_data == data) {
      // nothing to do
    } else {
      memcpy(record_data, data, page_header_->record_real_size);
    }

    RC rc = log_handler_.update_record(frame_, rid, data);
    if (OB_FAIL(rc)) {
      LOG_ERROR("Failed to update record. page_num %d:%d. rc=%s", 
                disk_buffer_pool_->file_desc(), frame_->page_num(), strrc(rc));
      // return rc; // ignore errors
    }

    return RC::SUCCESS;
  } else {
    LOG_DEBUG("Invalid slot_num %d, slot is empty, page_num %d.", rid.slot_num, frame_->page_num());
    return RC::RECORD_NOT_EXIST;
  }
}

RC RowRecordPageHandler::get_record(const RID &rid, Record &record)
{
  if (rid.slot_num >= page_header_->record_capacity) {
    LOG_ERROR("Invalid slot_num %d, exceed page's record capacity, frame=%s, page_header=%s",
              rid.slot_num, frame_->to_string().c_str(), page_header_->to_string().c_str());
    return RC::RECORD_INVALID_RID;
  }

  Bitmap bitmap(bitmap_, page_header_->record_capacity);
  if (!bitmap.get_bit(rid.slot_num)) {
    LOG_ERROR("Invalid slot_num:%d, slot is empty, page_num %d.", rid.slot_num, frame_->page_num());
    return RC::RECORD_NOT_EXIST;
  }

  record.set_rid(rid);
  record.set_data(get_record_data(rid.slot_num), page_header_->record_real_size);
  return RC::SUCCESS;
}

PageNum RecordPageHandler::get_page_num() const
{
  if (nullptr == page_header_) {
    return (PageNum)(-1);
  }
  return frame_->page_num();
}

bool RecordPageHandler::is_full() const { return page_header_->record_num >= page_header_->record_capacity; }

bool RecordPageHandler::is_empty() const { return page_header_->record_num == 0; }

RC PaxRecordPageHandler::insert_record(const char *data, RID *rid)
{
  ASSERT(rw_mode_ != ReadWriteMode::READ_ONLY, 
         "cannot insert record into page while the page is readonly");

  if (page_header_->record_num == page_header_->record_capacity) {
    LOG_WARN("Page is full, page_num %d:%d.", disk_buffer_pool_->file_desc(), frame_->page_num());
    return RC::RECORD_NOMEM;
  }

  // 找到空闲位置
  Bitmap bitmap(bitmap_, page_header_->record_capacity);
  int    index = bitmap.next_unsetted_bit(0);
  bitmap.set_bit(index);
  page_header_->record_num++;

  // 记录日志，与数据库恢复相关
  RC rc = log_handler_.insert_record(frame_, RID(get_page_num(), index), data);
  if (OB_FAIL(rc)) {
    LOG_ERROR("Failed to insert record. page_num %d:%d. rc=%s", disk_buffer_pool_->file_desc(), frame_->page_num(), strrc(rc));
    // return rc; // ignore errors
  }

  // record 按列拆分存入
  for (int col_id = 0, record_offset = 0; col_id < page_header_->column_num; ++col_id) {
    const int field_len = get_field_len(col_id);
    memcpy(get_field_data(index, col_id), data + record_offset, field_len);
    record_offset += field_len;
  }

  frame_->mark_dirty();

  if (rid) {
    rid->page_num = get_page_num();
    rid->slot_num = index;
  }

  // LOG_TRACE("Insert record. rid page_num=%d, slot num=%d", get_page_num(), index);
  return RC::SUCCESS;
}

class GlobalLobFileHandler
{
public:
  GlobalLobFileHandler() {}
  virtual ~GlobalLobFileHandler()
  {
    if (opened) {
      lob_file_handler.close_file();
    }
  }
  static LobFileHandler &get()
  {
    static GlobalLobFileHandler lb;
    if (!lb.opened) {
      const std::string file_name = "text_output";
      if (lb.lob_file_handler.open_file(file_name.c_str()) == RC::FILE_NOT_EXIST) {
        auto res = lb.lob_file_handler.create_file(file_name.c_str());
        if (res != RC::SUCCESS) {
          LOG_ERROR("Failed to create lob file.");
          throw std::runtime_error("Failed to create lob file.");
        }
      }
      lb.opened = true;
    }
    return lb.lob_file_handler;
  }

private:
  bool           opened{false};
  LobFileHandler lob_file_handler;
};

RC PaxRecordPageHandler::insert_chunk(const Chunk &chunk, int start_row, int &insert_rows)
{
  const int rows_to_insert    = chunk.rows() - start_row;
  const int rows_left_in_page = page_header_->record_capacity - page_header_->record_num;
  RC        rc                = rows_to_insert <= rows_left_in_page ? RC::SUCCESS : RC::RECORD_NOMEM;
  insert_rows                 = std::min(rows_to_insert, rows_left_in_page);
  std::vector<char> record_buffer(page_header_->record_real_size);
  std::vector<int>  record_offsets(page_header_->column_num);
  for (int col_id = 0, record_offset = 0; col_id < page_header_->column_num; col_id++) {
    record_offsets[col_id] = record_offset;
    record_offset += get_field_len(col_id);
  }
  Bitmap bitmap(bitmap_, page_header_->record_capacity);
  bitmap.set_bits(insert_rows);
  page_header_->record_num = insert_rows;
  for (int j = 0; j < chunk.column_num(); ++j) {
    const int col_id = chunk.column_ids(j);
    // 判断该列是否是TEXTS且列中存在vector buffer
    if (chunk.column(j).attr_type() == AttrType::TEXTS && chunk.column(j).is_vector_buffer_null() == false) {
      for (int i = start_row; i < start_row + insert_rows; ++i) {
        string_t *str = ((string_t *)chunk.column(j).data()) + i;
        // 若不是内联字符串，设置偏移量
        if (!str->is_inlined()) {
          ASSERT(str->is_inlined() == false, "TEXTS column should not be inlined");
          int64_t offset = 0;
          GlobalLobFileHandler::get().insert_data(offset, str->size(), str->data());
          str->set_offset(offset);
        }
        chunk.column(j).copy_to(get_field_data(i - start_row, col_id), i, 1);
      }
    } else {
      chunk.column(j).copy_to(get_field_data(0, col_id), start_row, insert_rows);
    }
  }
  frame_->mark_dirty();
  return rc;
}

RC PaxRecordPageHandler::delete_record(const RID *rid)
{
  ASSERT(rw_mode_ != ReadWriteMode::READ_ONLY, 
         "cannot delete record from page while the page is readonly");

  Bitmap bitmap(bitmap_, page_header_->record_capacity);
  if (bitmap.get_bit(rid->slot_num)) {
    bitmap.clear_bit(rid->slot_num);
    page_header_->record_num--;
    frame_->mark_dirty();

    RC rc = log_handler_.delete_record(frame_, *rid);
    if (OB_FAIL(rc)) {
      LOG_ERROR("Failed to delete record. page_num %d:%d. rc=%s", disk_buffer_pool_->file_desc(), frame_->page_num(), strrc(rc));
      // return rc; // ignore errors
    }

    return RC::SUCCESS;
  } else {
    LOG_DEBUG("Invalid slot_num %d, slot is empty, page_num %d.", rid->slot_num, frame_->page_num());
    return RC::RECORD_NOT_EXIST;
  }
}

RC PaxRecordPageHandler::get_record(const RID &rid, Record &record)
{
  if (rid.slot_num >= page_header_->record_capacity) {
    LOG_ERROR("Invalid slot_num %d, exceed page's record capacity, frame=%s, page_header=%s",
              rid.slot_num, frame_->to_string().c_str(), page_header_->to_string().c_str());
    return RC::RECORD_INVALID_RID;
  }
  Bitmap bitmap(bitmap_, page_header_->record_capacity);
  if (!bitmap.get_bit(rid.slot_num)) {
    LOG_ERROR("Invalid slot_num:%d, slot is empty, page_num %d.", rid.slot_num, frame_->page_num());
    return RC::RECORD_NOT_EXIST;
  }
  record.set_rid(rid);
  record.set_data_owner(static_cast<char *>(malloc(page_header_->record_real_size)), page_header_->record_real_size);
  for (int col_id = 0, record_offset = 0; col_id < page_header_->column_num; ++col_id) {
    const int   field_len  = get_field_len(col_id);
    const char *field_data = get_field_data(rid.slot_num, col_id);
    memcpy(record.data() + record_offset, field_data, field_len);
    record_offset += field_len;
  }
  return RC::SUCCESS;
}

// TODO: specify the column_ids that chunk needed. currenly we get all columns
RC PaxRecordPageHandler::get_chunk(Chunk &chunk)
{
  chunk.reset_data();
  RC     rc = RC::SUCCESS;
  Bitmap bitmap(bitmap_, page_header_->record_capacity);
  bool   fulfilled = page_header_->record_num == page_header_->record_capacity;
  for (int j = 0; j < chunk.column_num(); ++j) {
    const int col_id = chunk.column_ids(j);
    Column   &column = chunk.column(j);
    if (!fulfilled || chunk.column(j).attr_type() == AttrType::TEXTS) {
      for (int i = 0, index = 0; i < page_header_->record_num; ++i, ++index) {
        index = bitmap.next_setted_bit(index);
        if (chunk.column(j).attr_type() == AttrType::TEXTS) {
          string_t str = *(string_t *)get_field_data(index, j);
          if (!str.is_inlined()) {
            int64_t offset = str.get_offset();
            str            = chunk.column(j).get_vector_buffer()->empty_string(str.size());
            GlobalLobFileHandler::get().get_data(offset, str.size(), str.get_noinline_ptr());
          }
          rc = column.append_one((char *)&str);
        } else {
          rc = column.append_one(get_field_data(index, col_id));
        }
        if (OB_FAIL(rc)) {
          return rc;
        }
      }
    } else {
      RC rc = column.append(get_field_data(0, col_id), page_header_->record_num);
      if (OB_FAIL(rc)) {
        return rc;
      }
    }
  }
  return RC::SUCCESS;
}

char *PaxRecordPageHandler::get_field_data(SlotNum slot_num, int col_id)
{
  int *col_idx = reinterpret_cast<int *>(frame_->data() + page_header_->col_idx_offset);
  if (col_id == 0) {
    return frame_->data() + page_header_->data_offset + (get_field_len(col_id) * slot_num);
  } else {
    return frame_->data() + page_header_->data_offset + col_idx[col_id - 1] + (get_field_len(col_id) * slot_num);
  }
}

int PaxRecordPageHandler::get_field_len(int col_id)
{
  int *col_idx = reinterpret_cast<int *>(frame_->data() + page_header_->col_idx_offset);
  if (col_id == 0) {
    return col_idx[col_id] / page_header_->record_capacity;
  } else {
    return (col_idx[col_id] - col_idx[col_id - 1]) / page_header_->record_capacity;
  }
}

////////////////////////////////////////////////////////////////////////////////

RecordFileHandler::~RecordFileHandler() { this->close(); }

RC RecordFileHandler::init(
    DiskBufferPool &buffer_pool, LogHandler &log_handler, TableMeta *table_meta, LobFileHandler *lob_handler)
{
  if (disk_buffer_pool_ != nullptr) {
    LOG_ERROR("record file handler has been openned.");
    return RC::RECORD_OPENNED;
  }

  disk_buffer_pool_ = &buffer_pool;
  log_handler_      = &log_handler;
  table_meta_       = table_meta;
  lob_handler_      = lob_handler;

  RC rc = init_free_pages();

  LOG_INFO("open record file handle done. rc=%s", strrc(rc));
  return RC::SUCCESS;
}

void RecordFileHandler::close()
{
  if (disk_buffer_pool_ != nullptr) {
    free_pages_.clear();
    disk_buffer_pool_ = nullptr;
    log_handler_      = nullptr;
    table_meta_       = nullptr;
  }
}

RC RecordFileHandler::init_free_pages()
{
  // 遍历当前文件上所有页面，找到没有满的页面
  // 这个效率很低，会降低启动速度
  // NOTE: 由于是初始化时的动作，所以不需要加锁控制并发

  RC rc = RC::SUCCESS;

  BufferPoolIterator bp_iterator;
  bp_iterator.init(*disk_buffer_pool_, 1);
  unique_ptr<RecordPageHandler> record_page_handler(RecordPageHandler::create(storage_format_));
  PageNum                       current_page_num = 0;

  while (bp_iterator.has_next()) {
    current_page_num = bp_iterator.next();

    rc = record_page_handler->init(*disk_buffer_pool_, *log_handler_, current_page_num, ReadWriteMode::READ_ONLY);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to init record page handler. page num=%d, rc=%d:%s", current_page_num, rc, strrc(rc));
      return rc;
    }

    if (!record_page_handler->is_full()) {
      free_pages_.insert(current_page_num);
    }
    record_page_handler->cleanup();
  }
  LOG_INFO("record file handler init free pages done. free page num=%d, rc=%s", free_pages_.size(), strrc(rc));
  return rc;
}

RC RecordFileHandler::insert_record(const char *data, int record_size, RID *rid)
{
  RC ret = RC::SUCCESS;

  unique_ptr<RecordPageHandler> record_page_handler(RecordPageHandler::create(storage_format_));
  bool                          page_found       = false;
  PageNum                       current_page_num = 0;

  // 当前要访问free_pages对象，所以需要加锁。在非并发编译模式下，不需要考虑这个锁
  lock_.lock();

  // 找到没有填满的页面
  while (!free_pages_.empty()) {
    current_page_num = *free_pages_.begin();

    ret = record_page_handler->init(*disk_buffer_pool_, *log_handler_, current_page_num, ReadWriteMode::READ_WRITE);
    if (OB_FAIL(ret)) {
      lock_.unlock();
      LOG_WARN("failed to init record page handler. page num=%d, rc=%d:%s", current_page_num, ret, strrc(ret));
      return ret;
    }

    if (!record_page_handler->is_full()) {
      page_found = true;
      break;
    }
    record_page_handler->cleanup();
    free_pages_.erase(free_pages_.begin());
  }
  lock_.unlock();  // 如果找到了一个有效的页面，那么此时已经拿到了页面的写锁

  // 找不到就分配一个新的页面
  if (!page_found) {
    Frame *frame = nullptr;
    if ((ret = disk_buffer_pool_->allocate_page(&frame)) != RC::SUCCESS) {
      LOG_ERROR("Failed to allocate page while inserting record. ret:%d", ret);
      return ret;
    }

    current_page_num = frame->page_num();

    ret = record_page_handler->init_empty_page(
        *disk_buffer_pool_, *log_handler_, current_page_num, record_size, table_meta_, lob_handler_);
    if (OB_FAIL(ret)) {
      frame->unpin();
      LOG_ERROR("Failed to init empty page. ret:%d", ret);
      // this is for allocate_page
      return ret;
    }

    // frame 在allocate_page的时候，是有一个pin的，在init_empty_page时又会增加一个，所以这里手动释放一个
    frame->unpin();

    // 这里的加锁顺序看起来与上面是相反的，但是不会出现死锁
    // 上面的逻辑是先加lock锁，然后加页面写锁，这里是先加上
    // 了页面写锁，然后加lock的锁，但是不会引起死锁。
    // 为什么？
    lock_.lock();
    free_pages_.insert(current_page_num);
    lock_.unlock();
  }

  // 找到空闲位置
  return record_page_handler->insert_record(data, rid);
}

RC RecordFileHandler::insert_chunk(const Chunk &chunk, int record_size)
{
  unique_ptr<RecordPageHandler> record_page_handler(RecordPageHandler::create(storage_format_));
  auto                          find_empty_page = [this, record_size](RecordPageHandler *record_page_handler) {
    // 直接分配一个新的页面
    RC     ret   = RC::SUCCESS;
    Frame *frame = nullptr;
    if ((ret = disk_buffer_pool_->allocate_page(&frame)) != RC::SUCCESS) {
      LOG_ERROR("Failed to allocate page while inserting record. ret:%d", ret);
      return ret;
    }
    PageNum current_page_num = frame->page_num();
    ret                      = record_page_handler->init_empty_page(
        *disk_buffer_pool_, *log_handler_, current_page_num, record_size, table_meta_, lob_handler_);
    if (OB_FAIL(ret)) {
      frame->unpin();
      LOG_ERROR("Failed to init empty page. ret:%d", ret);
      return ret;
    }
    frame->unpin();
    lock_.lock();
    free_pages_.insert(current_page_num);
    lock_.unlock();
    return ret;
  };

  for (int start_row = 0; start_row < chunk.rows();) {
    int insert_rows = 0;
    RC  rc          = find_empty_page(record_page_handler.get());
    if (OB_FAIL(rc)) {
      return rc;
    }
    rc = record_page_handler->insert_chunk(chunk, start_row, insert_rows);
    record_page_handler->cleanup();
    if (OB_FAIL(rc) && rc != RC::RECORD_NOMEM) {
      return rc;
    }
    start_row += insert_rows;
  }

  return RC::SUCCESS;
}

RC RecordFileHandler::recover_insert_record(const char *data, int record_size, const RID &rid)
{
  RC ret = RC::SUCCESS;

  unique_ptr<RecordPageHandler> record_page_handler(RecordPageHandler::create(storage_format_));

  ret = record_page_handler->recover_init(*disk_buffer_pool_, rid.page_num);
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to init record page handler. page num=%d, rc=%s", rid.page_num, strrc(ret));
    return ret;
  }

  return record_page_handler->recover_insert_record(data, rid);
}

RC RecordFileHandler::delete_record(const RID *rid)
{
  RC rc = RC::SUCCESS;

  unique_ptr<RecordPageHandler> record_page_handler(RecordPageHandler::create(storage_format_));

  rc = record_page_handler->init(*disk_buffer_pool_, *log_handler_, rid->page_num, ReadWriteMode::READ_WRITE);
  if (OB_FAIL(rc)) {
    LOG_ERROR("Failed to init record page handler.page number=%d. rc=%s", rid->page_num, strrc(rc));
    return rc;
  }

  rc = record_page_handler->delete_record(rid);
  // 📢 这里注意要清理掉资源，否则会与insert_record中的加锁顺序冲突而可能出现死锁
  // delete record的加锁逻辑是拿到页面锁，删除指定记录，然后加上和释放record manager锁
  // insert record是加上 record manager锁，然后拿到指定页面锁再释放record manager锁
  record_page_handler->cleanup();
  if (OB_SUCC(rc)) {
    // 因为这里已经释放了页面锁，并发时，其它线程可能又把该页面填满了，那就不应该再放入 free_pages_
    // 中。但是这里可以不关心，因为在查找空闲页面时，会自动过滤掉已经满的页面
    lock_.lock();
    free_pages_.insert(rid->page_num);
    LOG_TRACE("add free page %d to free page list", rid->page_num);
    lock_.unlock();
  }
  return rc;
}

RC RecordFileHandler::get_record(const RID &rid, Record &record)
{
  unique_ptr<RecordPageHandler> page_handler(RecordPageHandler::create(storage_format_));

  RC rc = page_handler->init(*disk_buffer_pool_, *log_handler_, rid.page_num, ReadWriteMode::READ_WRITE);
  if (OB_FAIL(rc)) {
    LOG_ERROR("Failed to init record page handler.page number=%d", rid.page_num);
    return rc;
  }

  Record inplace_record;
  rc = page_handler->get_record(rid, inplace_record);
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to get record from record page handle. rid=%s, rc=%s", rid.to_string().c_str(), strrc(rc));
    return rc;
  }

  record.copy_data(inplace_record.data(), inplace_record.len());
  record.set_rid(rid);
  return rc;
}

RC RecordFileHandler::visit_record(const RID &rid, function<bool(Record &)> updater)
{
  unique_ptr<RecordPageHandler> page_handler(RecordPageHandler::create(storage_format_));

  RC rc = page_handler->init(*disk_buffer_pool_, *log_handler_, rid.page_num, ReadWriteMode::READ_WRITE);
  if (OB_FAIL(rc)) {
    LOG_ERROR("Failed to init record page handler.page number=%d", rid.page_num);
    return rc;
  }

  Record inplace_record;
  rc = page_handler->get_record(rid, inplace_record);
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to get record from record page handle. rid=%s, rc=%s", rid.to_string().c_str(), strrc(rc));
    return rc;
  }

  // 需要将数据复制出来再修改，否则update_record调用失败但是实际上数据却更新成功了，
  // 会导致数据库状态不正确
  Record record;
  record.copy_data(inplace_record.data(), inplace_record.len());
  record.set_rid(rid);

  bool updated = updater(record);
  if (updated) {
    rc = page_handler->update_record(rid, record.data());
  }
  return rc;
}

ChunkFileScanner::~ChunkFileScanner() { close_scan(); }

RC ChunkFileScanner::close_scan()
{
  if (disk_buffer_pool_ != nullptr) {
    disk_buffer_pool_ = nullptr;
  }

  if (record_page_handler_ != nullptr) {
    record_page_handler_->cleanup();
    delete record_page_handler_;
    record_page_handler_ = nullptr;
  }

  return RC::SUCCESS;
}

RC ChunkFileScanner::open_scan_chunk(
    Table *table, DiskBufferPool &buffer_pool, LogHandler &log_handler, ReadWriteMode mode)
{
  close_scan();

  table_            = table;
  disk_buffer_pool_ = &buffer_pool;
  log_handler_      = &log_handler;
  rw_mode_          = mode;

  RC rc = bp_iterator_.init(buffer_pool, 1);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to init bp iterator. rc=%d:%s", rc, strrc(rc));
    return rc;
  }
  if (table == nullptr || table->table_meta().storage_format() == StorageFormat::ROW_FORMAT) {
    record_page_handler_ = new RowRecordPageHandler();
  } else {
    record_page_handler_ = new PaxRecordPageHandler();
  }

  return rc;
}

RC ChunkFileScanner::next_chunk(Chunk &chunk)
{
  RC rc = RC::SUCCESS;

  while (bp_iterator_.has_next()) {
    PageNum page_num = bp_iterator_.next();
    record_page_handler_->cleanup();
    rc = record_page_handler_->init(*disk_buffer_pool_, *log_handler_, page_num, rw_mode_, table_->lob_handler());
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to init record page handler. page_num=%d, rc=%s", page_num, strrc(rc));
      return rc;
    }
    rc = record_page_handler_->get_chunk(chunk);
    if (rc == RC::SUCCESS) {
      return rc;
    } else if (rc == RC::RECORD_EOF) {
      break;
    } else {
      LOG_WARN("failed to get chunk from page. page_num=%d, rc=%s", page_num, strrc(rc));
      return rc;
    }
  }

  record_page_handler_->cleanup();
  return RC::RECORD_EOF;
}
