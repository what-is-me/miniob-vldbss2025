/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "common/type/big_integer_type.h"
#include "common/type/date_type.h"
#include "common/type/text_type.h"
#include "common/type/char_type.h"
#include "common/type/float_type.h"
#include "common/type/integer_type.h"
#include "common/type/data_type.h"
#include "common/type/vector_type.h"

// Todo: 实现新数据类型
// your code here

// 通过数据类型得到索引值，返回对应类型的指针
array<unique_ptr<DataType>, static_cast<int>(AttrType::MAXTYPE)> DataType::type_instances_ = {
    make_unique<DataType>(AttrType::UNDEFINED),
    make_unique<CharType>(),
    make_unique<BigIntegerType>(),
    make_unique<DateType>(),
    make_unique<TextType>(),
    make_unique<IntegerType>(),
    make_unique<FloatType>(),
    make_unique<VectorType>(),
    make_unique<DataType>(AttrType::BOOLEANS),
};