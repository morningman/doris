// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "exprs/decimalv3_operators.h"

#include <math.h>
#include <iomanip>
#include <sstream>

#include "exprs/anyval_util.h"
#include "exprs/case_expr.h"
#include "exprs/expr.h"
#include "runtime/tuple_row.h"
#include "util/string_parser.hpp"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number.h"

namespace doris {

    void DecimalV3Operators::init() {}

#define CAST_NUMBER_TO_DECIMAL(from_type, vec_type, to_type)                      \
    Decimal##to_type##Val DecimalV3Operators::cast_to_decimal##to_type##_val(     \
            FunctionContext* context,  const from_type& val) {                    \
        if (val.is_null) return Decimal##to_type##Val::null();                    \
        auto dv = vectorized::convert_to_decimal<vectorized::vec_type,            \
        vectorized::DataTypeDecimal<vectorized::Decimal##to_type>>(val.val,       \
                context->get_return_type().scale);                                \
        return Decimal##to_type##Val(dv.value);                                   \
    }

#define CAST_NUMBER_TO_DECIMALS()                                \
    CAST_NUMBER_TO_DECIMAL(TinyIntVal, DataTypeInt8, 32);        \
    CAST_NUMBER_TO_DECIMAL(SmallIntVal, DataTypeInt16, 32);      \
    CAST_NUMBER_TO_DECIMAL(IntVal, DataTypeInt32, 32);           \
    CAST_NUMBER_TO_DECIMAL(BigIntVal, DataTypeInt64, 32);        \
    CAST_NUMBER_TO_DECIMAL(LargeIntVal, DataTypeInt128, 32);     \
    CAST_NUMBER_TO_DECIMAL(TinyIntVal, DataTypeInt8, 64);        \
    CAST_NUMBER_TO_DECIMAL(SmallIntVal, DataTypeInt16, 64);      \
    CAST_NUMBER_TO_DECIMAL(IntVal, DataTypeInt32, 64);           \
    CAST_NUMBER_TO_DECIMAL(BigIntVal, DataTypeInt64, 64);        \
    CAST_NUMBER_TO_DECIMAL(LargeIntVal, DataTypeInt128, 64);     \
    CAST_NUMBER_TO_DECIMAL(TinyIntVal, DataTypeInt8, 128);       \
    CAST_NUMBER_TO_DECIMAL(SmallIntVal, DataTypeInt16, 128);     \
    CAST_NUMBER_TO_DECIMAL(IntVal, DataTypeInt32, 128);          \
    CAST_NUMBER_TO_DECIMAL(BigIntVal, DataTypeInt64, 128);       \
    CAST_NUMBER_TO_DECIMAL(LargeIntVal, DataTypeInt128, 128);

CAST_NUMBER_TO_DECIMALS();


#define CAST_DATE_TO_DECIMAL(to_type)                                                \
    Decimal##to_type##Val DecimalV3Operators::cast_to_decimal##to_type##_val(        \
            FunctionContext* context,  const DateTimeVal& val) {                     \
        if (val.is_null) return Decimal##to_type##Val::null();                       \
        DateTimeValue dt = DateTimeValue::from_datetime_val(val);                    \
        auto dv = vectorized::convert_to_decimal<vectorized::DataTypeInt64,          \
        vectorized::DataTypeDecimal<vectorized::Decimal##to_type>>(dt.to_int64(),    \
                context->get_return_type().scale);                                   \
        return Decimal##to_type##Val(dv.value);                                      \
    }

#define CAST_DATE_TO_DECIMALS()    \
    CAST_DATE_TO_DECIMAL(32)       \
    CAST_DATE_TO_DECIMAL(64)       \
    CAST_DATE_TO_DECIMAL(128)      \
 
CAST_DATE_TO_DECIMALS();


#define CAST_STRING_TO_DECIMAL(to_type)                                           \
    Decimal##to_type##Val DecimalV3Operators::cast_to_decimal##to_type##_val(     \
            FunctionContext* context,  const StringVal& val) {                    \
        if (val.is_null) return Decimal##to_type##Val::null();                    \
        StringParser::ParseResult result = StringParser::PARSE_SUCCESS;           \
        auto value = StringParser::string_to_decimal<int##to_type##_t>(                             \
                (const char*)val.ptr, val.len,                                    \
                context->get_return_type().precision,                             \
                context->get_return_type().scale, &result);                       \
        LOG(INFO) << "liaoxin string_to_decimal res: " << value; \
        if (result == StringParser::PARSE_FAILURE) {                              \
            return Decimal##to_type##Val::null();                                 \
        }                                                                         \
        return Decimal##to_type##Val(value);                                      \
    }

#define CAST_STRING_TO_DECIMALS()    \
    CAST_STRING_TO_DECIMAL(32)       \
    CAST_STRING_TO_DECIMAL(64)       \
    CAST_STRING_TO_DECIMAL(128)      \
 
CAST_STRING_TO_DECIMALS();

} // namespace doris
