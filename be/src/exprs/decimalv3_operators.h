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

#pragma once

#include <stdint.h>

#include "udf/udf.h"

namespace doris {

class Expr;
struct ExprValue;
class TupleRow;

/// Implementation of the decimal operators. These include the cast,
/// arithmetic and binary operators.
class DecimalV3Operators {
public:
    static void init();

    static Decimal32Val cast_to_decimal32_val(FunctionContext*, const TinyIntVal&);
    static Decimal32Val cast_to_decimal32_val(FunctionContext*, const SmallIntVal&);
    static Decimal32Val cast_to_decimal32_val(FunctionContext*, const IntVal&);
    static Decimal32Val cast_to_decimal32_val(FunctionContext*, const BigIntVal&);
    static Decimal32Val cast_to_decimal32_val(FunctionContext*, const LargeIntVal&);
    static Decimal32Val cast_to_decimal32_val(FunctionContext*, const FloatVal&);
    static Decimal32Val cast_to_decimal32_val(FunctionContext*, const DoubleVal&);
    static Decimal32Val cast_to_decimal32_val(FunctionContext*, const DateTimeVal&);
    static Decimal32Val cast_to_decimal32_val(FunctionContext*, const StringVal&);

    static Decimal64Val cast_to_decimal64_val(FunctionContext*, const TinyIntVal&);
    static Decimal64Val cast_to_decimal64_val(FunctionContext*, const SmallIntVal&);
    static Decimal64Val cast_to_decimal64_val(FunctionContext*, const IntVal&);
    static Decimal64Val cast_to_decimal64_val(FunctionContext*, const BigIntVal&);
    static Decimal64Val cast_to_decimal64_val(FunctionContext*, const LargeIntVal&);
    static Decimal64Val cast_to_decimal64_val(FunctionContext*, const FloatVal&);
    static Decimal64Val cast_to_decimal64_val(FunctionContext*, const DoubleVal&);
    static Decimal64Val cast_to_decimal64_val(FunctionContext*, const DateTimeVal&);
    static Decimal64Val cast_to_decimal64_val(FunctionContext*, const StringVal&);

    static Decimal128Val cast_to_decimal128_val(FunctionContext*, const TinyIntVal&);
    static Decimal128Val cast_to_decimal128_val(FunctionContext*, const SmallIntVal&);
    static Decimal128Val cast_to_decimal128_val(FunctionContext*, const IntVal&);
    static Decimal128Val cast_to_decimal128_val(FunctionContext*, const BigIntVal&);
    static Decimal128Val cast_to_decimal128_val(FunctionContext*, const LargeIntVal&);
    static Decimal128Val cast_to_decimal128_val(FunctionContext*, const FloatVal&);
    static Decimal128Val cast_to_decimal128_val(FunctionContext*, const DoubleVal&);
    static Decimal128Val cast_to_decimal128_val(FunctionContext*, const DateTimeVal&);
    static Decimal128Val cast_to_decimal128_val(FunctionContext*, const StringVal&);

    // static BooleanVal cast_to_boolean_val(FunctionContext*, const Decimal128Val&);
    // static TinyIntVal cast_to_tiny_int_val(FunctionContext*, const Decimal128Val&);
    // static SmallIntVal cast_to_small_int_val(FunctionContext*, const Decimal128Val&);
    // static IntVal cast_to_int_val(FunctionContext*, const Decimal128Val&);
    // static BigIntVal cast_to_big_int_val(FunctionContext*, const Decimal128Val&);
    // static LargeIntVal cast_to_large_int_val(FunctionContext*, const Decimal128Val&);
    // static FloatVal cast_to_float_val(FunctionContext*, const Decimal128Val&);
    // static DoubleVal cast_to_double_val(FunctionContext*, const Decimal128Val&);
    // static StringVal cast_to_string_val(FunctionContext*, const Decimal128Val&);
    // static DateTimeVal cast_to_datetime_val(FunctionContext*, const Decimal128Val&);
    // static DateTimeVal cast_to_date_val(FunctionContext*, const Decimal128Val&);

    // static Decimal128Val add_decimal128_val_decimal128_val(FunctionContext*, const Decimal128Val&,
                                                        // const Decimal128Val&);
    // static Decimal128Val subtract_decimal128_val_decimal128_val(FunctionContext*, const Decimal128Val&,
                                                             // const Decimal128Val&);
    // static Decimal128Val multiply_decimal128_val_decimal128_val(FunctionContext*, const Decimal128Val&,
                                                             // const Decimal128Val&);
    // static Decimal128Val divide_decimal128_val_decimal128_val(FunctionContext*, const Decimal128Val&,
                                                           // const Decimal128Val&);
    // static Decimal128Val mod_decimal128_val_decimal128_val(FunctionContext*, const Decimal128Val&,
                                                        // const Decimal128Val&);

    // static BooleanVal eq_decimal128_val_decimal128_val(FunctionContext*, const Decimal128Val&,
                                                     // const Decimal128Val&);
    // static BooleanVal ne_decimal128_val_decimal128_val(FunctionContext*, const Decimal128Val&,
                                                     // const Decimal128Val&);
    // static BooleanVal gt_decimal128_val_decimal128_val(FunctionContext*, const Decimal128Val&,
                                                     // const Decimal128Val&);
    // static BooleanVal lt_decimal128_val_decimal128_val(FunctionContext*, const Decimal128Val&,
                                                     // const Decimal128Val&);
    // static BooleanVal ge_decimal128_val_decimal128_val(FunctionContext*, const Decimal128Val&,
                                                     // const Decimal128Val&);
    // static BooleanVal le_decimal128_val_decimal128_val(FunctionContext*, const Decimal128Val&,
                                                     // const Decimal128Val&);
};

} // namespace doris
