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

#include "util/arrow/row_batch.h"

#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <glog/logging.h>
#include <stdint.h>

#include <algorithm>
#include <cstdlib>
#include <memory>
#include <utility>
#include <vector>

#include "runtime/define_primitive_type.h"
#include "runtime/descriptors.h"
#include "runtime/types.h"
#include "util/arrow/block_convertor.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_agg_state.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {

Status convert_to_arrow_type(const vectorized::DataTypePtr& origin_type,
                             std::shared_ptr<arrow::DataType>* result,
                             const std::string& timezone) {
    auto type = get_serialized_type(origin_type);
    switch (type->get_primitive_type()) {
    case TYPE_NULL:
        *result = arrow::null();
        break;
    case TYPE_TINYINT:
        *result = arrow::int8();
        break;
    case TYPE_SMALLINT:
        *result = arrow::int16();
        break;
    case TYPE_INT:
        *result = arrow::int32();
        break;
    case TYPE_BIGINT:
        *result = arrow::int64();
        break;
    case TYPE_FLOAT:
        *result = arrow::float32();
        break;
    case TYPE_DOUBLE:
        *result = arrow::float64();
        break;
    case TYPE_TIMEV2:
        *result = arrow::float64();
        break;
    case TYPE_IPV4:
        // ipv4 is uint32, but parquet not uint32, it's will be convert to int64
        // so use int32 directly
        *result = arrow::int32();
        break;
    case TYPE_IPV6:
        *result = arrow::utf8();
        break;
    case TYPE_LARGEINT:
    case TYPE_VARCHAR:
    case TYPE_CHAR:
    case TYPE_DATE:
    case TYPE_DATETIME:
    case TYPE_STRING:
    case TYPE_JSONB:
        *result = arrow::utf8();
        break;
    case TYPE_DATEV2:
        *result = std::make_shared<arrow::Date32Type>();
        break;
    case TYPE_DATETIMEV2:
        if (type->get_scale() > 3) {
            *result = std::make_shared<arrow::TimestampType>(arrow::TimeUnit::MICRO, timezone);
        } else if (type->get_scale() > 0) {
            *result = std::make_shared<arrow::TimestampType>(arrow::TimeUnit::MILLI, timezone);
        } else {
            *result = std::make_shared<arrow::TimestampType>(arrow::TimeUnit::SECOND, timezone);
        }
        break;
    case TYPE_DECIMALV2:
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128I:
        *result = std::make_shared<arrow::Decimal128Type>(type->get_precision(), type->get_scale());
        break;
    case TYPE_DECIMAL256:
        *result = std::make_shared<arrow::Decimal256Type>(type->get_precision(), type->get_scale());
        break;
    case TYPE_BOOLEAN:
        *result = arrow::boolean();
        break;
    case TYPE_ARRAY: {
        const auto* type_arr = assert_cast<const vectorized::DataTypeArray*>(
                vectorized::remove_nullable(type).get());
        std::shared_ptr<arrow::DataType> item_type;
        static_cast<void>(convert_to_arrow_type(type_arr->get_nested_type(), &item_type, timezone));
        *result = std::make_shared<arrow::ListType>(item_type);
        break;
    }
    case TYPE_MAP: {
        const auto* type_map = assert_cast<const vectorized::DataTypeMap*>(
                vectorized::remove_nullable(type).get());
        std::shared_ptr<arrow::DataType> key_type;
        std::shared_ptr<arrow::DataType> val_type;
        static_cast<void>(convert_to_arrow_type(type_map->get_key_type(), &key_type, timezone));
        static_cast<void>(convert_to_arrow_type(type_map->get_value_type(), &val_type, timezone));
        *result = std::make_shared<arrow::MapType>(key_type, val_type);
        break;
    }
    case TYPE_STRUCT: {
        const auto* type_struct = assert_cast<const vectorized::DataTypeStruct*>(
                vectorized::remove_nullable(type).get());
        std::vector<std::shared_ptr<arrow::Field>> fields;
        for (size_t i = 0; i < type_struct->get_elements().size(); i++) {
            std::shared_ptr<arrow::DataType> field_type;
            static_cast<void>(
                    convert_to_arrow_type(type_struct->get_element(i), &field_type, timezone));
            fields.push_back(
                    std::make_shared<arrow::Field>(type_struct->get_element_name(i), field_type,
                                                   type_struct->get_element(i)->is_nullable()));
        }
        *result = std::make_shared<arrow::StructType>(fields);
        break;
    }
    case TYPE_VARIANT: {
        *result = arrow::utf8();
        break;
    }
    case TYPE_QUANTILE_STATE:
    case TYPE_BITMAP:
    case TYPE_HLL: {
        *result = arrow::binary();
        break;
    }
    default:
        return Status::InvalidArgument("Unknown primitive type({}) convert to Arrow type",
                                       type->get_name());
    }
    return Status::OK();
}

Status get_arrow_schema_from_block(const vectorized::Block& block,
                                   std::shared_ptr<arrow::Schema>* result,
                                   const std::string& timezone) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (const auto& type_and_name : block) {
        std::shared_ptr<arrow::DataType> arrow_type;
        RETURN_IF_ERROR(convert_to_arrow_type(type_and_name.type, &arrow_type, timezone));
        fields.push_back(std::make_shared<arrow::Field>(type_and_name.name, arrow_type,
                                                        type_and_name.type->is_nullable()));
    }
    *result = arrow::schema(std::move(fields));
    return Status::OK();
}

Status get_arrow_schema_from_expr_ctxs(const vectorized::VExprContextSPtrs& output_vexpr_ctxs,
                                       std::shared_ptr<arrow::Schema>* result,
                                       const std::string& timezone) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (int i = 0; i < output_vexpr_ctxs.size(); i++) {
        std::shared_ptr<arrow::DataType> arrow_type;
        auto root_expr = output_vexpr_ctxs.at(i)->root();
        RETURN_IF_ERROR(convert_to_arrow_type(root_expr->data_type(), &arrow_type, timezone));
        auto field_name = root_expr->is_slot_ref() && !root_expr->expr_label().empty()
                                  ? root_expr->expr_label()
                                  : fmt::format("{}_{}", root_expr->data_type()->get_name(), i);
        fields.push_back(
                std::make_shared<arrow::Field>(field_name, arrow_type, root_expr->is_nullable()));
    }
    *result = arrow::schema(std::move(fields));
    return Status::OK();
}

Status serialize_record_batch(const arrow::RecordBatch& record_batch, std::string* result) {
    // create sink memory buffer outputstream with the computed capacity
    int64_t capacity;
    arrow::Status a_st = arrow::ipc::GetRecordBatchSize(record_batch, &capacity);
    if (!a_st.ok()) {
        return Status::InternalError("GetRecordBatchSize failure, reason: {}", a_st.ToString());
    }
    auto sink_res = arrow::io::BufferOutputStream::Create(capacity, arrow::default_memory_pool());
    if (!sink_res.ok()) {
        return Status::InternalError("create BufferOutputStream failure, reason: {}",
                                     sink_res.status().ToString());
    }
    std::shared_ptr<arrow::io::BufferOutputStream> sink = sink_res.ValueOrDie();
    // create RecordBatch Writer
    auto res = arrow::ipc::MakeStreamWriter(sink.get(), record_batch.schema());
    if (!res.ok()) {
        return Status::InternalError("open RecordBatchStreamWriter failure, reason: {}",
                                     res.status().ToString());
    }
    // write RecordBatch to memory buffer outputstream
    std::shared_ptr<arrow::ipc::RecordBatchWriter> record_batch_writer = res.ValueOrDie();
    a_st = record_batch_writer->WriteRecordBatch(record_batch);
    if (!a_st.ok()) {
        return Status::InternalError("write record batch failure, reason: {}", a_st.ToString());
    }
    a_st = record_batch_writer->Close();
    if (!a_st.ok()) {
        return Status::InternalError("Close failed, reason: {}", a_st.ToString());
    }
    auto finish_res = sink->Finish();
    if (!finish_res.ok()) {
        return Status::InternalError("allocate result buffer failure, reason: {}",
                                     finish_res.status().ToString());
    }
    *result = finish_res.ValueOrDie()->ToString();
    // close the sink
    a_st = sink->Close();
    if (!a_st.ok()) {
        return Status::InternalError("Close failed, reason: {}", a_st.ToString());
    }
    return Status::OK();
}

Status serialize_arrow_schema(std::shared_ptr<arrow::Schema>* schema, std::string* result) {
    auto make_empty_result = arrow::RecordBatch::MakeEmpty(*schema);
    if (!make_empty_result.ok()) {
        return Status::InternalError("serialize_arrow_schema failed, reason: {}",
                                     make_empty_result.status().ToString());
    }
    auto batch = make_empty_result.ValueOrDie();
    return serialize_record_batch(*batch, result);
}

} // namespace doris
