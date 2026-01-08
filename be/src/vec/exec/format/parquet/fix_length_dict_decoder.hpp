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

#include "common/config.h"
#include "util/bit_util.h"
#include "util/memcpy_inlined.h"
#include "vec/columns/column_dictionary.h"
#include "vec/columns/column_nullable.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/exec/format/parquet/decoder.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

template <tparquet::Type::type type>
struct PhysicalTypeTraits {};

template <>
struct PhysicalTypeTraits<tparquet::Type::INT32> {
    using CppType = int32_t;
};

template <>
struct PhysicalTypeTraits<tparquet::Type::INT64> {
    using CppType = int64_t;
};

template <>
struct PhysicalTypeTraits<tparquet::Type::INT96> {
    using CppType = ParquetInt96;
};

template <>
struct PhysicalTypeTraits<tparquet::Type::FLOAT> {
    using CppType = float;
};

template <>
struct PhysicalTypeTraits<tparquet::Type::DOUBLE> {
    using CppType = double;
};

template <>
struct PhysicalTypeTraits<tparquet::Type::FIXED_LEN_BYTE_ARRAY> {
    using CppType = Slice;
};

template <tparquet::Type::type PhysicalType>
class FixLengthDictDecoder final : public BaseDictDecoder {
public:
    using cppType = PhysicalTypeTraits<PhysicalType>::CppType;
    FixLengthDictDecoder() = default;
    ~FixLengthDictDecoder() override = default;

    Status decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                         ColumnSelectVector& select_vector, bool is_dict_filter) override {
        if (select_vector.has_filter()) {
            return _decode_values<true>(doris_column, data_type, select_vector, is_dict_filter);
        } else {
            return _decode_values<false>(doris_column, data_type, select_vector, is_dict_filter);
        }
    }

    template <bool has_filter>
    Status _decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                          ColumnSelectVector& select_vector, bool is_dict_filter) {
        size_t non_null_size = select_vector.num_values() - select_vector.num_nulls();
        if (doris_column->is_column_dictionary() &&
            assert_cast<ColumnDictI32&>(*doris_column).dict_size() == 0) {
            std::vector<StringRef> dict_items;

            char* dict_item_address = (char*)_dict.get();
            dict_items.reserve(_dict_items.size());
            for (int i = 0; i < _dict_items.size(); ++i) {
                dict_items.emplace_back(dict_item_address, _type_length);
                dict_item_address += _type_length;
            }
            assert_cast<ColumnDictI32&>(*doris_column)
                    .insert_many_dict_data(dict_items.data(),
                                           cast_set<uint32_t>(dict_items.size()));
        }
        _indexes.resize(non_null_size);
        _index_batch_decoder->GetBatch(_indexes.data(), cast_set<uint32_t>(non_null_size));

        if (doris_column->is_column_dictionary() || is_dict_filter) {
            return _decode_dict_values<has_filter>(doris_column, select_vector, is_dict_filter);
        }

        // Apply cache-aware optimization: if dictionary is small, decode all values
        // regardless of filter to leverage cache locality and vectorization
        bool use_cache_aware = _should_use_cache_aware_optimization() &&
                               config::parquet_cache_aware_dict_decoder_enable;
        if (use_cache_aware && has_filter) {
            // For small dictionaries, decode all values ignoring filter
            return _decode_fixed_values_batch(doris_column, data_type, select_vector);
        }
        return _decode_fixed_values<has_filter>(doris_column, data_type, select_vector);
    }

protected:
    // Get dictionary size in bytes
    size_t _get_dict_size() const override {
        if constexpr (PhysicalType == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
            return _dict_items.size() * _type_length;
        } else {
            return _dict_items.size() * sizeof(cppType);
        }
    }

    // Batch decode all values ignoring filter (for small dictionaries)
    // This leverages cache locality and vectorization for small dictionaries.
    // For small dictionaries, the cost of dictionary lookup is low, so we decode
    // all values (including filtered ones) to benefit from sequential access patterns.
    // Note: _indexes should already be populated by _decode_values before calling this method.
    Status _decode_fixed_values_batch(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                      ColumnSelectVector& select_vector) {
        size_t primitive_length = remove_nullable(data_type)->get_size_of_value_in_memory();
        size_t data_index = doris_column->size() * primitive_length;
        size_t total_values = select_vector.num_values();
        size_t scale_size = total_values * (_type_length / primitive_length);
        doris_column->resize(doris_column->size() + scale_size);
        char* raw_data = const_cast<char*>(doris_column->get_raw_data().data);
        
        // Decode all non-null values in batch (including filtered ones)
        // The filter will be applied later in the pipeline to remove unwanted values
        // _indexes is already populated in _decode_values
        if constexpr (PhysicalType == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
            char* dst_ptr = raw_data + data_index;
            size_t dict_index = 0;
            ColumnSelectVector::DataReadType read_type;
            // Process all values including filtered ones for cache-aware optimization
            while (size_t run_length = select_vector.get_next_run<true>(&read_type)) {
                if (read_type == ColumnSelectVector::CONTENT ||
                    read_type == ColumnSelectVector::FILTERED_CONTENT) {
                    // Decode both content and filtered content
                    for (size_t i = 0; i < run_length; ++i) {
                        auto& slice = _dict_items[_indexes[dict_index++]];
                        doris::memcpy_inlined(dst_ptr, slice.get_data(), _type_length);
                        dst_ptr += _type_length;
                    }
                } else if (read_type == ColumnSelectVector::NULL_DATA ||
                           read_type == ColumnSelectVector::FILTERED_NULL) {
                    // Skip null positions
                    dst_ptr += run_length * _type_length;
                }
            }
        } else {
            cppType* dst_ptr = reinterpret_cast<cppType*>(raw_data + data_index);
            size_t dict_index = 0;
            ColumnSelectVector::DataReadType read_type;
            while (size_t run_length = select_vector.get_next_run<true>(&read_type)) {
                if (read_type == ColumnSelectVector::CONTENT ||
                    read_type == ColumnSelectVector::FILTERED_CONTENT) {
                    // Decode both content and filtered content
                    for (size_t i = 0; i < run_length; ++i) {
                        *dst_ptr++ = _dict_items[_indexes[dict_index++]];
                    }
                } else if (read_type == ColumnSelectVector::NULL_DATA ||
                           read_type == ColumnSelectVector::FILTERED_NULL) {
                    // Skip null positions
                    dst_ptr += run_length;
                }
            }
        }
        
        return Status::OK();
    }
    template <bool has_filter>
    Status _decode_fixed_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                ColumnSelectVector& select_vector) {
        size_t primitive_length = remove_nullable(data_type)->get_size_of_value_in_memory();
        size_t data_index = doris_column->size() * primitive_length;
        size_t scale_size = (select_vector.num_values() - select_vector.num_filtered()) *
                            (_type_length / primitive_length);
        doris_column->resize(doris_column->size() + scale_size);
        // doris_column is of type MutableColumnPtr, which uses get_raw_data
        // to return a StringRef, hence the use of const_cast.
        char* raw_data = const_cast<char*>(doris_column->get_raw_data().data);
        size_t dict_index = 0;
        ColumnSelectVector::DataReadType read_type;
        while (size_t run_length = select_vector.get_next_run<has_filter>(&read_type)) {
            switch (read_type) {
            case ColumnSelectVector::CONTENT: {
                if constexpr (PhysicalType == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
                    // Optimized path: use memcpy_inlined and reduce address calculations
                    char* dst_ptr = raw_data + data_index;
                    size_t i = 0;
                    // Loop unrolling: process 4 elements at a time
                    for (; i + 4 <= run_length; i += 4) {
                        auto& slice0 = _dict_items[_indexes[dict_index++]];
                        doris::memcpy_inlined(dst_ptr, slice0.get_data(), _type_length);
                        dst_ptr += _type_length;
                        
                        auto& slice1 = _dict_items[_indexes[dict_index++]];
                        doris::memcpy_inlined(dst_ptr, slice1.get_data(), _type_length);
                        dst_ptr += _type_length;
                        
                        auto& slice2 = _dict_items[_indexes[dict_index++]];
                        doris::memcpy_inlined(dst_ptr, slice2.get_data(), _type_length);
                        dst_ptr += _type_length;
                        
                        auto& slice3 = _dict_items[_indexes[dict_index++]];
                        doris::memcpy_inlined(dst_ptr, slice3.get_data(), _type_length);
                        dst_ptr += _type_length;
                    }
                    // Process remaining elements
                    for (; i < run_length; ++i) {
                        auto& slice = _dict_items[_indexes[dict_index++]];
                        doris::memcpy_inlined(dst_ptr, slice.get_data(), _type_length);
                        dst_ptr += _type_length;
                    }
                    data_index = dst_ptr - raw_data;
                } else {
                    // Original path for non-FIXED_LEN_BYTE_ARRAY types
                    for (size_t i = 0; i < run_length; ++i) {
                        *(cppType*)(raw_data + data_index) = _dict_items[_indexes[dict_index++]];
                        data_index += _type_length;
                    }
                }
                break;
            }
            case ColumnSelectVector::NULL_DATA: {
                data_index += run_length * _type_length;
                break;
            }
            case ColumnSelectVector::FILTERED_CONTENT: {
                dict_index += run_length;
                break;
            }
            case ColumnSelectVector::FILTERED_NULL: {
                // do nothing
                break;
            }
            }
        }
        return Status::OK();
    }

    Status set_dict(DorisUniqueBufferPtr<uint8_t>& dict, int32_t length,
                    size_t num_values) override {
        if (num_values * _type_length != length) {
            return Status::Corruption("Wrong dictionary data for fixed length type");
        }
        _dict = std::move(dict);
        if (_dict == nullptr) {
            return Status::Corruption("Wrong dictionary data for byte array type, dict is null.");
        }
        char* dict_item_address = reinterpret_cast<char*>(_dict.get());
        _dict_items.resize(num_values);
        for (size_t i = 0; i < num_values; ++i) {
            if constexpr (PhysicalType == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
                _dict_items[i] = Slice {dict_item_address, (size_t)_type_length};
            } else {
                _dict_items[i] = *((cppType*)dict_item_address);
            }
            dict_item_address += _type_length;
        }
        return Status::OK();
    }

    Status read_dict_values_to_column(MutableColumnPtr& doris_column) override {
        size_t dict_items_size = _dict_items.size();
        std::vector<StringRef> dict_values;
        dict_values.reserve(dict_items_size);
        auto* dict_item_address = (const char*)_dict.get();
        for (size_t i = 0; i < dict_items_size; ++i) {
            dict_values.emplace_back(dict_item_address, _type_length);
            dict_item_address += _type_length;
        }
        doris_column->insert_many_strings(&dict_values[0], dict_items_size);
        return Status::OK();
    }

    MutableColumnPtr convert_dict_column_to_string_column(const ColumnInt32* dict_column) override {
        auto res = ColumnString::create();
        std::vector<StringRef> dict_values;
        dict_values.reserve(dict_column->size());
        const auto& data = dict_column->get_data();
        auto* dict_item_address = (const char*)_dict.get();

        for (size_t i = 0; i < dict_column->size(); ++i) {
            dict_values.emplace_back(dict_item_address + data[i] * _type_length, _type_length);
        }
        res->insert_many_strings(&dict_values[0], dict_values.size());
        return res;
    }
    // For dictionary encoding
    std::vector<typename PhysicalTypeTraits<PhysicalType>::CppType> _dict_items;
};
#include "common/compile_check_end.h"

} // namespace doris::vectorized
