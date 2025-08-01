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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/AggregateFunctionDistinct.h
// and modified by Doris

#pragma once

#include <assert.h>
#include <glog/logging.h>
#include <stddef.h>

#include <algorithm>
#include <memory>
#include <new>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/io/io_helper.h"
#include "vec/io/var_int.h"

namespace doris {
#include "common/compile_check_begin.h"
namespace vectorized {
class Arena;
class BufferReadable;
class BufferWritable;
template <PrimitiveType>
class ColumnVector;
} // namespace vectorized
} // namespace doris
template <typename, typename>
struct DefaultHash;

namespace doris::vectorized {

template <PrimitiveType T, bool stable>
struct AggregateFunctionDistinctSingleNumericData {
    /// When creating, the hash table must be small.
    using Container = std::conditional_t<
            stable, phmap::flat_hash_map<typename PrimitiveTypeTraits<T>::CppType, uint32_t>,
            phmap::flat_hash_set<typename PrimitiveTypeTraits<T>::CppType>>;
    using Self = AggregateFunctionDistinctSingleNumericData<T, stable>;
    Container data;

    void clear() { data.clear(); }

    void add(const IColumn** columns, size_t /* columns_num */, size_t row_num, Arena*) {
        const auto& vec =
                assert_cast<const ColumnVector<T>&, TypeCheckOnRelease::DISABLE>(*columns[0])
                        .get_data();
        if constexpr (stable) {
            data.emplace(vec[row_num], data.size());
        } else {
            data.insert(vec[row_num]);
        }
    }

    void merge(const Self& rhs, Arena*) {
        DCHECK(!stable);
        if constexpr (!stable) {
            data.merge(Container(rhs.data));
        }
    }

    void serialize(BufferWritable& buf) const {
        DCHECK(!stable);
        if constexpr (!stable) {
            buf.write_var_uint(data.size());
            for (const auto& value : data) {
                buf.write_binary(value);
            }
        }
    }

    void deserialize(BufferReadable& buf, Arena*) {
        DCHECK(!stable);
        if constexpr (!stable) {
            uint64_t new_size = 0;
            buf.read_var_uint(new_size);
            typename PrimitiveTypeTraits<T>::CppType x;
            for (size_t i = 0; i < new_size; ++i) {
                buf.read_binary(x);
                data.insert(x);
            }
        }
    }

    MutableColumns get_arguments(const DataTypes& argument_types) const {
        MutableColumns argument_columns;
        argument_columns.emplace_back(argument_types[0]->create_column());

        if constexpr (stable) {
            argument_columns[0]->resize(data.size());
            auto ptr = (typename PrimitiveTypeTraits<T>::CppType*)const_cast<char*>(
                    argument_columns[0]->get_raw_data().data);
            for (auto it : data) {
                ptr[it.second] = it.first;
            }
        } else {
            for (const auto& elem : data) {
                argument_columns[0]->insert(Field::create_field<T>(elem));
            }
        }

        return argument_columns;
    }
};

template <bool stable>
struct AggregateFunctionDistinctGenericData {
    /// When creating, the hash table must be small.
    using Container = std::conditional_t<stable, phmap::flat_hash_map<StringRef, uint32_t>,
                                         phmap::flat_hash_set<StringRef, StringRefHash>>;
    using Self = AggregateFunctionDistinctGenericData;
    Container data;

    void clear() { data.clear(); }

    void merge(const Self& rhs, Arena* arena) {
        DCHECK(!stable);
        if constexpr (!stable) {
            for (const auto& elem : rhs.data) {
                StringRef key = elem;
                key.data = arena->insert(key.data, key.size);
                data.emplace(key);
            }
        }
    }

    void serialize(BufferWritable& buf) const {
        DCHECK(!stable);
        if constexpr (!stable) {
            buf.write_var_uint(data.size());
            for (const auto& elem : data) {
                buf.write_binary(elem);
            }
        }
    }

    void deserialize(BufferReadable& buf, Arena* arena) {
        DCHECK(!stable);
        if constexpr (!stable) {
            UInt64 size;
            buf.read_var_uint(size);

            StringRef ref;
            for (size_t i = 0; i < size; ++i) {
                buf.read_binary(ref);
                data.insert(ref);
            }
        }
    }
};

template <bool stable>
struct AggregateFunctionDistinctSingleGenericData
        : public AggregateFunctionDistinctGenericData<stable> {
    using Base = AggregateFunctionDistinctGenericData<stable>;
    using Base::data;
    void add(const IColumn** columns, size_t /* columns_num */, size_t row_num, Arena* arena) {
        auto key = columns[0]->get_data_at(row_num);
        key.data = arena->insert(key.data, key.size);

        if constexpr (stable) {
            data.emplace(key, data.size());
        } else {
            data.insert(key);
        }
    }

    MutableColumns get_arguments(const DataTypes& argument_types) const {
        MutableColumns argument_columns;
        argument_columns.emplace_back(argument_types[0]->create_column());
        if constexpr (stable) {
            std::vector<StringRef> tmp(data.size());
            for (auto it : data) {
                tmp[it.second] = it.first;
            }
            for (int i = 0; i < data.size(); i++) {
                argument_columns[0]->insert_data(tmp[i].data, tmp[i].size);
            }
        } else {
            for (const auto& elem : data) {
                argument_columns[0]->insert_data(elem.data, elem.size);
            }
        }

        return argument_columns;
    }
};

template <bool stable>
struct AggregateFunctionDistinctMultipleGenericData
        : public AggregateFunctionDistinctGenericData<stable> {
    using Base = AggregateFunctionDistinctGenericData<stable>;
    using Base::data;
    void add(const IColumn** columns, size_t columns_num, size_t row_num, Arena* arena) {
        const char* begin = nullptr;
        StringRef key(begin, 0);
        for (size_t i = 0; i < columns_num; ++i) {
            auto cur_ref = columns[i]->serialize_value_into_arena(row_num, *arena, begin);
            key.data = cur_ref.data - key.size;
            key.size += cur_ref.size;
        }

        if constexpr (stable) {
            data.emplace(key, data.size());
        } else {
            data.emplace(key);
        }
    }

    MutableColumns get_arguments(const DataTypes& argument_types) const {
        MutableColumns argument_columns(argument_types.size());
        for (size_t i = 0; i < argument_types.size(); ++i) {
            argument_columns[i] = argument_types[i]->create_column();
        }

        if constexpr (stable) {
            std::vector<StringRef> tmp(data.size());
            for (auto it : data) {
                tmp[it.second] = it.first;
            }
            for (int i = 0; i < data.size(); i++) {
                const char* begin = tmp[i].data;
                for (auto& column : argument_columns) {
                    begin = column->deserialize_and_insert_from_arena(begin);
                }
            }
        } else {
            for (const auto& elem : data) {
                const char* begin = elem.data;
                for (auto& column : argument_columns) {
                    begin = column->deserialize_and_insert_from_arena(begin);
                }
            }
        }

        return argument_columns;
    }
};

/** Adaptor for aggregate functions.
  * Adding -Distinct suffix to aggregate function
**/
template <template <bool stable> typename Data, bool stable = false>
class AggregateFunctionDistinct
        : public IAggregateFunctionDataHelper<Data<stable>,
                                              AggregateFunctionDistinct<Data, stable>> {
private:
    size_t prefix_size;
    AggregateFunctionPtr nested_func;
    size_t arguments_num;

    AggregateDataPtr get_nested_place(AggregateDataPtr __restrict place) const noexcept {
        return place + prefix_size;
    }

    ConstAggregateDataPtr get_nested_place(ConstAggregateDataPtr __restrict place) const noexcept {
        return place + prefix_size;
    }

public:
    AggregateFunctionDistinct(AggregateFunctionPtr nested_func_, const DataTypes& arguments)
            : IAggregateFunctionDataHelper<Data<stable>, AggregateFunctionDistinct<Data, stable>>(
                      arguments),
              nested_func(std::move(nested_func_)),
              arguments_num(arguments.size()) {
        size_t nested_size = nested_func->align_of_data();
        CHECK_GT(nested_size, 0);
        prefix_size = (sizeof(Data<stable>) + nested_size - 1) / nested_size * nested_size;
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena* arena) const override {
        this->data(place).add(columns, arguments_num, row_num, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena* arena) const override {
        this->data(place).merge(this->data(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena* arena) const override {
        this->data(place).deserialize(buf, arena);
    }

    void insert_result_into(ConstAggregateDataPtr targetplace, IColumn& to) const override {
        auto* place = const_cast<AggregateDataPtr>(targetplace);
        auto arguments = this->data(place).get_arguments(this->argument_types);
        ColumnRawPtrs arguments_raw(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i) {
            arguments_raw[i] = arguments[i].get();
        }

        assert(!arguments.empty());
        Arena arena;
        nested_func->add_batch_single_place(arguments[0]->size(), get_nested_place(place),
                                            arguments_raw.data(), &arena);
        nested_func->insert_result_into(get_nested_place(place), to);
        // for distinct agg function, the real calculate is add_batch_single_place at last step of insert_result_into function.
        // but with distinct agg and over() window function together, the result will be inserted into many times with different rows
        // so we need to clear the data, thus not to affect the next insert_result_into
        this->data(place).clear();
    }

    void reset(AggregateDataPtr place) const override {
        this->data(place).clear();
        nested_func->reset(get_nested_place(place));
    }

    size_t size_of_data() const override { return prefix_size + nested_func->size_of_data(); }

    size_t align_of_data() const override { return nested_func->align_of_data(); }

    void create(AggregateDataPtr __restrict place) const override {
        new (place) Data<stable>;
        SAFE_CREATE(nested_func->create(get_nested_place(place)),
                    this->data(place).~Data<stable>());
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override {
        this->data(place).~Data<stable>();
        nested_func->destroy(get_nested_place(place));
    }

    String get_name() const override { return nested_func->get_name() + "Distinct"; }

    DataTypePtr get_return_type() const override { return nested_func->get_return_type(); }

    IAggregateFunction* transmit_to_stable() override {
        return new AggregateFunctionDistinct<Data, true>(nested_func,
                                                         IAggregateFunction::argument_types);
    }
};

template <typename T>
struct FunctionStableTransfer {
    using FunctionStable = T;
};

template <template <bool stable> typename Data>
struct FunctionStableTransfer<AggregateFunctionDistinct<Data, false>> {
    using FunctionStable = AggregateFunctionDistinct<Data, true>;
};

} // namespace doris::vectorized

#include "common/compile_check_end.h"
