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

#include <parallel_hashmap/phmap.h>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_string.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_map.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

template <PrimitiveType K>
struct AggregateFunctionMapAggData {
    using KeyType = typename PrimitiveTypeTraits<K>::ColumnItemType;
    using Map = phmap::flat_hash_map<StringRef, int64_t>;

    AggregateFunctionMapAggData() { throw Exception(Status::FatalError("__builtin_unreachable")); }

    AggregateFunctionMapAggData(const DataTypes& argument_types) {
        _key_type = remove_nullable(argument_types[0]);
        _value_type = make_nullable(argument_types[1]);
        _key_column = _key_type->create_column();
        _value_column = _value_type->create_column();
    }

    void reset() {
        _map.clear();
        _key_column->clear();
        _value_column->clear();
    }

    void add(StringRef key, const Field& value) {
        DCHECK(key.data != nullptr);
        if (UNLIKELY(_map.find(key) != _map.end())) {
            return;
        }

        key.data = _arena.insert(key.data, key.size);

        _map.emplace(key, _key_column->size());
        _key_column->insert_data(key.data, key.size);
        _value_column->insert(value);
    }

    void add(const Field& key_, const Field& value) {
        DCHECK(!key_.is_null());
        auto key_array = vectorized::get<Array>(key_);
        auto value_array = vectorized::get<Array>(value);

        const auto count = key_array.size();
        DCHECK_EQ(count, value_array.size());

        for (size_t i = 0; i != count; ++i) {
            StringRef key;
            if constexpr (is_string_type(K)) {
                auto& string = key_array[i].get<String>();
                key.data = string.data();
                key.size = string.size();
            } else {
                auto& k = key_array[i].get<KeyType>();
                key.data = reinterpret_cast<const char*>(&k);
                key.size = sizeof(k);
            }

            if (UNLIKELY(_map.find(key) != _map.end())) {
                continue;
            }

            key.data = _arena.insert(key.data, key.size);

            _map.emplace(key, _key_column->size());
            _key_column->insert_data(key.data, key.size);
            _value_column->insert(value_array[i]);
        }
    }

    void merge(const AggregateFunctionMapAggData& other) {
        const size_t num_rows = other._key_column->size();
        if (num_rows == 0) {
            return;
        }

        auto& other_key_column = assert_cast<KeyColumnType&>(*other._key_column);

        for (size_t i = 0; i != num_rows; ++i) {
            auto key = static_cast<KeyColumnType&>(other_key_column).get_data_at(i);
            if (_map.find(key) != _map.cend()) {
                continue;
            }
            key.data = _arena.insert(key.data, key.size);

            _map.emplace(key, _key_column->size());
            static_cast<KeyColumnType&>(*_key_column).insert_data(key.data, key.size);

            auto value = other._value_column->get_data_at(i);
            _value_column->insert_data(value.data, value.size);
        }
    }

    void insert_result_into(IColumn& to) const {
        auto& dst = assert_cast<ColumnMap&>(to);
        size_t num_rows = _key_column->size();
        auto& offsets = dst.get_offsets();
        auto& dst_key_column = assert_cast<ColumnNullable&>(dst.get_keys());
        dst_key_column.get_null_map_data().resize_fill(dst_key_column.get_null_map_data().size() +
                                                       num_rows);
        dst_key_column.get_nested_column().insert_range_from(*_key_column, 0, num_rows);
        dst.get_values().insert_range_from(*_value_column, 0, num_rows);
        if (offsets.empty()) {
            offsets.push_back(num_rows);
        } else {
            offsets.push_back(offsets.back() + num_rows);
        }
    }

    void write(BufferWritable& buf) const {
        const size_t size = _key_column->size();
        buf.write_binary(size);
        for (size_t i = 0; i < size; i++) {
            buf.write_binary(assert_cast<KeyColumnType&, TypeCheckOnRelease::DISABLE>(*_key_column)
                                     .get_data_at(i));
        }
        for (size_t i = 0; i < size; i++) {
            buf.write_binary(_value_column->get_data_at(i));
        }
    }

    void read(BufferReadable& buf) {
        size_t size = 0;
        buf.read_binary(size);
        StringRef key;
        for (size_t i = 0; i < size; i++) {
            buf.read_binary(key);
            DCHECK(_map.find(key) == _map.cend());
            key.data = _arena.insert(key.data, key.size);
            assert_cast<KeyColumnType&, TypeCheckOnRelease::DISABLE>(*_key_column)
                    .insert_data(key.data, key.size);
        }
        StringRef val;
        for (size_t i = 0; i < size; i++) {
            buf.read_binary(val);
            _value_column->insert_data(val.data, val.size);
        }
    }

private:
    using KeyColumnType = typename PrimitiveTypeTraits<K>::ColumnType;
    Map _map;
    Arena _arena;
    IColumn::MutablePtr _key_column;
    IColumn::MutablePtr _value_column;
    DataTypePtr _key_type;
    DataTypePtr _value_type;
};

template <typename Data, PrimitiveType K>
class AggregateFunctionMapAgg final
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionMapAgg<Data, K>> {
public:
    using KeyColumnType = typename PrimitiveTypeTraits<K>::ColumnType;
    AggregateFunctionMapAgg() = default;
    AggregateFunctionMapAgg(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionMapAgg<Data, K>>(
                      argument_types_) {}

    std::string get_name() const override { return "map_agg_v1"; }

    DataTypePtr get_return_type() const override {
        /// keys and values column of `ColumnMap` are always nullable.
        return std::make_shared<DataTypeMap>(make_nullable(argument_types[0]),
                                             make_nullable(argument_types[1]));
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena*) const override {
        if (columns[0]->is_nullable()) {
            const auto& nullable_col =
                    assert_cast<const ColumnNullable&, TypeCheckOnRelease::DISABLE>(*columns[0]);
            const auto& nullable_map = nullable_col.get_null_map_data();
            if (nullable_map[row_num]) {
                return;
            }
            Field value;
            columns[1]->get(row_num, value);
            this->data(place).add(assert_cast<const KeyColumnType&, TypeCheckOnRelease::DISABLE>(
                                          nullable_col.get_nested_column())
                                          .get_data_at(row_num),
                                  value);
        } else {
            Field value;
            columns[1]->get(row_num, value);
            this->data(place).add(
                    assert_cast<const KeyColumnType&, TypeCheckOnRelease::DISABLE>(*columns[0])
                            .get_data_at(row_num),
                    value);
        }
    }

    void create(AggregateDataPtr __restrict place) const override {
        new (place) Data(argument_types);
    }

    void reset(AggregateDataPtr place) const override { this->data(place).reset(); }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena*) const override {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena*) const override {
        this->data(place).read(buf);
    }

    void streaming_agg_serialize_to_column(const IColumn** columns, MutableColumnPtr& dst,
                                           const size_t num_rows, Arena*) const override {
        auto& col = assert_cast<ColumnMap&>(*dst);
        for (size_t i = 0; i != num_rows; ++i) {
            Field key, value;
            columns[0]->get(i, key);
            if (key.is_null()) {
                col.insert(Field::create_field<TYPE_MAP>(
                        Map {Field::create_field<TYPE_ARRAY>(Array {}),
                             Field::create_field<TYPE_ARRAY>(Array {})}));
                continue;
            }

            columns[1]->get(i, value);
            col.insert(Field::create_field<TYPE_MAP>(
                    Map {Field::create_field<TYPE_ARRAY>(Array {key}),
                         Field::create_field<TYPE_ARRAY>(Array {value})}));
        }
    }

    void deserialize_from_column(AggregateDataPtr places, const IColumn& column, Arena*,
                                 size_t num_rows) const override {
        const auto& col = assert_cast<const ColumnMap&>(column);
        auto* data = &(this->data(places));
        for (size_t i = 0; i != num_rows; ++i) {
            auto map = doris::vectorized::get<Map>(col[i]);
            data->add(map[0], map[1]);
        }
    }

    void serialize_to_column(const std::vector<AggregateDataPtr>& places, size_t offset,
                             MutableColumnPtr& dst, const size_t num_rows) const override {
        for (size_t i = 0; i != num_rows; ++i) {
            Data& data_ = this->data(places[i] + offset);
            data_.insert_result_into(*dst);
        }
    }

    void deserialize_and_merge_from_column(AggregateDataPtr __restrict place, const IColumn& column,
                                           Arena*) const override {
        auto& col = assert_cast<const ColumnMap&>(column);
        const size_t num_rows = column.size();
        for (size_t i = 0; i != num_rows; ++i) {
            auto map = doris::vectorized::get<Map>(col[i]);
            this->data(place).add(map[0], map[1]);
        }
    }

    void deserialize_and_merge_from_column_range(AggregateDataPtr __restrict place,
                                                 const IColumn& column, size_t begin, size_t end,
                                                 Arena*) const override {
        DCHECK(end <= column.size() && begin <= end)
                << ", begin:" << begin << ", end:" << end << ", column.size():" << column.size();
        const auto& col = assert_cast<const ColumnMap&>(column);
        for (size_t i = begin; i <= end; ++i) {
            auto map = doris::vectorized::get<Map>(col[i]);
            this->data(place).add(map[0], map[1]);
        }
    }

    void deserialize_and_merge_vec(const AggregateDataPtr* places, size_t offset,
                                   AggregateDataPtr rhs, const IColumn* column, Arena*,
                                   const size_t num_rows) const override {
        const auto& col = assert_cast<const ColumnMap&>(*column);
        for (size_t i = 0; i != num_rows; ++i) {
            auto map = doris::vectorized::get<Map>(col[i]);
            this->data(places[i] + offset).add(map[0], map[1]);
        }
    }

    void deserialize_and_merge_vec_selected(const AggregateDataPtr* places, size_t offset,
                                            AggregateDataPtr rhs, const IColumn* column, Arena*,
                                            const size_t num_rows) const override {
        const auto& col = assert_cast<const ColumnMap&>(*column);
        for (size_t i = 0; i != num_rows; ++i) {
            if (places[i]) {
                auto map = doris::vectorized::get<Map>(col[i]);
                this->data(places[i] + offset).add(map[0], map[1]);
            }
        }
    }

    void serialize_without_key_to_column(ConstAggregateDataPtr __restrict place,
                                         IColumn& to) const override {
        this->data(place).insert_result_into(to);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        this->data(place).insert_result_into(to);
    }

    [[nodiscard]] MutableColumnPtr create_serialize_column() const override {
        return get_return_type()->create_column();
    }

    [[nodiscard]] DataTypePtr get_serialized_type() const override { return get_return_type(); }

protected:
    using IAggregateFunction::argument_types;
};

} // namespace doris::vectorized

#include "common/compile_check_end.h"
