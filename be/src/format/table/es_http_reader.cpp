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

#include "format/table/es_http_reader.h"

#include <gen_cpp/PlanNodes_types.h>

#include "exec/es/es_scan_reader.h"
#include "exec/es/es_scroll_parser.h"
#include "exec/es/es_scroll_query.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"

namespace doris {

EsHttpReader::EsHttpReader(const std::vector<SlotDescriptor*>& file_slot_descs,
                           RuntimeState* state, RuntimeProfile* profile,
                           const TFileRangeDesc& range, const TFileScanRangeParams& params,
                           const TupleDescriptor* tuple_desc)
        : _state(state),
          _tuple_desc(tuple_desc),
          _range(range),
          _params(params),
          _file_slot_descs(file_slot_descs) {}

EsHttpReader::~EsHttpReader() = default;

Status EsHttpReader::init_reader() {
    // Build properties map from Thrift params, combining per-range (es_params)
    // and per-node (es_properties) parameters.
    std::map<std::string, std::string> properties;

    // Per-node shared properties (auth, query_dsl, doc_values_mode, etc.)
    if (_params.__isset.es_properties) {
        properties.insert(_params.es_properties.begin(), _params.es_properties.end());
    }

    // Per-range shard-specific properties (override per-node if same key)
    if (_range.__isset.table_format_params && _range.table_format_params.__isset.es_params) {
        for (const auto& [key, value] : _range.table_format_params.es_params) {
            properties[key] = value;
        }
    }

    // Set batch_size from runtime state
    properties[ESScanReader::KEY_BATCH_SIZE] = std::to_string(_state->batch_size());

    // Extract docvalue and fields context
    if (_params.__isset.es_docvalue_context) {
        _docvalue_context = _params.es_docvalue_context;
    }
    if (_params.__isset.es_fields_context) {
        _fields_context = _params.es_fields_context;
    }

    // Build column names for query DSL
    std::vector<std::string> column_names;
    for (const auto* slot_desc : _tuple_desc->slots()) {
        column_names.push_back(slot_desc->col_name());
    }

    // Build the final query body using ESScrollQueryBuilder
    properties[ESScanReader::KEY_QUERY] =
            ESScrollQueryBuilder::build(properties, column_names, _docvalue_context,
                                        &_doc_value_mode);

    // Extract host for ESScanReader constructor
    const std::string& host = properties.at(ESScanReader::KEY_HOST_PORT);
    _es_reader = std::make_unique<ESScanReader>(host, properties, _doc_value_mode);

    return _es_reader->open();
}

Status EsHttpReader::get_next_block(Block* block, size_t* read_rows, bool* eof) {
    if (_es_eof) {
        *eof = true;
        *read_rows = 0;
        return Status::OK();
    }

    auto column_size = _tuple_desc->slots().size();
    std::vector<MutableColumnPtr> columns(column_size);
    for (size_t i = 0; i < column_size; i++) {
        columns[i] = block->get_by_position(i).column->assume_mutable();
    }

    size_t rows_before = columns[0]->size();
    const int batch_size = _state->batch_size();

    while (columns[0]->size() - rows_before < batch_size && !_es_eof) {
        RETURN_IF_CANCELLED(_state);

        if (_line_eof && _batch_eof) {
            _es_eof = true;
            break;
        }

        while (!_batch_eof) {
            if (_line_eof || _es_scroll_parser == nullptr) {
                RETURN_IF_ERROR(_scroll_and_parse());
                if (_batch_eof) {
                    _es_eof = true;
                    break;
                }
            }

            RETURN_IF_ERROR(_es_scroll_parser->fill_columns(_tuple_desc, columns, &_line_eof,
                                                            _docvalue_context,
                                                            _state->timezone_obj()));
            if (!_line_eof) {
                break;
            }
        }
    }

    *read_rows = columns[0]->size() - rows_before;
    *eof = _es_eof && *read_rows == 0;
    return Status::OK();
}

Status EsHttpReader::_scroll_and_parse() {
    RETURN_IF_ERROR(_es_reader->get_next(&_batch_eof, _es_scroll_parser));
    _line_eof = false;
    return Status::OK();
}

Status EsHttpReader::close() {
    if (_es_reader) {
        RETURN_IF_ERROR(_es_reader->close());
    }
    return Status::OK();
}

Status EsHttpReader::get_columns(std::unordered_map<std::string, DataTypePtr>* name_to_type,
                                  std::unordered_set<std::string>* missing_cols) {
    for (const auto* slot : _file_slot_descs) {
        name_to_type->emplace(slot->col_name(), slot->type());
    }
    return Status::OK();
}

} // namespace doris
