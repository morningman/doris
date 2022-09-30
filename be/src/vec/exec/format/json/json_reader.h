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

#include "exprs/json_functions.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "vec/exec/format/generic_reader.h"

namespace doris::vectorized {

class FileReader;
class LineReader;
class GenericJsonReader : public GenericReader {
public:
    GenericJsonReader(RuntimeState* state, ScannerCounter* counter, RuntimeProfile* profile,
                      bool strip_outer_array, bool num_as_string, bool fuzzy_parse,
                      bool* scanner_eof, FileReader* file_reader, LineReader* line_reader);

    ~GenericJsonReader();

    Status init(const std::string& jsonpath, const std::string& json_root);

    virtual Status get_next_block(Block* block, bool* eof) override;

    virtual Status get_columns(std::unordered_map<std::string, TypeDescriptor>* name_to_type,
                               std::unordered_set<std::string>* missing_cols) override;

    Status read_json_column(std::vector<MutableColumnPtr>& columns,
                            const std::vector<SlotDescriptor*>& slot_descs, bool* is_empty_row,
                            bool* eof);

private:
    Status _parse_jsonpath_and_json_root(const std::string& jsonpath, const std::string& json_root);

    Status _generate_json_paths(const std::string& jsonpath,
                                std::vector<std::vector<JsonPath>>* paths);

    Status (GenericJsonReader::*_vhandle_json_callback)(
            std::vector<vectorized::MutableColumnPtr>& columns,
            const std::vector<SlotDescriptor*>& slot_descs, bool* is_empty_row, bool* eof);

    Status _vhandle_simple_json(std::vector<MutableColumnPtr>& columns,
                                const std::vector<SlotDescriptor*>& slot_descs, bool* is_empty_row,
                                bool* eof);

    Status _vhandle_flat_array_complex_json(std::vector<MutableColumnPtr>& columns,
                                            const std::vector<SlotDescriptor*>& slot_descs,
                                            bool* is_empty_row, bool* eof);

    Status _vhandle_nested_complex_json(std::vector<MutableColumnPtr>& columns,
                                        const std::vector<SlotDescriptor*>& slot_descs,
                                        bool* is_empty_row, bool* eof);

    Status _write_columns_by_jsonpath(rapidjson::Value& objectValue,
                                      const std::vector<SlotDescriptor*>& slot_descs,
                                      std::vector<MutableColumnPtr>& columns, bool* valid);

    Status _set_column_value(rapidjson::Value& objectValue, std::vector<MutableColumnPtr>& columns,
                             const std::vector<SlotDescriptor*>& slot_descs, bool* valid);

    Status _write_data_to_column(rapidjson::Value::ConstValueIterator value,
                                 SlotDescriptor* slot_desc, vectorized::IColumn* column_ptr,
                                 bool* valid);

    Status _parse_json(bool* is_empty_row, bool* eof);

    Status _append_error_msg(const rapidjson::Value& objectValue, std::string error_msg,
                             std::string col_name, bool* valid);

private:
    int _next_line;
    int _total_lines;
    RuntimeState* _state;
    ScannerCounter* _counter;
    RuntimeProfile* _profile;
    FileReader* _cur_file_reader;
    // line reader is valid only when "read_json_by_line" is true
    LineReader* _cur_line_reader;
    bool _closed;
    bool _strip_outer_array;
    bool _num_as_string;
    bool _fuzzy_parse;
    RuntimeProfile::Counter* _bytes_read_counter;
    RuntimeProfile::Counter* _read_timer;
    RuntimeProfile::Counter* _file_read_timer;

    std::vector<std::vector<JsonPath>> _parsed_json_paths;
    std::vector<JsonPath> _parsed_json_root;

    rapidjson::Document _origin_json_doc; // origin json document object from parsed json string
    rapidjson::Value* _json_doc; // _json_doc equals _final_json_doc iff not set `json_root`
    std::unordered_map<std::string, int> _name_map;

    // point to the _scanner_eof of JsonScanner
    bool* _scanner_eof;
    bool _cur_reader_eof;
};
} // namespace doris::vectorized
