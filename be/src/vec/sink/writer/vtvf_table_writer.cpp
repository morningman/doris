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

#include "vec/sink/writer/vtvf_table_writer.h"

#include <fmt/format.h>
#include <gen_cpp/PlanNodes_types.h>

#include <sstream>

#include "common/status.h"
#include "io/file_factory.h"
#include "io/fs/local_file_system.h"
#include "runtime/runtime_state.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/runtime/vcsv_transformer.h"
#include "vec/runtime/vorc_transformer.h"
#include "vec/runtime/vparquet_transformer.h"

namespace doris::vectorized {

VTVFTableWriter::VTVFTableWriter(const TDataSink& t_sink, const VExprContextSPtrs& output_exprs,
                                 std::shared_ptr<pipeline::Dependency> dep,
                                 std::shared_ptr<pipeline::Dependency> fin_dep)
        : AsyncResultWriter(output_exprs, dep, fin_dep) {
    _tvf_sink = t_sink.tvf_table_sink;
}

Status VTVFTableWriter::open(RuntimeState* state, RuntimeProfile* profile) {
    _state = state;

    // Init profile counters
    RuntimeProfile* writer_profile = profile->create_child("VTVFTableWriter", true, true);
    _written_rows_counter = ADD_COUNTER(writer_profile, "NumWrittenRows", TUnit::UNIT);
    _written_data_bytes = ADD_COUNTER(writer_profile, "WrittenDataBytes", TUnit::BYTES);
    _file_write_timer = ADD_TIMER(writer_profile, "FileWriteTime");
    _writer_close_timer = ADD_TIMER(writer_profile, "FileWriterCloseTime");

    _file_path = _tvf_sink.file_path;
    _max_file_size_bytes = _tvf_sink.__isset.max_file_size_bytes ? _tvf_sink.max_file_size_bytes : 0;
    _delete_existing_files_flag =
            _tvf_sink.__isset.delete_existing_files ? _tvf_sink.delete_existing_files : true;

    // Delete existing files if requested
    if (_delete_existing_files_flag) {
        RETURN_IF_ERROR(_delete_existing_files());
    }

    return _create_next_file_writer();
}

Status VTVFTableWriter::write(RuntimeState* state, vectorized::Block& block) {
    COUNTER_UPDATE(_written_rows_counter, block.rows());

    {
        SCOPED_TIMER(_file_write_timer);
        RETURN_IF_ERROR(_vfile_writer->write(block));
    }

    _current_written_bytes = _vfile_writer->written_len();

    // Auto-split if max file size is set
    if (_max_file_size_bytes > 0) {
        RETURN_IF_ERROR(_create_new_file_if_exceed_size());
    }

    return Status::OK();
}

Status VTVFTableWriter::close(Status status) {
    if (!status.ok()) {
        return status;
    }

    SCOPED_TIMER(_writer_close_timer);
    return _close_file_writer(true);
}

Status VTVFTableWriter::_create_file_writer(const std::string& file_name) {
    TFileType::type file_type = _tvf_sink.file_type;
    std::map<std::string, std::string> properties;
    if (_tvf_sink.__isset.properties) {
        properties = _tvf_sink.properties;
    }

    _file_writer_impl = DORIS_TRY(FileFactory::create_file_writer(
            file_type, _state->exec_env(), {}, properties, file_name,
            {
                    .write_file_cache = false,
                    .sync_file_data = false,
            }));

    TFileFormatType::type format = _tvf_sink.file_format;
    switch (format) {
    case TFileFormatType::FORMAT_CSV_PLAIN: {
        std::string column_separator =
                _tvf_sink.__isset.column_separator ? _tvf_sink.column_separator : ",";
        std::string line_delimiter =
                _tvf_sink.__isset.line_delimiter ? _tvf_sink.line_delimiter : "\n";
        TFileCompressType::type compress_type = TFileCompressType::PLAIN;
        if (_tvf_sink.__isset.compression_type) {
            compress_type = _tvf_sink.compression_type;
        }
        _vfile_writer.reset(new VCSVTransformer(_state, _file_writer_impl.get(),
                                                _vec_output_expr_ctxs, false, {}, {}, column_separator,
                                                line_delimiter, false, compress_type));
        break;
    }
    case TFileFormatType::FORMAT_PARQUET: {
        // Build parquet schemas from columns
        std::vector<TParquetSchema> parquet_schemas;
        if (_tvf_sink.__isset.columns) {
            for (const auto& col : _tvf_sink.columns) {
                TParquetSchema schema;
                schema.__set_schema_column_name(col.column_name);
                parquet_schemas.push_back(schema);
            }
        }
        _vfile_writer.reset(new VParquetTransformer(
                _state, _file_writer_impl.get(), _vec_output_expr_ctxs, parquet_schemas, false,
                {TParquetCompressionType::SNAPPY, TParquetVersion::PARQUET_1_0, false, false}));
        break;
    }
    case TFileFormatType::FORMAT_ORC: {
        TFileCompressType::type compress_type = TFileCompressType::PLAIN;
        if (_tvf_sink.__isset.compression_type) {
            compress_type = _tvf_sink.compression_type;
        }
        _vfile_writer.reset(new VOrcTransformer(_state, _file_writer_impl.get(),
                                                _vec_output_expr_ctxs, "", {}, false,
                                                compress_type));
        break;
    }
    default:
        return Status::InternalError("Unsupported TVF sink file format: {}", format);
    }

    LOG(INFO) << "TVF table writer created file: " << file_name
              << ", format: " << format
              << ", query_id: " << print_id(_state->query_id());

    return _vfile_writer->open();
}

Status VTVFTableWriter::_create_next_file_writer() {
    std::string file_name;
    RETURN_IF_ERROR(_get_next_file_name(&file_name));
    return _create_file_writer(file_name);
}

Status VTVFTableWriter::_close_file_writer(bool done) {
    if (_vfile_writer) {
        RETURN_IF_ERROR(_vfile_writer->close());
        COUNTER_UPDATE(_written_data_bytes, _vfile_writer->written_len());
        _vfile_writer.reset(nullptr);
    } else if (_file_writer_impl && _file_writer_impl->state() != io::FileWriter::State::CLOSED) {
        RETURN_IF_ERROR(_file_writer_impl->close());
    }

    if (!done) {
        RETURN_IF_ERROR(_create_next_file_writer());
    }
    return Status::OK();
}

Status VTVFTableWriter::_create_new_file_if_exceed_size() {
    if (_max_file_size_bytes <= 0 || _current_written_bytes < _max_file_size_bytes) {
        return Status::OK();
    }
    SCOPED_TIMER(_writer_close_timer);
    RETURN_IF_ERROR(_close_file_writer(false));
    _current_written_bytes = 0;
    return Status::OK();
}

Status VTVFTableWriter::_get_next_file_name(std::string* file_name) {
    // Determine file extension
    std::string ext;
    switch (_tvf_sink.file_format) {
    case TFileFormatType::FORMAT_CSV_PLAIN:
        ext = "csv";
        break;
    case TFileFormatType::FORMAT_PARQUET:
        ext = "parquet";
        break;
    case TFileFormatType::FORMAT_ORC:
        ext = "orc";
        break;
    default:
        ext = "dat";
        break;
    }

    if (_file_idx == 0 && _max_file_size_bytes <= 0) {
        // Single file mode: use the path as-is if it already has extension
        if (_file_path.find('.') != std::string::npos) {
            *file_name = _file_path;
        } else {
            *file_name = fmt::format("{}.{}", _file_path, ext);
        }
    } else {
        // Multi-file (auto-split) mode: append index
        // Strip extension from base path if present
        std::string base = _file_path;
        auto dot_pos = base.rfind('.');
        if (dot_pos != std::string::npos) {
            base = base.substr(0, dot_pos);
        }
        *file_name = fmt::format("{}_{}.{}", base, _file_idx, ext);
    }
    _file_idx++;
    return Status::OK();
}

Status VTVFTableWriter::_delete_existing_files() {
    if (_tvf_sink.file_type == TFileType::FILE_LOCAL) {
        // For local files, try to delete the file if it exists
        bool exists = false;
        RETURN_IF_ERROR(io::global_local_filesystem()->exists(_file_path, &exists));
        if (exists) {
            RETURN_IF_ERROR(io::global_local_filesystem()->delete_file(_file_path));
        }
    }
    // For S3/HDFS, we don't delete existing files by default
    // as it requires more complex handling (e.g., directory listing)
    return Status::OK();
}

} // namespace doris::vectorized
