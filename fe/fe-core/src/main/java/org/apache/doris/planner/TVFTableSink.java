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

package org.apache.doris.planner;

import org.apache.doris.catalog.Column;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.thrift.TColumn;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TTVFTableSink;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * TVFTableSink is used for INSERT INTO tvf_name(properties) SELECT ...
 * It writes query results to files via TVF (local/s3/hdfs).
 */
public class TVFTableSink extends DataSink {
    private final String tvfName;
    private final Map<String, String> properties;
    private final List<Column> cols;
    private TDataSink tDataSink;

    public TVFTableSink(String tvfName, Map<String, String> properties, List<Column> cols) {
        this.tvfName = tvfName;
        this.properties = properties;
        this.cols = cols;
    }

    public void bindDataSink() throws AnalysisException {
        TTVFTableSink tSink = new TTVFTableSink();
        tSink.setTvfName(tvfName);

        String filePath = properties.get("file_path");
        tSink.setFilePath(filePath);

        // Set file format
        String format = properties.getOrDefault("format", "csv").toLowerCase();
        TFileFormatType formatType = getFormatType(format);
        tSink.setFileFormat(formatType);

        // Set file type based on TVF name
        TFileType fileType = getFileType(tvfName);
        tSink.setFileType(fileType);

        // Set all properties for BE to access
        tSink.setProperties(properties);

        // Set columns
        List<TColumn> tColumns = new ArrayList<>();
        for (Column col : cols) {
            tColumns.add(col.toThrift());
        }
        tSink.setColumns(tColumns);

        // Set column separator
        String columnSeparator = properties.getOrDefault("column_separator", ",");
        tSink.setColumnSeparator(columnSeparator);

        // Set line delimiter
        String lineDelimiter = properties.getOrDefault("line_delimiter", "\n");
        tSink.setLineDelimiter(lineDelimiter);

        // Set max file size
        String maxFileSizeStr = properties.get("max_file_size");
        if (maxFileSizeStr != null) {
            tSink.setMaxFileSizeBytes(Long.parseLong(maxFileSizeStr));
        }

        // Set delete existing files flag
        String deleteExisting = properties.getOrDefault("delete_existing_files", "true");
        tSink.setDeleteExistingFiles(Boolean.parseBoolean(deleteExisting));

        // Set compression type
        String compression = properties.get("compression_type");
        if (compression != null) {
            tSink.setCompressionType(getCompressType(compression));
        }

        // Set backend id for local TVF
        String backendIdStr = properties.get("backend_id");
        if (backendIdStr != null) {
            tSink.setBackendId(Long.parseLong(backendIdStr));
        }

        // Set hadoop config for hdfs/s3
        if (tvfName.equals("hdfs") || tvfName.equals("s3")) {
            tSink.setHadoopConfig(properties);
        }

        tDataSink = new TDataSink(TDataSinkType.TVF_TABLE_SINK);
        tDataSink.setTvfTableSink(tSink);
    }

    private TFileFormatType getFormatType(String format) throws AnalysisException {
        switch (format) {
            case "csv":
                return TFileFormatType.FORMAT_CSV_PLAIN;
            case "parquet":
                return TFileFormatType.FORMAT_PARQUET;
            case "orc":
                return TFileFormatType.FORMAT_ORC;
            default:
                throw new AnalysisException("Unsupported TVF sink format: " + format
                        + ". Supported formats: csv, parquet, orc");
        }
    }

    private TFileType getFileType(String tvfName) throws AnalysisException {
        switch (tvfName.toLowerCase()) {
            case "local":
                return TFileType.FILE_LOCAL;
            case "s3":
                return TFileType.FILE_S3;
            case "hdfs":
                return TFileType.FILE_HDFS;
            default:
                throw new AnalysisException("Unsupported TVF type: " + tvfName);
        }
    }

    private TFileCompressType getCompressType(String compressType) {
        switch (compressType.toLowerCase()) {
            case "snappy":
                return TFileCompressType.SNAPPYBLOCK;
            case "lz4":
                return TFileCompressType.LZ4BLOCK;
            case "gzip":
            case "gz":
                return TFileCompressType.GZ;
            case "zstd":
                return TFileCompressType.ZSTD;
            default:
                return TFileCompressType.PLAIN;
        }
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(prefix).append("TVF TABLE SINK\n");
        strBuilder.append(prefix).append("  tvfName: ").append(tvfName).append("\n");
        strBuilder.append(prefix).append("  filePath: ").append(properties.get("file_path")).append("\n");
        strBuilder.append(prefix).append("  format: ").append(properties.getOrDefault("format", "csv")).append("\n");
        return strBuilder.toString();
    }

    @Override
    protected TDataSink toThrift() {
        return tDataSink;
    }

    @Override
    public DataPartition getOutputPartition() {
        return null;
    }
}
