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

suite("test_insert_into_hdfs_tvf", "external,hive,tvf,external_docker") {

    String hdfs_port = context.config.otherConfigs.get("hive2HdfsPort")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    def hdfsUserName = "doris"
    def defaultFS = "hdfs://${externalEnvIp}:${hdfs_port}"
    def hdfsBasePath = "/tmp/test_insert_into_hdfs_tvf"

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("enableHiveTest not true, skip")
        return
    }

    def hdfsWriteProps = { String path, String format ->
        return """
            "file_path" = "${hdfsBasePath}/${path}",
            "format" = "${format}",
            "hadoop.username" = "${hdfsUserName}",
            "fs.defaultFS" = "${defaultFS}"
        """
    }

    def hdfsReadProps = { String path, String format ->
        return """
            "uri" = "${defaultFS}${hdfsBasePath}/${path}",
            "hadoop.username" = "${hdfsUserName}",
            "format" = "${format}"
        """
    }

    // ============ Source tables ============

    sql """ DROP TABLE IF EXISTS insert_tvf_test_src """
    sql """
        CREATE TABLE IF NOT EXISTS insert_tvf_test_src (
            c_bool      BOOLEAN,
            c_tinyint   TINYINT,
            c_smallint  SMALLINT,
            c_int       INT,
            c_bigint    BIGINT,
            c_float     FLOAT,
            c_double    DOUBLE,
            c_decimal   DECIMAL(10,2),
            c_date      DATE,
            c_datetime  DATETIME,
            c_varchar   VARCHAR(100),
            c_string    STRING
        ) DISTRIBUTED BY HASH(c_int) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """

    sql """
        INSERT INTO insert_tvf_test_src VALUES
            (true,  1,  100,  1000,  100000,  1.1,  2.2,  123.45, '2024-01-01', '2024-01-01 10:00:00', 'hello', 'world'),
            (false, 2,  200,  2000,  200000,  3.3,  4.4,  678.90, '2024-06-15', '2024-06-15 12:30:00', 'foo',   'bar'),
            (true,  3,  300,  3000,  300000,  5.5,  6.6,  999.99, '2024-12-31', '2024-12-31 23:59:59', 'test',  'data'),
            (NULL,  NULL, NULL, NULL, NULL,   NULL, NULL,  NULL,   NULL,         NULL,                  NULL,    NULL),
            (false, -1, -100, -1000, -100000, -1.1, -2.2, -123.45,'2020-02-29', '2020-02-29 00:00:00', '',      'special_chars');
    """

    sql """ DROP TABLE IF EXISTS insert_tvf_complex_src """
    sql """
        CREATE TABLE IF NOT EXISTS insert_tvf_complex_src (
            c_int    INT,
            c_array  ARRAY<INT>,
            c_map    MAP<STRING, INT>,
            c_struct STRUCT<f1:INT, f2:STRING>
        ) DISTRIBUTED BY HASH(c_int) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """

    sql """
        INSERT INTO insert_tvf_complex_src VALUES
            (1, [1, 2, 3],  {'a': 1, 'b': 2}, {1, 'hello'}),
            (2, [4, 5],     {'x': 10},         {2, 'world'}),
            (3, [],         {},                 {3, ''}),
            (4, NULL,       NULL,               NULL);
    """

    sql """ DROP TABLE IF EXISTS insert_tvf_join_src """
    sql """
        CREATE TABLE IF NOT EXISTS insert_tvf_join_src (
            c_int    INT,
            c_label  STRING
        ) DISTRIBUTED BY HASH(c_int) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """

    sql """ INSERT INTO insert_tvf_join_src VALUES (1000, 'label_a'), (2000, 'label_b'), (3000, 'label_c'); """

    // ============ 1. HDFS CSV basic types ============

    sql """
        INSERT INTO hdfs(
            ${hdfsWriteProps("basic_csv.csv", "csv")},
            "delete_existing_files" = "true"
        ) SELECT * FROM insert_tvf_test_src ORDER BY c_int;
    """

    order_qt_hdfs_csv_basic_types """
        SELECT * FROM hdfs(
            ${hdfsReadProps("basic_csv.csv", "csv")}
        ) ORDER BY c1;
    """

    // ============ 2. HDFS Parquet basic types ============

    sql """
        INSERT INTO hdfs(
            ${hdfsWriteProps("basic_parquet.parquet", "parquet")},
            "delete_existing_files" = "true"
        ) SELECT * FROM insert_tvf_test_src ORDER BY c_int;
    """

    order_qt_hdfs_parquet_basic_types """
        SELECT * FROM hdfs(
            ${hdfsReadProps("basic_parquet.parquet", "parquet")}
        ) ORDER BY c_int;
    """

    // ============ 3. HDFS ORC basic types ============

    sql """
        INSERT INTO hdfs(
            ${hdfsWriteProps("basic_orc.orc", "orc")},
            "delete_existing_files" = "true"
        ) SELECT * FROM insert_tvf_test_src ORDER BY c_int;
    """

    order_qt_hdfs_orc_basic_types """
        SELECT * FROM hdfs(
            ${hdfsReadProps("basic_orc.orc", "orc")}
        ) ORDER BY c_int;
    """

    // ============ 4. HDFS Parquet complex types ============

    sql """
        INSERT INTO hdfs(
            ${hdfsWriteProps("complex_parquet.parquet", "parquet")},
            "delete_existing_files" = "true"
        ) SELECT * FROM insert_tvf_complex_src ORDER BY c_int;
    """

    order_qt_hdfs_parquet_complex_types """
        SELECT * FROM hdfs(
            ${hdfsReadProps("complex_parquet.parquet", "parquet")}
        ) ORDER BY c_int;
    """

    // ============ 5. HDFS ORC complex types ============

    sql """
        INSERT INTO hdfs(
            ${hdfsWriteProps("complex_orc.orc", "orc")},
            "delete_existing_files" = "true"
        ) SELECT * FROM insert_tvf_complex_src ORDER BY c_int;
    """

    order_qt_hdfs_orc_complex_types """
        SELECT * FROM hdfs(
            ${hdfsReadProps("complex_orc.orc", "orc")}
        ) ORDER BY c_int;
    """

    // ============ 6. HDFS CSV separator: comma ============

    sql """
        INSERT INTO hdfs(
            ${hdfsWriteProps("sep_comma.csv", "csv")},
            "column_separator" = ",",
            "delete_existing_files" = "true"
        ) SELECT c_int, c_varchar, c_string FROM insert_tvf_test_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_hdfs_csv_sep_comma """
        SELECT * FROM hdfs(
            ${hdfsReadProps("sep_comma.csv", "csv")},
            "column_separator" = ","
        ) ORDER BY c1;
    """

    // ============ 7. HDFS CSV separator: tab ============

    sql """
        INSERT INTO hdfs(
            ${hdfsWriteProps("sep_tab.csv", "csv")},
            "column_separator" = "\t",
            "delete_existing_files" = "true"
        ) SELECT c_int, c_varchar, c_string FROM insert_tvf_test_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_hdfs_csv_sep_tab """
        SELECT * FROM hdfs(
            ${hdfsReadProps("sep_tab.csv", "csv")},
            "column_separator" = "\t"
        ) ORDER BY c1;
    """

    // ============ 8. HDFS CSV separator: pipe ============

    sql """
        INSERT INTO hdfs(
            ${hdfsWriteProps("sep_pipe.csv", "csv")},
            "column_separator" = "|",
            "delete_existing_files" = "true"
        ) SELECT c_int, c_varchar, c_string FROM insert_tvf_test_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_hdfs_csv_sep_pipe """
        SELECT * FROM hdfs(
            ${hdfsReadProps("sep_pipe.csv", "csv")},
            "column_separator" = "|"
        ) ORDER BY c1;
    """

    // ============ 9. HDFS CSV separator: multi-char ============

    sql """
        INSERT INTO hdfs(
            ${hdfsWriteProps("sep_multi.csv", "csv")},
            "column_separator" = ";;",
            "delete_existing_files" = "true"
        ) SELECT c_int, c_varchar, c_string FROM insert_tvf_test_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_hdfs_csv_sep_multi """
        SELECT * FROM hdfs(
            ${hdfsReadProps("sep_multi.csv", "csv")},
            "column_separator" = ";;"
        ) ORDER BY c1;
    """

    // ============ 10. HDFS CSV line delimiter: CRLF ============

    sql """
        INSERT INTO hdfs(
            ${hdfsWriteProps("line_crlf.csv", "csv")},
            "line_delimiter" = "\r\n",
            "delete_existing_files" = "true"
        ) SELECT c_int, c_varchar, c_string FROM insert_tvf_test_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_hdfs_csv_line_crlf """
        SELECT * FROM hdfs(
            ${hdfsReadProps("line_crlf.csv", "csv")},
            "line_delimiter" = "\r\n"
        ) ORDER BY c1;
    """

    // ============ 11. HDFS CSV compress: gz ============

    sql """
        INSERT INTO hdfs(
            ${hdfsWriteProps("compress_gz.csv.gz", "csv")},
            "compression_type" = "gz",
            "delete_existing_files" = "true"
        ) SELECT c_int, c_varchar, c_string FROM insert_tvf_test_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_hdfs_csv_compress_gz """
        SELECT * FROM hdfs(
            ${hdfsReadProps("compress_gz.csv.gz", "csv")},
            "compress_type" = "gz"
        ) ORDER BY c1;
    """

    // ============ 12. HDFS CSV compress: zstd ============

    sql """
        INSERT INTO hdfs(
            ${hdfsWriteProps("compress_zstd.csv.zst", "csv")},
            "compression_type" = "zstd",
            "delete_existing_files" = "true"
        ) SELECT c_int, c_varchar, c_string FROM insert_tvf_test_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_hdfs_csv_compress_zstd """
        SELECT * FROM hdfs(
            ${hdfsReadProps("compress_zstd.csv.zst", "csv")},
            "compress_type" = "zstd"
        ) ORDER BY c1;
    """

    // ============ 13. HDFS CSV compress: lz4 ============

    sql """
        INSERT INTO hdfs(
            ${hdfsWriteProps("compress_lz4.csv.lz4", "csv")},
            "compression_type" = "lz4block",
            "delete_existing_files" = "true"
        ) SELECT c_int, c_varchar, c_string FROM insert_tvf_test_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_hdfs_csv_compress_lz4 """
        SELECT * FROM hdfs(
            ${hdfsReadProps("compress_lz4.csv.lz4", "csv")},
            "compress_type" = "lz4block"
        ) ORDER BY c1;
    """

    // ============ 14. HDFS CSV compress: snappy ============

    sql """
        INSERT INTO hdfs(
            ${hdfsWriteProps("compress_snappy.csv.snappy", "csv")},
            "compression_type" = "snappyblock",
            "delete_existing_files" = "true"
        ) SELECT c_int, c_varchar, c_string FROM insert_tvf_test_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_hdfs_csv_compress_snappy """
        SELECT * FROM hdfs(
            ${hdfsReadProps("compress_snappy.csv.snappy", "csv")},
            "compress_type" = "snappyblock"
        ) ORDER BY c1;
    """

    // ============ 15. HDFS Overwrite mode ============

    // First write: 5 rows
    sql """
        INSERT INTO hdfs(
            ${hdfsWriteProps("overwrite.csv", "csv")},
            "delete_existing_files" = "true"
        ) SELECT c_int, c_varchar FROM insert_tvf_test_src ORDER BY c_int;
    """

    order_qt_hdfs_overwrite_first """
        SELECT * FROM hdfs(
            ${hdfsReadProps("overwrite.csv", "csv")}
        ) ORDER BY c1;
    """

    // Second write: 2 rows with overwrite
    sql """
        INSERT INTO hdfs(
            ${hdfsWriteProps("overwrite.csv", "csv")},
            "delete_existing_files" = "true"
        ) SELECT c_int, c_varchar FROM insert_tvf_test_src WHERE c_int > 0 ORDER BY c_int LIMIT 2;
    """

    order_qt_hdfs_overwrite_second """
        SELECT * FROM hdfs(
            ${hdfsReadProps("overwrite.csv", "csv")}
        ) ORDER BY c1;
    """

    // ============ 16. HDFS Append mode ============

    // First write
    sql """
        INSERT INTO hdfs(
            ${hdfsWriteProps("append.parquet", "parquet")},
            "delete_existing_files" = "true"
        ) SELECT c_int, c_varchar FROM insert_tvf_test_src WHERE c_int = 1000;
    """

    order_qt_hdfs_append_first """
        SELECT * FROM hdfs(
            ${hdfsReadProps("append.parquet", "parquet")}
        ) ORDER BY c_int;
    """

    // Second write (append)
    sql """
        INSERT INTO hdfs(
            ${hdfsWriteProps("append.parquet", "parquet")},
            "delete_existing_files" = "false"
        ) SELECT c_int, c_varchar FROM insert_tvf_test_src WHERE c_int = 2000;
    """

    order_qt_hdfs_append_second """
        SELECT * FROM hdfs(
            ${hdfsReadProps("append.parquet", "parquet")}
        ) ORDER BY c_int;
    """

    // ============ 17. HDFS Complex SELECT: constant expressions ============

    sql """
        INSERT INTO hdfs(
            ${hdfsWriteProps("const_expr.csv", "csv")},
            "delete_existing_files" = "true"
        ) SELECT 1, 'hello', 3.14, CAST('2024-01-01' AS DATE);
    """

    order_qt_hdfs_const_expr """
        SELECT * FROM hdfs(
            ${hdfsReadProps("const_expr.csv", "csv")}
        ) ORDER BY c1;
    """

    // ============ 18. HDFS Complex SELECT: WHERE + GROUP BY ============

    sql """
        INSERT INTO hdfs(
            ${hdfsWriteProps("where_groupby.csv", "csv")},
            "delete_existing_files" = "true"
        ) SELECT c_bool, COUNT(*), SUM(c_int) FROM insert_tvf_test_src WHERE c_int IS NOT NULL GROUP BY c_bool ORDER BY c_bool;
    """

    order_qt_hdfs_where_groupby """
        SELECT * FROM hdfs(
            ${hdfsReadProps("where_groupby.csv", "csv")}
        ) ORDER BY c1;
    """

    // ============ 19. HDFS Complex SELECT: JOIN ============

    sql """
        INSERT INTO hdfs(
            ${hdfsWriteProps("join_query.csv", "csv")},
            "delete_existing_files" = "true"
        ) SELECT a.c_int, a.c_varchar, b.c_label
          FROM insert_tvf_test_src a INNER JOIN insert_tvf_join_src b ON a.c_int = b.c_int
          ORDER BY a.c_int;
    """

    order_qt_hdfs_join_query """
        SELECT * FROM hdfs(
            ${hdfsReadProps("join_query.csv", "csv")}
        ) ORDER BY c1;
    """

    // ============ 20. HDFS Complex SELECT: subquery ============

    sql """
        INSERT INTO hdfs(
            ${hdfsWriteProps("subquery.csv", "csv")},
            "delete_existing_files" = "true"
        ) SELECT * FROM (SELECT c_int, c_varchar, c_string FROM insert_tvf_test_src WHERE c_int IS NOT NULL ORDER BY c_int) sub;
    """

    order_qt_hdfs_subquery """
        SELECT * FROM hdfs(
            ${hdfsReadProps("subquery.csv", "csv")}
        ) ORDER BY c1;
    """

    // ============ 21. HDFS Complex SELECT: type cast ============

    sql """
        INSERT INTO hdfs(
            ${hdfsWriteProps("type_cast.csv", "csv")},
            "delete_existing_files" = "true"
        ) SELECT CAST(c_int AS BIGINT), CAST(c_float AS DOUBLE), CAST(c_date AS STRING)
          FROM insert_tvf_test_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_hdfs_type_cast """
        SELECT * FROM hdfs(
            ${hdfsReadProps("type_cast.csv", "csv")}
        ) ORDER BY c1;
    """

    // ============ 22. HDFS Complex SELECT: UNION ALL ============

    sql """
        INSERT INTO hdfs(
            ${hdfsWriteProps("union_query.csv", "csv")},
            "delete_existing_files" = "true"
        ) SELECT c_int, c_varchar FROM insert_tvf_test_src WHERE c_int = 1000
          UNION ALL
          SELECT c_int, c_varchar FROM insert_tvf_test_src WHERE c_int = 2000;
    """

    order_qt_hdfs_union_query """
        SELECT * FROM hdfs(
            ${hdfsReadProps("union_query.csv", "csv")}
        ) ORDER BY c1;
    """

    // ============ 23. Error: missing file_path ============

    test {
        sql """
            INSERT INTO hdfs(
                "format" = "csv",
                "hadoop.username" = "${hdfsUserName}",
                "fs.defaultFS" = "${defaultFS}"
            ) SELECT 1;
        """
        exception "file_path"
    }

    // ============ 24. Error: missing format ============

    test {
        sql """
            INSERT INTO hdfs(
                "file_path" = "${hdfsBasePath}/err.csv",
                "hadoop.username" = "${hdfsUserName}",
                "fs.defaultFS" = "${defaultFS}"
            ) SELECT 1;
        """
        exception "format"
    }

    // ============ 25. Error: unsupported format ============

    test {
        sql """
            INSERT INTO hdfs(
                "file_path" = "${hdfsBasePath}/err.json",
                "format" = "json",
                "hadoop.username" = "${hdfsUserName}",
                "fs.defaultFS" = "${defaultFS}"
            ) SELECT 1;
        """
        exception "Unsupported"
    }

    // ============ Cleanup ============

    sql """ DROP TABLE IF EXISTS insert_tvf_test_src """
    sql """ DROP TABLE IF EXISTS insert_tvf_complex_src """
    sql """ DROP TABLE IF EXISTS insert_tvf_join_src """
}
