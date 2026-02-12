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

suite("test_insert_into_local_tvf", "p0,tvf,external,external_docker") {

    List<List<Object>> backends = sql """ show backends """
    assertTrue(backends.size() > 0)
    def be_id = backends[0][0]
    def be_host = backends[0][1]
    def basePath = "/tmp/test_insert_into_local_tvf"

    // Clean and create basePath on the BE node
    sshExec("root", be_host, "rm -rf ${basePath}", false)
    sshExec("root", be_host, "mkdir -p ${basePath}")
    sshExec("root", be_host, "chmod 777 ${basePath}")

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

    // ============ 1. CSV basic types ============

    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/basic_csv.csv",
            "backend_id" = "${be_id}",
            "format" = "csv"
        ) SELECT * FROM insert_tvf_test_src ORDER BY c_int;
    """

    order_qt_csv_basic_types """
        SELECT * FROM local(
            "file_path" = "${basePath}/basic_csv.csv",
            "backend_id" = "${be_id}",
            "format" = "csv"
        ) ORDER BY c1;
    """

    // ============ 2. Parquet basic types ============

    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/basic_parquet.parquet",
            "backend_id" = "${be_id}",
            "format" = "parquet"
        ) SELECT * FROM insert_tvf_test_src ORDER BY c_int;
    """

    order_qt_parquet_basic_types """
        SELECT * FROM local(
            "file_path" = "${basePath}/basic_parquet.parquet",
            "backend_id" = "${be_id}",
            "format" = "parquet"
        ) ORDER BY c_int;
    """

    // ============ 3. ORC basic types ============

    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/basic_orc.orc",
            "backend_id" = "${be_id}",
            "format" = "orc"
        ) SELECT * FROM insert_tvf_test_src ORDER BY c_int;
    """

    order_qt_orc_basic_types """
        SELECT * FROM local(
            "file_path" = "${basePath}/basic_orc.orc",
            "backend_id" = "${be_id}",
            "format" = "orc"
        ) ORDER BY c_int;
    """

    // ============ 4. Parquet complex types ============

    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/complex_parquet.parquet",
            "backend_id" = "${be_id}",
            "format" = "parquet"
        ) SELECT * FROM insert_tvf_complex_src ORDER BY c_int;
    """

    order_qt_parquet_complex_types """
        SELECT * FROM local(
            "file_path" = "${basePath}/complex_parquet.parquet",
            "backend_id" = "${be_id}",
            "format" = "parquet"
        ) ORDER BY c_int;
    """

    // ============ 5. ORC complex types ============

    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/complex_orc.orc",
            "backend_id" = "${be_id}",
            "format" = "orc"
        ) SELECT * FROM insert_tvf_complex_src ORDER BY c_int;
    """

    order_qt_orc_complex_types """
        SELECT * FROM local(
            "file_path" = "${basePath}/complex_orc.orc",
            "backend_id" = "${be_id}",
            "format" = "orc"
        ) ORDER BY c_int;
    """

    // ============ 6. CSV separator: comma ============

    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/sep_comma.csv",
            "backend_id" = "${be_id}",
            "format" = "csv",
            "column_separator" = ","
        ) SELECT c_int, c_varchar, c_string FROM insert_tvf_test_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_csv_sep_comma """
        SELECT * FROM local(
            "file_path" = "${basePath}/sep_comma.csv",
            "backend_id" = "${be_id}",
            "format" = "csv",
            "column_separator" = ","
        ) ORDER BY c1;
    """

    // ============ 7. CSV separator: tab ============

    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/sep_tab.csv",
            "backend_id" = "${be_id}",
            "format" = "csv",
            "column_separator" = "\t"
        ) SELECT c_int, c_varchar, c_string FROM insert_tvf_test_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_csv_sep_tab """
        SELECT * FROM local(
            "file_path" = "${basePath}/sep_tab.csv",
            "backend_id" = "${be_id}",
            "format" = "csv",
            "column_separator" = "\t"
        ) ORDER BY c1;
    """

    // ============ 8. CSV separator: pipe ============

    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/sep_pipe.csv",
            "backend_id" = "${be_id}",
            "format" = "csv",
            "column_separator" = "|"
        ) SELECT c_int, c_varchar, c_string FROM insert_tvf_test_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_csv_sep_pipe """
        SELECT * FROM local(
            "file_path" = "${basePath}/sep_pipe.csv",
            "backend_id" = "${be_id}",
            "format" = "csv",
            "column_separator" = "|"
        ) ORDER BY c1;
    """

    // ============ 9. CSV separator: multi-char ============

    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/sep_multi.csv",
            "backend_id" = "${be_id}",
            "format" = "csv",
            "column_separator" = ";;"
        ) SELECT c_int, c_varchar, c_string FROM insert_tvf_test_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_csv_sep_multi """
        SELECT * FROM local(
            "file_path" = "${basePath}/sep_multi.csv",
            "backend_id" = "${be_id}",
            "format" = "csv",
            "column_separator" = ";;"
        ) ORDER BY c1;
    """

    // ============ 10. CSV line delimiter: CRLF ============

    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/line_crlf.csv",
            "backend_id" = "${be_id}",
            "format" = "csv",
            "line_delimiter" = "\r\n"
        ) SELECT c_int, c_varchar, c_string FROM insert_tvf_test_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_csv_line_crlf """
        SELECT * FROM local(
            "file_path" = "${basePath}/line_crlf.csv",
            "backend_id" = "${be_id}",
            "format" = "csv",
            "line_delimiter" = "\r\n"
        ) ORDER BY c1;
    """

    // ============ 11. CSV compress: gz ============

    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/compress_gz.csv.gz",
            "backend_id" = "${be_id}",
            "format" = "csv",
            "compression_type" = "gz"
        ) SELECT c_int, c_varchar, c_string FROM insert_tvf_test_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_csv_compress_gz """
        SELECT * FROM local(
            "file_path" = "${basePath}/compress_gz.csv.gz",
            "backend_id" = "${be_id}",
            "format" = "csv",
            "compress_type" = "gz"
        ) ORDER BY c1;
    """

    // ============ 12. CSV compress: zstd ============

    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/compress_zstd.csv.zst",
            "backend_id" = "${be_id}",
            "format" = "csv",
            "compression_type" = "zstd"
        ) SELECT c_int, c_varchar, c_string FROM insert_tvf_test_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_csv_compress_zstd """
        SELECT * FROM local(
            "file_path" = "${basePath}/compress_zstd.csv.zst",
            "backend_id" = "${be_id}",
            "format" = "csv",
            "compress_type" = "zstd"
        ) ORDER BY c1;
    """

    // ============ 13. CSV compress: lz4 ============

    // TODO: lz4 read meet error: LZ4F_getFrameInfo error: ERROR_frameType_unknown
    // sql """
    //     INSERT INTO local(
    //         "file_path" = "${basePath}/compress_lz4.csv.lz4",
    //         "backend_id" = "${be_id}",
    //         "format" = "csv",
    //         "compression_type" = "lz4"
    //     ) SELECT c_int, c_varchar, c_string FROM insert_tvf_test_src WHERE c_int IS NOT NULL ORDER BY c_int;
    // """

    // order_qt_csv_compress_lz4 """
    //     SELECT * FROM local(
    //         "file_path" = "${basePath}/compress_lz4.csv.lz4",
    //         "backend_id" = "${be_id}",
    //         "format" = "csv",
    //         "compress_type" = "lz4"
    //     ) ORDER BY c1;
    // """

    // ============ 14. CSV compress: snappy ============

    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/compress_snappy.csv.snappy",
            "backend_id" = "${be_id}",
            "format" = "csv",
            "compression_type" = "snappy"
        ) SELECT c_int, c_varchar, c_string FROM insert_tvf_test_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_csv_compress_snappy """
        SELECT * FROM local(
            "file_path" = "${basePath}/compress_snappy.csv.snappy",
            "backend_id" = "${be_id}",
            "format" = "csv",
            "compress_type" = "snappyblock"
        ) ORDER BY c1;
    """

    // ============ 15. Overwrite mode (delete_existing_files=true) ============

    // First write: 5 rows
    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/overwrite.csv",
            "backend_id" = "${be_id}",
            "format" = "csv"
        ) SELECT c_int, c_varchar FROM insert_tvf_test_src ORDER BY c_int;
    """

    order_qt_overwrite_first """
        SELECT * FROM local(
            "file_path" = "${basePath}/overwrite.csv",
            "backend_id" = "${be_id}",
            "format" = "csv"
        ) ORDER BY c1;
    """

    // Second write: 2 rows with overwrite
    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/overwrite.csv",
            "backend_id" = "${be_id}",
            "format" = "csv",
            "delete_existing_files" = "true"
        ) SELECT c_int, c_varchar FROM insert_tvf_test_src WHERE c_int > 0 ORDER BY c_int LIMIT 2;
    """

    order_qt_overwrite_second """
        SELECT * FROM local(
            "file_path" = "${basePath}/overwrite.csv",
            "backend_id" = "${be_id}",
            "format" = "csv"
        ) ORDER BY c1;
    """

    // ============ 16. Append mode (delete_existing_files=false) ============

    // First write
    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/append.parquet",
            "backend_id" = "${be_id}",
            "format" = "parquet",
            "delete_existing_files" = "false"
        ) SELECT c_int, c_varchar FROM insert_tvf_test_src WHERE c_int = 1000;
    """

    order_qt_append_first """
        SELECT * FROM local(
            "file_path" = "${basePath}/append.parquet",
            "backend_id" = "${be_id}",
            "format" = "parquet"
        ) ORDER BY c_int;
    """

    // Second write (append)
    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/append.parquet",
            "backend_id" = "${be_id}",
            "format" = "parquet",
            "delete_existing_files" = "false"
        ) SELECT c_int, c_varchar FROM insert_tvf_test_src WHERE c_int = 2000;
    """

    order_qt_append_second """
        SELECT * FROM local(
            "file_path" = "${basePath}/append.parquet",
            "backend_id" = "${be_id}",
            "format" = "parquet"
        ) ORDER BY c_int;
    """

    // ============ 17. Complex SELECT: constant expressions ============

    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/const_expr.csv",
            "backend_id" = "${be_id}",
            "format" = "csv"
        ) SELECT 1, 'hello', 3.14, CAST('2024-01-01' AS DATE);
    """

    order_qt_const_expr """
        SELECT * FROM local(
            "file_path" = "${basePath}/const_expr.csv",
            "backend_id" = "${be_id}",
            "format" = "csv"
        ) ORDER BY c1;
    """

    // ============ 18. Complex SELECT: WHERE + GROUP BY ============

    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/where_groupby.csv",
            "backend_id" = "${be_id}",
            "format" = "csv"
        ) SELECT c_bool, COUNT(*), SUM(c_int) FROM insert_tvf_test_src WHERE c_int IS NOT NULL GROUP BY c_bool ORDER BY c_bool;
    """

    order_qt_where_groupby """
        SELECT * FROM local(
            "file_path" = "${basePath}/where_groupby.csv",
            "backend_id" = "${be_id}",
            "format" = "csv"
        ) ORDER BY c1;
    """

    // ============ 19. Complex SELECT: JOIN ============

    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/join_query.csv",
            "backend_id" = "${be_id}",
            "format" = "csv"
        ) SELECT a.c_int, a.c_varchar, b.c_label
          FROM insert_tvf_test_src a INNER JOIN insert_tvf_join_src b ON a.c_int = b.c_int
          ORDER BY a.c_int;
    """

    order_qt_join_query """
        SELECT * FROM local(
            "file_path" = "${basePath}/join_query.csv",
            "backend_id" = "${be_id}",
            "format" = "csv"
        ) ORDER BY c1;
    """

    // ============ 20. Complex SELECT: subquery ============

    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/subquery.csv",
            "backend_id" = "${be_id}",
            "format" = "csv"
        ) SELECT * FROM (SELECT c_int, c_varchar, c_string FROM insert_tvf_test_src WHERE c_int IS NOT NULL ORDER BY c_int) sub;
    """

    order_qt_subquery """
        SELECT * FROM local(
            "file_path" = "${basePath}/subquery.csv",
            "backend_id" = "${be_id}",
            "format" = "csv"
        ) ORDER BY c1;
    """

    // ============ 21. Complex SELECT: type cast ============

    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/type_cast.csv",
            "backend_id" = "${be_id}",
            "format" = "csv"
        ) SELECT CAST(c_int AS BIGINT), CAST(c_float AS DOUBLE), CAST(c_date AS STRING)
          FROM insert_tvf_test_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_type_cast """
        SELECT * FROM local(
            "file_path" = "${basePath}/type_cast.csv",
            "backend_id" = "${be_id}",
            "format" = "csv"
        ) ORDER BY c1;
    """

    // ============ 22. Complex SELECT: UNION ALL ============

    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/union_query.csv",
            "backend_id" = "${be_id}",
            "format" = "csv"
        ) SELECT c_int, c_varchar FROM insert_tvf_test_src WHERE c_int = 1000
          UNION ALL
          SELECT c_int, c_varchar FROM insert_tvf_test_src WHERE c_int = 2000;
    """

    order_qt_union_query """
        SELECT * FROM local(
            "file_path" = "${basePath}/union_query.csv",
            "backend_id" = "${be_id}",
            "format" = "csv"
        ) ORDER BY c1;
    """

    // ============ 23. Error: missing file_path ============

    test {
        sql """
            INSERT INTO local(
                "backend_id" = "${be_id}",
                "format" = "csv"
            ) SELECT 1;
        """
        exception "file_path"
    }

    // ============ 24. Error: missing format ============

    test {
        sql """
            INSERT INTO local(
                "file_path" = "${basePath}/err.csv",
                "backend_id" = "${be_id}"
            ) SELECT 1;
        """
        exception "format"
    }

    // ============ 25. Error: missing backend_id for local ============

    test {
        sql """
            INSERT INTO local(
                "file_path" = "${basePath}/err.csv",
                "format" = "csv"
            ) SELECT 1;
        """
        exception "backend_id"
    }

    // ============ 26. Error: unsupported TVF name ============

    test {
        sql """
            INSERT INTO unknown_tvf(
                "file_path" = "/tmp/err.csv",
                "format" = "csv"
            ) SELECT 1;
        """
        exception "INSERT INTO TVF only supports"
    }

    // ============ 27. Error: unsupported format ============

    test {
        sql """
            INSERT INTO local(
                "file_path" = "${basePath}/err.json",
                "backend_id" = "${be_id}",
                "format" = "json"
            ) SELECT 1;
        """
        exception "Unsupported TVF sink format"
    }

    // ============ Cleanup ============

    sshExec("root", be_host, "rm -rf ${basePath}", false)
    sql """ DROP TABLE IF EXISTS insert_tvf_test_src """
    sql """ DROP TABLE IF EXISTS insert_tvf_complex_src """
    sql """ DROP TABLE IF EXISTS insert_tvf_join_src """
}
