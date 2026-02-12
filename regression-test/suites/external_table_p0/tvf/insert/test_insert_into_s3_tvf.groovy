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

suite("test_insert_into_s3_tvf", "p0,external,external_docker") {

    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName")

    if (ak == null || ak == "" || sk == null || sk == "") {
        logger.info("S3 not configured, skip")
        return
    }

    def s3BasePath = "${bucket}/regression/insert_tvf_test"

    def s3WriteProps = { String path, String format ->
        return """
            "file_path" = "s3://${s3BasePath}/${path}",
            "format" = "${format}",
            "s3.endpoint" = "${s3_endpoint}",
            "s3.access_key" = "${ak}",
            "s3.secret_key" = "${sk}",
            "s3.region" = "${region}"
        """
    }

    def s3ReadProps = { String path, String format ->
        return """
            "uri" = "https://${bucket}.${s3_endpoint}/regression/insert_tvf_test/${path}",
            "s3.access_key" = "${ak}",
            "s3.secret_key" = "${sk}",
            "format" = "${format}",
            "region" = "${region}"
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

    // ============ 1. S3 CSV basic types ============

    sql """
        INSERT INTO s3(
            ${s3WriteProps("basic_csv.csv", "csv")}
        ) SELECT * FROM insert_tvf_test_src ORDER BY c_int;
    """

    order_qt_s3_csv_basic_types """
        SELECT * FROM s3(
            ${s3ReadProps("basic_csv.csv", "csv")}
        ) ORDER BY c1;
    """

    // ============ 2. S3 Parquet basic types ============

    sql """
        INSERT INTO s3(
            ${s3WriteProps("basic_parquet.parquet", "parquet")}
        ) SELECT * FROM insert_tvf_test_src ORDER BY c_int;
    """

    order_qt_s3_parquet_basic_types """
        SELECT * FROM s3(
            ${s3ReadProps("basic_parquet.parquet", "parquet")}
        ) ORDER BY c_int;
    """

    // ============ 3. S3 ORC basic types ============

    sql """
        INSERT INTO s3(
            ${s3WriteProps("basic_orc.orc", "orc")}
        ) SELECT * FROM insert_tvf_test_src ORDER BY c_int;
    """

    order_qt_s3_orc_basic_types """
        SELECT * FROM s3(
            ${s3ReadProps("basic_orc.orc", "orc")}
        ) ORDER BY c_int;
    """

    // ============ 4. S3 Parquet complex types ============

    sql """
        INSERT INTO s3(
            ${s3WriteProps("complex_parquet.parquet", "parquet")}
        ) SELECT * FROM insert_tvf_complex_src ORDER BY c_int;
    """

    order_qt_s3_parquet_complex_types """
        SELECT * FROM s3(
            ${s3ReadProps("complex_parquet.parquet", "parquet")}
        ) ORDER BY c_int;
    """

    // ============ 5. S3 ORC complex types ============

    sql """
        INSERT INTO s3(
            ${s3WriteProps("complex_orc.orc", "orc")}
        ) SELECT * FROM insert_tvf_complex_src ORDER BY c_int;
    """

    order_qt_s3_orc_complex_types """
        SELECT * FROM s3(
            ${s3ReadProps("complex_orc.orc", "orc")}
        ) ORDER BY c_int;
    """

    // ============ 6. S3 CSV separator: comma ============

    sql """
        INSERT INTO s3(
            ${s3WriteProps("sep_comma.csv", "csv")},
            "column_separator" = ","
        ) SELECT c_int, c_varchar, c_string FROM insert_tvf_test_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_s3_csv_sep_comma """
        SELECT * FROM s3(
            ${s3ReadProps("sep_comma.csv", "csv")},
            "column_separator" = ","
        ) ORDER BY c1;
    """

    // ============ 7. S3 CSV separator: tab ============

    sql """
        INSERT INTO s3(
            ${s3WriteProps("sep_tab.csv", "csv")},
            "column_separator" = "\t"
        ) SELECT c_int, c_varchar, c_string FROM insert_tvf_test_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_s3_csv_sep_tab """
        SELECT * FROM s3(
            ${s3ReadProps("sep_tab.csv", "csv")},
            "column_separator" = "\t"
        ) ORDER BY c1;
    """

    // ============ 8. S3 CSV separator: pipe ============

    sql """
        INSERT INTO s3(
            ${s3WriteProps("sep_pipe.csv", "csv")},
            "column_separator" = "|"
        ) SELECT c_int, c_varchar, c_string FROM insert_tvf_test_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_s3_csv_sep_pipe """
        SELECT * FROM s3(
            ${s3ReadProps("sep_pipe.csv", "csv")},
            "column_separator" = "|"
        ) ORDER BY c1;
    """

    // ============ 9. S3 CSV separator: multi-char ============

    sql """
        INSERT INTO s3(
            ${s3WriteProps("sep_multi.csv", "csv")},
            "column_separator" = ";;"
        ) SELECT c_int, c_varchar, c_string FROM insert_tvf_test_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_s3_csv_sep_multi """
        SELECT * FROM s3(
            ${s3ReadProps("sep_multi.csv", "csv")},
            "column_separator" = ";;"
        ) ORDER BY c1;
    """

    // ============ 10. S3 CSV line delimiter: CRLF ============

    sql """
        INSERT INTO s3(
            ${s3WriteProps("line_crlf.csv", "csv")},
            "line_delimiter" = "\r\n"
        ) SELECT c_int, c_varchar, c_string FROM insert_tvf_test_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_s3_csv_line_crlf """
        SELECT * FROM s3(
            ${s3ReadProps("line_crlf.csv", "csv")},
            "line_delimiter" = "\r\n"
        ) ORDER BY c1;
    """

    // ============ 11. S3 CSV compress: gz ============

    sql """
        INSERT INTO s3(
            ${s3WriteProps("compress_gz.csv.gz", "csv")},
            "compression_type" = "gz"
        ) SELECT c_int, c_varchar, c_string FROM insert_tvf_test_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_s3_csv_compress_gz """
        SELECT * FROM s3(
            ${s3ReadProps("compress_gz.csv.gz", "csv")},
            "compress_type" = "gz"
        ) ORDER BY c1;
    """

    // ============ 12. S3 CSV compress: zstd ============

    sql """
        INSERT INTO s3(
            ${s3WriteProps("compress_zstd.csv.zst", "csv")},
            "compression_type" = "zstd"
        ) SELECT c_int, c_varchar, c_string FROM insert_tvf_test_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_s3_csv_compress_zstd """
        SELECT * FROM s3(
            ${s3ReadProps("compress_zstd.csv.zst", "csv")},
            "compress_type" = "zstd"
        ) ORDER BY c1;
    """

    // ============ 13. S3 CSV compress: lz4 ============

    sql """
        INSERT INTO s3(
            ${s3WriteProps("compress_lz4.csv.lz4", "csv")},
            "compression_type" = "lz4block"
        ) SELECT c_int, c_varchar, c_string FROM insert_tvf_test_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_s3_csv_compress_lz4 """
        SELECT * FROM s3(
            ${s3ReadProps("compress_lz4.csv.lz4", "csv")},
            "compress_type" = "lz4block"
        ) ORDER BY c1;
    """

    // ============ 14. S3 CSV compress: snappy ============

    sql """
        INSERT INTO s3(
            ${s3WriteProps("compress_snappy.csv.snappy", "csv")},
            "compression_type" = "snappyblock"
        ) SELECT c_int, c_varchar, c_string FROM insert_tvf_test_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_s3_csv_compress_snappy """
        SELECT * FROM s3(
            ${s3ReadProps("compress_snappy.csv.snappy", "csv")},
            "compress_type" = "snappyblock"
        ) ORDER BY c1;
    """

    // ============ 15. S3 Overwrite mode ============

    // First write: 5 rows
    sql """
        INSERT INTO s3(
            ${s3WriteProps("overwrite.csv", "csv")}
        ) SELECT c_int, c_varchar FROM insert_tvf_test_src ORDER BY c_int;
    """

    order_qt_s3_overwrite_first """
        SELECT * FROM s3(
            ${s3ReadProps("overwrite.csv", "csv")}
        ) ORDER BY c1;
    """

    // Second write: 2 rows with overwrite
    sql """
        INSERT INTO s3(
            ${s3WriteProps("overwrite.csv", "csv")},
            "delete_existing_files" = "true"
        ) SELECT c_int, c_varchar FROM insert_tvf_test_src WHERE c_int > 0 ORDER BY c_int LIMIT 2;
    """

    order_qt_s3_overwrite_second """
        SELECT * FROM s3(
            ${s3ReadProps("overwrite.csv", "csv")}
        ) ORDER BY c1;
    """

    // ============ 16. S3 Append mode ============

    // First write
    sql """
        INSERT INTO s3(
            ${s3WriteProps("append.parquet", "parquet")},
            "delete_existing_files" = "false"
        ) SELECT c_int, c_varchar FROM insert_tvf_test_src WHERE c_int = 1000;
    """

    order_qt_s3_append_first """
        SELECT * FROM s3(
            ${s3ReadProps("append.parquet", "parquet")}
        ) ORDER BY c_int;
    """

    // Second write (append)
    sql """
        INSERT INTO s3(
            ${s3WriteProps("append.parquet", "parquet")},
            "delete_existing_files" = "false"
        ) SELECT c_int, c_varchar FROM insert_tvf_test_src WHERE c_int = 2000;
    """

    order_qt_s3_append_second """
        SELECT * FROM s3(
            ${s3ReadProps("append.parquet", "parquet")}
        ) ORDER BY c_int;
    """

    // ============ 17. S3 Complex SELECT: constant expressions ============

    sql """
        INSERT INTO s3(
            ${s3WriteProps("const_expr.csv", "csv")}
        ) SELECT 1, 'hello', 3.14, CAST('2024-01-01' AS DATE);
    """

    order_qt_s3_const_expr """
        SELECT * FROM s3(
            ${s3ReadProps("const_expr.csv", "csv")}
        ) ORDER BY c1;
    """

    // ============ 18. S3 Complex SELECT: WHERE + GROUP BY ============

    sql """
        INSERT INTO s3(
            ${s3WriteProps("where_groupby.csv", "csv")}
        ) SELECT c_bool, COUNT(*), SUM(c_int) FROM insert_tvf_test_src WHERE c_int IS NOT NULL GROUP BY c_bool ORDER BY c_bool;
    """

    order_qt_s3_where_groupby """
        SELECT * FROM s3(
            ${s3ReadProps("where_groupby.csv", "csv")}
        ) ORDER BY c1;
    """

    // ============ 19. S3 Complex SELECT: JOIN ============

    sql """
        INSERT INTO s3(
            ${s3WriteProps("join_query.csv", "csv")}
        ) SELECT a.c_int, a.c_varchar, b.c_label
          FROM insert_tvf_test_src a INNER JOIN insert_tvf_join_src b ON a.c_int = b.c_int
          ORDER BY a.c_int;
    """

    order_qt_s3_join_query """
        SELECT * FROM s3(
            ${s3ReadProps("join_query.csv", "csv")}
        ) ORDER BY c1;
    """

    // ============ 20. S3 Complex SELECT: subquery ============

    sql """
        INSERT INTO s3(
            ${s3WriteProps("subquery.csv", "csv")}
        ) SELECT * FROM (SELECT c_int, c_varchar, c_string FROM insert_tvf_test_src WHERE c_int IS NOT NULL ORDER BY c_int) sub;
    """

    order_qt_s3_subquery """
        SELECT * FROM s3(
            ${s3ReadProps("subquery.csv", "csv")}
        ) ORDER BY c1;
    """

    // ============ 21. S3 Complex SELECT: type cast ============

    sql """
        INSERT INTO s3(
            ${s3WriteProps("type_cast.csv", "csv")}
        ) SELECT CAST(c_int AS BIGINT), CAST(c_float AS DOUBLE), CAST(c_date AS STRING)
          FROM insert_tvf_test_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_s3_type_cast """
        SELECT * FROM s3(
            ${s3ReadProps("type_cast.csv", "csv")}
        ) ORDER BY c1;
    """

    // ============ 22. S3 Complex SELECT: UNION ALL ============

    sql """
        INSERT INTO s3(
            ${s3WriteProps("union_query.csv", "csv")}
        ) SELECT c_int, c_varchar FROM insert_tvf_test_src WHERE c_int = 1000
          UNION ALL
          SELECT c_int, c_varchar FROM insert_tvf_test_src WHERE c_int = 2000;
    """

    order_qt_s3_union_query """
        SELECT * FROM s3(
            ${s3ReadProps("union_query.csv", "csv")}
        ) ORDER BY c1;
    """

    // ============ 23. Error: missing file_path ============

    test {
        sql """
            INSERT INTO s3(
                "format" = "csv",
                "s3.endpoint" = "${s3_endpoint}",
                "s3.access_key" = "${ak}",
                "s3.secret_key" = "${sk}",
                "s3.region" = "${region}"
            ) SELECT 1;
        """
        exception "file_path"
    }

    // ============ 24. Error: missing format ============

    test {
        sql """
            INSERT INTO s3(
                "file_path" = "s3://${s3BasePath}/err.csv",
                "s3.endpoint" = "${s3_endpoint}",
                "s3.access_key" = "${ak}",
                "s3.secret_key" = "${sk}",
                "s3.region" = "${region}"
            ) SELECT 1;
        """
        exception "format"
    }

    // ============ 25. Error: unsupported format ============

    test {
        sql """
            INSERT INTO s3(
                "file_path" = "s3://${s3BasePath}/err.json",
                "format" = "json",
                "s3.endpoint" = "${s3_endpoint}",
                "s3.access_key" = "${ak}",
                "s3.secret_key" = "${sk}",
                "s3.region" = "${region}"
            ) SELECT 1;
        """
        exception "Unsupported"
    }

    // ============ Cleanup ============

    sql """ DROP TABLE IF EXISTS insert_tvf_test_src """
    sql """ DROP TABLE IF EXISTS insert_tvf_complex_src """
    sql """ DROP TABLE IF EXISTS insert_tvf_join_src """
}
