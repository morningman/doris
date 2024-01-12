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

package org.apache.doris.avro;

import org.apache.doris.common.jni.utils.OffHeap;
import org.apache.doris.common.jni.vec.VectorTable;

import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AvroJNIScannerTest {

    @Test
    public void testGetNext() throws IOException {
        OffHeap.setTesting();
        AvroJNIScanner scanner = new AvroJNIScanner(10, buildParamsKafka());
        scanner.open();
        long metaAddress = 0;
        do {
            metaAddress = scanner.getNextBatchMeta();
            if (metaAddress != 0) {
                long rows = OffHeap.getLong(null, metaAddress);
                VectorTable restoreTable = VectorTable.createReadableTable(scanner.getTable().getColumnTypes(),
                        scanner.getTable().getFields(), metaAddress);
                System.out.println(restoreTable.dump((int) rows));
            }
            scanner.resetTable();
        } while (metaAddress != 0);
        scanner.releaseTable();
        scanner.close();
    }

    private Map<String, String> buildParams() {
        Map<String, String> requiredParams = new HashMap<>();
        // https://zyk-gz-1316291683.cos.ap-guangzhou.myqcloud.com/path/all_complex.avro
        requiredParams.put("is_get_table_schema", "false");
        requiredParams.put("file_type", "3");
        requiredParams.put("uri", "s3://zyk-gz-1316291683/path/all_type.avro");
        requiredParams.put("s3.access_key", "");
        requiredParams.put("s3.secret_key", "");
        requiredParams.put("s3.endpoint", "cos.ap-guangzhou.myqcloud.com");
        requiredParams.put("s3.region", "ap-guangzhou");
        // requiredParams.put("columns_types", "boolean#int#bigint#float#double#string#binary#binary#array<int>#map<string,int>#string#struct<c1:int,c2:double,c3:string>#struct<c1:string>");
        requiredParams.put("columns_types", "boolean#int#bigint#float#double#string#array<int>#map<string,int>#string#struct<c1:int,c2:double,c3:string>#struct<c1:string>#map<string,array<bigint>>#array<map<string,boolean>>");
        // requiredParams.put("required_fields", "aBoolean,aInt,aLong,aFloat,aDouble,aString,aBytes,aFixed,anArray,aMap,anEnum,aRecord,aUnion");
        requiredParams.put("required_fields", "aBoolean,aInt,aLong,aFloat,aDouble,aString,anArray,aMap,anEnum,aRecord,aUnion,mapArrayLong,arrayMapBoolean");
        requiredParams.put("hive.serde", "org.apache.hadoop.hive.serde2.avro.AvroSerDe");

        return requiredParams;
    }

    private Map<String, String> buildParams2() {
        Map<String, String> requiredParams = new HashMap<>();
        requiredParams.put("is_get_table_schema", "false");
        requiredParams.put("file_type", "3");
        requiredParams.put("uri", "s3://zyk-gz-1316291683/path/avro_all_types");
        requiredParams.put("s3.access_key", "");
        requiredParams.put("s3.secret_key", "");
        requiredParams.put("s3.endpoint", "cos.ap-guangzhou.myqcloud.com");
        requiredParams.put("s3.region", "ap-guangzhou");
        //  required_fields##########t_null_string,t_null_varchar,t_null_char,t_null_array_int,t_null_decimal_precision_2,t_null_decimal_precision_4,t_null_decimal_precision_8,t_null_decimal_precision_17,t_null_decimal_precision_18,t_null_decimal_precision_38,t_empty_string,t_string,t_empty_varchar,t_varchar,t_varchar_max_length,t_char,t_int,t_bigint,t_float,t_double,t_boolean_true,t_boolean_false,t_date,t_timestamp,t_decimal_precision_2,t_decimal_precision_4,t_decimal_precision_8,t_decimal_precision_17,t_decimal_precision_18,t_decimal_precision_38,t_binary,t_map_string,t_array_empty,t_array_string,t_array_int,t_array_bigint,t_array_float,t_array_double,t_array_boolean,t_array_varchar,t_array_char,t_array_date,t_array_timestamp,t_array_decimal_precision_2,t_array_decimal_precision_4,t_array_decimal_precision_8,t_array_decimal_precision_17,t_array_decimal_precision_18,t_array_decimal_precision_38,t_struct_bigint,t_complex,t_struct_nested,t_struct_null,t_struct_non_nulls_after_nulls,t_array_string_starting_with_nulls,t_array_string_with_nulls_in_between,t_array_string_ending_with_nulls,t_array_string_all_nulls
        // columns_types############struct<string:string>#struct<string:string>#struct<string:string>#struct<array:array<struct<int:int>>>#struct<bytes:string>#struct<bytes:string>#struct<bytes:string>#struct<bytes:string>#struct<bytes:string>#struct<bytes:string>#struct<string:string>#struct<string:string>#struct<string:string>#struct<string:string>#struct<string:string>#struct<string:string>#struct<int:int>#struct<long:bigint>#struct<float:float>#struct<double:double>#struct<boolean:boolean>#struct<boolean:boolean>#struct<int:int>#struct<long:bigint>#struct<bytes:string>#struct<bytes:string>#struct<bytes:string>#struct<bytes:string>#struct<bytes:string>#struct<bytes:string>#struct<bytes:string>#struct<map:map<string,struct<string:string>>>#struct<array:array<struct<string:string>>>#struct<array:array<struct<string:string>>>#struct<array:array<struct<int:int>>>#struct<array:array<struct<long:bigint>>>#struct<array:array<struct<float:float>>>#struct<array:array<struct<double:double>>>#struct<array:array<struct<boolean:boolean>>>#struct<array:array<struct<string:string>>>#struct<array:array<struct<string:string>>>#struct<array:array<struct<int:int>>>#struct<array:array<struct<long:bigint>>>#struct<array:array<struct<bytes:string>>>#struct<array:array<struct<bytes:string>>>#struct<array:array<struct<bytes:string>>>#struct<array:array<struct<bytes:string>>>#struct<array:array<struct<bytes:string>>>#struct<array:array<struct<bytes:string>>>#struct<record_0:struct<s_bigint:struct<long:bigint>>>#struct<map:map<string,struct<array:array<struct<record_1:struct<s_int:struct<int:int>>>>>>>#struct<record_2:struct<struct_field:struct<array:array<struct<string:string>>>>>#struct<record_3:struct<struct_field_null:struct<string:string>,struct_field_null2:struct<string:string>>>#struct<record_4:struct<struct_non_nulls_after_nulls1:struct<int:int>,struct_non_nulls_after_nulls2:struct<string:string>>>#struct<array:array<struct<string:string>>>#struct<array:array<struct<string:string>>>#struct<array:array<struct<string:string>>>#struct<array:array<struct<string:string>>>
        requiredParams.put("columns_types", "struct<string:string>#struct<string:string>#struct<string:string>#struct<array:array<struct<int:int>>>#struct<bytes:string>#struct<bytes:string>#struct<bytes:string>#struct<bytes:string>#struct<bytes:string>#struct<bytes:string>#struct<string:string>#struct<string:string>#struct<string:string>#struct<string:string>#struct<string:string>#struct<string:string>#struct<int:int>#struct<long:bigint>#struct<float:float>#struct<double:double>#struct<boolean:boolean>#struct<boolean:boolean>#struct<int:int>#struct<long:bigint>#struct<bytes:string>#struct<bytes:string>#struct<bytes:string>#struct<bytes:string>#struct<bytes:string>#struct<bytes:string>#struct<bytes:string>#struct<map:map<string,struct<string:string>>>#struct<array:array<struct<string:string>>>#struct<array:array<struct<string:string>>>#struct<array:array<struct<int:int>>>#struct<array:array<struct<long:bigint>>>#struct<array:array<struct<float:float>>>#struct<array:array<struct<double:double>>>#struct<array:array<struct<boolean:boolean>>>#struct<array:array<struct<string:string>>>#struct<array:array<struct<string:string>>>#struct<array:array<struct<int:int>>>#struct<array:array<struct<long:bigint>>>#struct<array:array<struct<bytes:string>>>#struct<array:array<struct<bytes:string>>>#struct<array:array<struct<bytes:string>>>#struct<array:array<struct<bytes:string>>>#struct<array:array<struct<bytes:string>>>#struct<array:array<struct<bytes:string>>>#struct<record_0:struct<s_bigint:struct<long:bigint>>>#struct<map:map<string,struct<array:array<struct<record_1:struct<s_int:struct<int:int>>>>>>>#struct<record_2:struct<struct_field:struct<array:array<struct<string:string>>>>>#struct<record_3:struct<struct_field_null:struct<string:string>,struct_field_null2:struct<string:string>>>#struct<record_4:struct<struct_non_nulls_after_nulls1:struct<int:int>,struct_non_nulls_after_nulls2:struct<string:string>>>#struct<array:array<struct<string:string>>>#struct<array:array<struct<string:string>>>#struct<array:array<struct<string:string>>>#struct<array:array<struct<string:string>>>");
        requiredParams.put("required_fields", "t_null_string,t_null_varchar,t_null_char,t_null_array_int,t_null_decimal_precision_2,t_null_decimal_precision_4,t_null_decimal_precision_8,t_null_decimal_precision_17,t_null_decimal_precision_18,t_null_decimal_precision_38,t_empty_string,t_string,t_empty_varchar,t_varchar,t_varchar_max_length,t_char,t_int,t_bigint,t_float,t_double,t_boolean_true,t_boolean_false,t_date,t_timestamp,t_decimal_precision_2,t_decimal_precision_4,t_decimal_precision_8,t_decimal_precision_17,t_decimal_precision_18,t_decimal_precision_38,t_binary,t_map_string,t_array_empty,t_array_string,t_array_int,t_array_bigint,t_array_float,t_array_double,t_array_boolean,t_array_varchar,t_array_char,t_array_date,t_array_timestamp,t_array_decimal_precision_2,t_array_decimal_precision_4,t_array_decimal_precision_8,t_array_decimal_precision_17,t_array_decimal_precision_18,t_array_decimal_precision_38,t_struct_bigint,t_complex,t_struct_nested,t_struct_null,t_struct_non_nulls_after_nulls,t_array_string_starting_with_nulls,t_array_string_with_nulls_in_between,t_array_string_ending_with_nulls,t_array_string_all_nulls");
        requiredParams.put("hive.serde", "org.apache.hadoop.hive.serde2.avro.AvroSerDe");

        return requiredParams;
    }

    // select * from hdfs(
    //             "uri" = "hdfs://10.16.10.6:8012/path/all_type.avro",
    //             "fs.defaultFS" = "hdfs://10.16.10.6:8012",
    //             "hadoop.username" = "doris",
    //             "format" = "avro");
    private Map<String, String> buildParamsHdfs() {
        Map<String, String> requiredParams = new HashMap<>();
        requiredParams.put("is_get_table_schema", "false");
        requiredParams.put("file_type", "4");
        requiredParams.put("uri", "hdfs://10.16.10.6:8121/wdl/path/all_type.avro");
        requiredParams.put("fs.defaultFS", "hdfs://10.16.10.6:8121");
        requiredParams.put("hadoop.username", "doris");
        requiredParams.put("columns_types", "boolean#int#bigint#float#double#string#array<int>#map<string,int>#string#struct<a:int,b:double,c:string>#struct<string:string>#map<string,array<bigint>>#array<map<string,boolean>>");
        requiredParams.put("required_fields", "aBoolean,aInt,aLong,aFloat,aDouble,aString,anArray,aMap,anEnum,aRecord,aUnion,mapArrayLong,arrayMapBoolean");
        requiredParams.put("hive.serde", "org.apache.hadoop.hive.serde2.avro.AvroSerDe");

        return requiredParams;
    }

    private Map<String, String> buildParamsKafka() {
        Map<String, String> requiredParams = new HashMap<>();
        requiredParams.put("is_get_table_schema", "false");
        requiredParams.put("file_type", "6");
        requiredParams.put("topic", "avro-complex7");

        // requiredParams.put("partition", "4");
        // requiredParams.put("start.offset", "100");
        // requiredParams.put("max.rows", "100");

        requiredParams.put("split_size", "4");
        requiredParams.put("split_start_offset", "100");
        requiredParams.put("split_file_size", "10");

        requiredParams.put("broker_list", "10.16.10.6:9092");
        requiredParams.put("group.id", "doris-consumer-group");
        // requiredParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // requiredParams.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        requiredParams.put("schema.registry.url", "http://10.16.10.6:8082");

        requiredParams.put("columns_types", "varchar(65533)#boolean#string#string#string#map<string,bigint>#array<bigint>#double#bigint");
        requiredParams.put("required_fields", "name,boolean_type,favorite_number,favorite_color,enum_value,map_value,array_value,double_type,long_type");
        requiredParams.put("hive.serde", "org.apache.hadoop.hive.serde2.avro.AvroSerDe");
        return requiredParams;
    }

}
