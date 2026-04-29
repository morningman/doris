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

package org.apache.doris.datasource;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.persist.gson.GsonUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Persistence-compatibility tests for the M3-15 cutover where iceberg catalogs
 * stop being persisted as IcebergXxxExternalCatalog and start going through the
 * SPI path as PluginDrivenExternalCatalog.
 *
 * <p>Verifies that GSON RuntimeTypeAdapter aliases redirect every legacy
 * iceberg catalog/database/table simple-name to the PluginDriven equivalent so
 * pre-cutover image / editlog entries keep loading.</p>
 */
public class IcebergCatalogImageCompatTest {

    private static final String[] LEGACY_CATALOG_SIMPLE_NAMES = new String[] {
            "IcebergExternalCatalog",
            "IcebergHMSExternalCatalog",
            "IcebergGlueExternalCatalog",
            "IcebergRestExternalCatalog",
            "IcebergDLFExternalCatalog",
            "IcebergHadoopExternalCatalog",
            "IcebergJdbcExternalCatalog",
            "IcebergS3TablesExternalCatalog"
    };

    @Test
    public void legacyCatalogClazzNamesDeserializeAsPluginDriven() {
        for (String legacyClazz : LEGACY_CATALOG_SIMPLE_NAMES) {
            String json = legacyCatalogJson(legacyClazz);
            CatalogIf<?> back = GsonUtils.GSON.fromJson(json, CatalogIf.class);
            Assertions.assertNotNull(back, "deserialization returned null for " + legacyClazz);
            Assertions.assertTrue(back instanceof PluginDrivenExternalCatalog,
                    "legacy clazz '" + legacyClazz
                            + "' must deserialize as PluginDrivenExternalCatalog, got "
                            + back.getClass().getName());
            PluginDrivenExternalCatalog plugin = (PluginDrivenExternalCatalog) back;
            Assertions.assertEquals(99L, plugin.getId());
            Assertions.assertEquals("ice_compat_" + legacyClazz, plugin.getName());
            Map<String, String> props = plugin.getCatalogProperty().getProperties();
            Assertions.assertEquals("iceberg", props.get(CatalogMgr.CATALOG_TYPE_PROP),
                    "iceberg type property must be preserved through the alias");
            Assertions.assertEquals("rest", props.get("iceberg.catalog.type"));
            Assertions.assertEquals("https://example/glue/warehouse", props.get("warehouse"));
        }
    }

    @Test
    public void legacyDatabaseClazzNameDeserializesAsPluginDriven() {
        String json = "{\"clazz\":\"IcebergExternalDatabase\","
                + "\"id\":11,\"name\":\"db1\",\"dbProperties\":{\"properties\":{}}}";
        DatabaseIf<?> back = GsonUtils.GSON.fromJson(json, DatabaseIf.class);
        Assertions.assertNotNull(back);
        Assertions.assertTrue(back instanceof PluginDrivenExternalDatabase,
                "IcebergExternalDatabase must deserialize as PluginDrivenExternalDatabase, got "
                        + back.getClass().getName());
    }

    @Test
    public void legacyTableClazzNameDeserializesAsPluginDriven() {
        String json = "{\"clazz\":\"IcebergExternalTable\","
                + "\"id\":21,\"name\":\"tbl1\","
                + "\"dbName\":\"db1\","
                + "\"type\":\"ICEBERG_EXTERNAL_TABLE\","
                + "\"fullSchema\":[]}";
        org.apache.doris.catalog.TableIf back = GsonUtils.GSON.fromJson(
                json, org.apache.doris.catalog.TableIf.class);
        Assertions.assertNotNull(back);
        Assertions.assertTrue(back instanceof PluginDrivenExternalTable,
                "IcebergExternalTable must deserialize as PluginDrivenExternalTable, got "
                        + back.getClass().getName());
    }

    /**
     * Round-trip: a freshly constructed PluginDrivenExternalCatalog with iceberg
     * properties survives serialize → deserialize and keeps its properties.
     */
    @Test
    public void pluginDrivenIcebergCatalogRoundTripsItsProperties() {
        Map<String, String> props = new HashMap<>();
        props.put(CatalogMgr.CATALOG_TYPE_PROP, "iceberg");
        props.put("iceberg.catalog.type", "rest");
        props.put("uri", "http://example/rest");
        props.put("warehouse", "s3://b/wh");

        PluginDrivenExternalCatalog original = new PluginDrivenExternalCatalog(
                42L, "ice_round_trip", null, props, "round-trip", null);

        String json = GsonUtils.GSON.toJson(original, CatalogIf.class);
        Assertions.assertTrue(json.contains("\"clazz\":\"PluginDrivenExternalCatalog\""),
                "newly serialized iceberg catalog must use the PluginDriven discriminator: " + json);

        CatalogIf<?> back = GsonUtils.GSON.fromJson(json, CatalogIf.class);
        Assertions.assertTrue(back instanceof PluginDrivenExternalCatalog);
        PluginDrivenExternalCatalog restored = (PluginDrivenExternalCatalog) back;
        Assertions.assertEquals(42L, restored.getId());
        Assertions.assertEquals("ice_round_trip", restored.getName());
        Map<String, String> restoredProps = restored.getCatalogProperty().getProperties();
        Assertions.assertEquals("iceberg", restoredProps.get(CatalogMgr.CATALOG_TYPE_PROP));
        Assertions.assertEquals("rest", restoredProps.get("iceberg.catalog.type"));
        Assertions.assertEquals("http://example/rest", restoredProps.get("uri"));
        Assertions.assertEquals("s3://b/wh", restoredProps.get("warehouse"));
    }

    private static String legacyCatalogJson(String legacyClazz) {
        // Mimics the shape of pre-M3-15 image entries: a CatalogIf subtype with
        // discriminator "clazz" plus the canonical ExternalCatalog fields.
        return "{"
                + "\"clazz\":\"" + legacyClazz + "\","
                + "\"id\":99,"
                + "\"name\":\"ice_compat_" + legacyClazz + "\","
                + "\"type\":\"PLUGIN\","
                + "\"comment\":\"\","
                + "\"catalogProperty\":{\"properties\":{"
                + "\"type\":\"iceberg\","
                + "\"iceberg.catalog.type\":\"rest\","
                + "\"warehouse\":\"https://example/glue/warehouse\""
                + "}}"
                + "}";
    }
}
