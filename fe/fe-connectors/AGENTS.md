# AGENTS.md — Doris FE Connector Plugins

This directory contains connector plugins for Apache Doris external data sources. Each connector is an independent Maven module that implements the `CatalogProvider` SPI interface, enabling Doris to dynamically discover and load external data source support.

## Architecture Overview

```
fe/fe-core/src/.../datasource/spi/
├── CatalogProvider.java           # SPI interface (all connectors implement this)
├── CatalogProviderRegistry.java   # In-memory registry of loaded providers
└── CatalogPluginLoader.java       # Scans lib/connectors/ and loads JAR plugins

fe/fe-connectors/
├── AGENTS.md                      # This guide
└── connector-es/                  # Reference implementation (Elasticsearch)
    ├── pom.xml
    └── src/main/java/.../es/
        ├── EsCatalogProvider.java  # implements CatalogProvider
        ├── EsExternalCatalog.java  # extends ExternalCatalog
        ├── EsExternalDatabase.java
        ├── EsExternalTable.java
        ├── source/EsScanNode.java  # extends ScanNode
        └── ...
```

### Runtime Loading Flow

1. FE startup → `Env.java` calls `CatalogPluginLoader.loadPlugins()`
2. `CatalogPluginLoader` scans `${DORIS_HOME}/lib/connectors/` subdirectories
3. Each subdirectory's JARs are loaded via an isolated `URLClassLoader`
4. `ServiceLoader<CatalogProvider>` discovers provider implementations
5. Providers are registered in `CatalogProviderRegistry`
6. `CatalogFactory.createCatalog()` checks the registry before falling back to the hardcoded switch-case

## Creating a New Connector Plugin

### Step 1: Create Module Directory

```bash
mkdir -p fe/fe-connectors/connector-{name}/src/main/java/org/apache/doris/datasource/{name}
mkdir -p fe/fe-connectors/connector-{name}/src/main/java/org/apache/doris/datasource/{name}/source
mkdir -p fe/fe-connectors/connector-{name}/src/main/resources/META-INF/services
```

Replace `{name}` with the connector name (e.g., `jdbc`, `iceberg`, `paimon`).

### Step 2: Create `pom.xml`

Copy and adapt from `connector-es/pom.xml`. Key points:

```xml
<parent>
    <groupId>org.apache.doris</groupId>
    <version>${revision}</version>
    <artifactId>fe</artifactId>
    <relativePath>../../pom.xml</relativePath>
</parent>
<artifactId>connector-{name}</artifactId>
<name>Doris FE Connector Plugin - {Name}</name>

<dependencies>
    <!-- fe-core: provided scope (available at runtime, not packaged) -->
    <dependency>
        <groupId>${project.groupId}</groupId>
        <artifactId>fe-core</artifactId>
        <version>${project.version}</version>
        <scope>provided</scope>
    </dependency>

    <!-- Lombok: provided scope -->
    <dependency>
        <groupId>org.projectlombok</groupId>
        <artifactId>lombok</artifactId>
        <scope>provided</scope>
    </dependency>

    <!-- Connector-specific dependencies (non-provided, will be shaded into fat JAR) -->
    <!-- Example: -->
    <!-- <dependency>
        <groupId>some.third.party</groupId>
        <artifactId>client-lib</artifactId>
        <version>x.y.z</version>
    </dependency> -->
</dependencies>

<build>
    <finalName>doris-connector-{name}</finalName>
    <directory>${project.basedir}/target/</directory>
    <plugins>
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-shade-plugin</artifactId>
            <executions>
                <execution>
                    <phase>package</phase>
                    <goals><goal>shade</goal></goals>
                    <configuration>
                        <artifactSet>
                            <excludes>
                                <exclude>org.apache.doris:fe-core</exclude>
                                <exclude>org.apache.doris:fe-common</exclude>
                                <exclude>org.apache.doris:fe-catalog</exclude>
                            </excludes>
                        </artifactSet>
                    </configuration>
                </execution>
            </executions>
        </plugin>
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-javadoc-plugin</artifactId>
            <configuration><skip>true</skip></configuration>
        </plugin>
    </plugins>
</build>
```

**Important rules:**
- `fe-core` MUST be `provided` scope — it is the parent classloader at runtime
- `lombok` MUST be `provided` scope
- Connector-specific third-party libraries should be non-provided (default scope), so the shade plugin bundles them into the fat JAR
- The shade plugin excludes all Doris modules to avoid duplication

### Step 3: Implement `CatalogProvider`

Create `{Name}CatalogProvider.java` implementing `org.apache.doris.datasource.spi.CatalogProvider`:

```java
package org.apache.doris.datasource.{name};

import org.apache.doris.datasource.spi.CatalogProvider;
// ... other imports

public class {Name}CatalogProvider implements CatalogProvider {

    @Override
    public String getType() {
        // Must match the "type" property in CREATE CATALOG statement
        return "{name}";
    }

    @Override
    public ExternalCatalog createCatalog(long catalogId, String name, String resource,
            Map<String, String> props, String comment) {
        return new {Name}ExternalCatalog(catalogId, name, resource, props, comment);
    }

    @Override
    public void initializeCatalog(ExternalCatalog catalog) {
        // Optional: additional initialization logic
        // Most connectors handle this in ExternalCatalog.initLocalObjectsImpl()
    }

    @Override
    public ExternalDatabase<? extends ExternalTable> createDatabase(ExternalCatalog catalog,
            long dbId, String localDbName, String remoteDbName) {
        return new {Name}ExternalDatabase(catalog, dbId, localDbName, remoteDbName);
    }

    @Override
    public ExternalTable createTable(ExternalCatalog catalog,
            ExternalDatabase<? extends ExternalTable> db,
            long tblId, String localName, String remoteName) {
        return new {Name}ExternalTable(tblId, localName, remoteName,
                ({Name}ExternalCatalog) catalog, ({Name}ExternalDatabase) db);
    }

    @Override
    public ScanNode createScanNode(PlanNodeId id, TupleDescriptor desc,
            ExternalTable table, SessionVariable sv, ScanContext scanContext) {
        return new {Name}ScanNode(id, desc, table, scanContext);
    }

    @Override
    public ExternalMetadataOps createMetadataOps(ExternalCatalog catalog,
            Map<String, String> props) {
        // Return null if DDL is not supported
        return null;
    }

    @Override
    public List<String> listDatabaseNames(ExternalCatalog catalog) {
        return (({Name}ExternalCatalog) catalog).listDatabaseNames();
    }

    @Override
    public List<String> listTableNames(ExternalCatalog catalog,
            SessionContext ctx, String dbName) {
        return (({Name}ExternalCatalog) catalog).listTableNamesFromRemote(ctx, dbName);
    }

    @Override
    public boolean tableExist(ExternalCatalog catalog, SessionContext ctx,
            String dbName, String tblName) {
        return (({Name}ExternalCatalog) catalog).tableExist(ctx, dbName, tblName);
    }
}
```

### Step 4: Register SPI Service

Create the file:
```
src/main/resources/META-INF/services/org.apache.doris.datasource.spi.CatalogProvider
```

Content (must include the Apache license header):
```
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# ...
# under the License.

org.apache.doris.datasource.{name}.{Name}CatalogProvider
```

### Step 5: Implement Connector Classes

At minimum, you need:

| Class | Extends/Implements | Purpose |
|---|---|---|
| `{Name}ExternalCatalog` | `ExternalCatalog` | Catalog metadata, connection management |
| `{Name}ExternalDatabase` | `ExternalDatabase<{Name}ExternalTable>` | Database-level metadata |
| `{Name}ExternalTable` | `ExternalTable` | Table-level metadata, schema |
| `{Name}ScanNode` | `ScanNode` | Query execution plan node |

All source files must use the package `org.apache.doris.datasource.{name}` (or `org.apache.doris.datasource.{name}.source` for ScanNode).

### Step 6: Register in Parent `pom.xml`

Add the module to `fe/pom.xml`:
```xml
<module>fe-connectors/connector-{name}</module>
```

### Step 7: Update `build.sh`

All connectors are built by default. You only need to add `--no-connector-{name}` for users who want to exclude it.

Four changes required in `build.sh`:

1. **Add getopt entries:**
```bash
    -l 'no-connector-{name}' \
```

2. **Add variable (default ON):**
```bash
BUILD_CONNECTOR_{NAME}=1
```

3. **Add case handler for exclusion:**
```bash
--no-connector-{name})
    BUILD_CONNECTOR_{NAME}=0
    shift
    ;;
```

4. **Add module assembly and output copy:**
```bash
# In the FE_MODULES assembly section:
if [[ "${BUILD_CONNECTOR_{NAME}}" -eq 1 ]]; then
    modules+=("fe-connectors/connector-{name}")
fi

# In the FE output copy section:
if [[ "${BUILD_CONNECTOR_{NAME}}" -eq 1 ]]; then
    mkdir -p "${DORIS_OUTPUT}/fe/lib/connectors/{name}/"
    cp -r -p "${DORIS_HOME}/fe/fe-connectors/connector-{name}/target/doris-connector-{name}.jar" \
        "${DORIS_OUTPUT}/fe/lib/connectors/{name}/"
fi
```

### Step 8: Update `GsonUtils.java` for Backward Compatibility

If migrating an existing connector, add a `registerCompatibleSubtype` entry in `GsonUtils.java` so that old EditLog entries with the original class name can be deserialized as `ExternalCatalog`:

```java
// {Name}: old EditLog "{Name}ExternalCatalog" maps to ExternalCatalog
.registerCompatibleSubtype(ExternalCatalog.class, {Name}ExternalCatalog.class.getSimpleName())
```

This ensures that metadata written before the migration can still be loaded.

## Migrating an Existing Connector from fe-core

When moving an existing data source from `fe-core` to a connector plugin:

1. **Move source files:** `git mv` from `fe-core/src/.../datasource/{name}/` to `fe-connectors/connector-{name}/src/.../datasource/{name}/`
2. **Create `{Name}CatalogProvider`** implementing `CatalogProvider`
3. **Update `ExternalCatalog.java`:** The SPI-delegating default implementations of `initLocalObjectsImpl()`, `listTableNamesFromRemote()`, `tableExist()` will automatically route to the provider
4. **Update `PhysicalPlanTranslator.java`:** The `createScanNode` already tries SPI first via `CatalogProviderRegistry`; remove the direct `instanceof` check for the migrated type if present
5. **Update `GsonUtils.java`:** Add `registerCompatibleSubtype` for backward compatibility
6. **Keep the fallback switch-case in `CatalogFactory.java`** for backward compatibility during the transition period; it can be removed after the connector plugin is stable
7. **Follow all steps in "Creating a New Connector Plugin"** (pom.xml, SPI service file, build.sh, parent pom)

## Build and Test

```bash
# Full build (all connectors included by default)
./build.sh --fe --be -j8

# Build everything except a specific connector
./build.sh --fe --be --no-connector-{name} -j8
```

Output artifacts:
```
output/fe/lib/connectors/{name}/doris-connector-{name}.jar
```

## Checklist for New Connector PR

- [ ] `fe/fe-connectors/connector-{name}/pom.xml` created with correct parent, dependencies (`fe-core` provided, `lombok` provided), shade plugin
- [ ] `{Name}CatalogProvider` implements all `CatalogProvider` methods
- [ ] `META-INF/services/org.apache.doris.datasource.spi.CatalogProvider` file created with license header
- [ ] `{Name}ExternalCatalog`, `{Name}ExternalDatabase`, `{Name}ExternalTable`, `{Name}ScanNode` implemented
- [ ] `fe/pom.xml` updated with new module
- [ ] `build.sh` updated: getopt, variable, case handler, module assembly, output copy
- [ ] `GsonUtils.java` updated with `registerCompatibleSubtype` (if migrating existing connector)
- [ ] All source files have Apache 2.0 license headers
- [ ] All resource files have Apache 2.0 license headers (use `#` comment style)
- [ ] Import order is lexicographical (CI enforces this via checkstyle)
