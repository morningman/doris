# fe-connector — Developer Handbook

Reference manual for authors and committers of Doris connector plugins
loaded by the FE through the new connector SPI. This document describes
the public API surface added under `fe/fe-connector/fe-connector-api/` and
`fe/fe-connector/fe-connector-spi/` and the rules a connector implementation
must follow to be loaded by fe-core.

The handbook is the source of truth for migrating an existing data source
onto the SPI or building a new one. Every public type listed here is part of
the supported contract; anything not listed is internal and may change
without notice.

---

## 1. Overview

`fe-connector` is the FE-side plugin surface that lets a data-source
integration (Hive, Iceberg, JDBC, Elasticsearch, MaxCompute, …) participate
in Doris query planning, metadata, governance, and DML without depending on
any internal `fe-core` class. fe-core loads connector plugins via the JDK
ServiceLoader mechanism, hands each one a `ConnectorContext` for engine
services, and consumes the plugin through the narrow contracts defined in
this module.

The module is split into two artifacts:

| Module | Purpose | May depend on |
| --- | --- | --- |
| `fe-connector-api` | Pure data + contract types. No dependency on fe-core, fe-common, fe-catalog, or `fe-connector-spi`. Plugins implement these and the engine consumes them. | only `fe-common-thrift` and JDK |
| `fe-connector-spi` | Plugin-discovery and engine-supplied runtime services. Depends on `fe-connector-api`. | `fe-connector-api`, `extension-spi` |

The dependency direction is one-way: **api never depends on spi; spi
depends on api**. Engine code (fe-core) is free to depend on both. Plugins
typically depend on both. Two plugins MUST NOT depend on each other.

The contract is bidirectional but asymmetric:

* **Engine → plugin**: through `ConnectorProvider`, `Connector`,
  `ConnectorMetadata`, the various optional `*Ops` accessors, and the
  scan / write contracts under `api.scan` / `api.write`.
* **Plugin → engine**: through the `ConnectorContext` instance handed in
  at `ConnectorProvider.create(...)` time. Plugins never reach into
  fe-core; everything they need (cache registry, credential broker, audit
  publisher, scheduler, registry, …) is exposed as an `Ops` method on
  the context.

## 2. Module layout

```
fe/fe-connector/
├── fe-connector-api/        # data types, sealed events, *Ops contracts
├── fe-connector-spi/        # ConnectorProvider, ConnectorContext, ConnectorRegistry
├── fe-connector-jdbc/       # JDBC reference implementation (most complete read+pushdown)
├── fe-connector-es/         # Elasticsearch reference implementation
├── fe-connector-hms/        # Hive Metastore catalog (host for D10 dispatch)
├── fe-connector-hive/       # Hive (file-format) connector
├── fe-connector-iceberg/    # Iceberg
├── fe-connector-paimon/     # Paimon
├── fe-connector-hudi/       # Hudi
├── fe-connector-trino/      # Trino-protocol passthrough
└── fe-connector-maxcompute/ # MaxCompute
```

`pom.xml` is an aggregator: it does **not** declare dependencies and ships
no Java code. Each connector module pulls in `fe-connector-api` +
`fe-connector-spi` and is loaded independently by the
`DirectoryPluginRuntimeManager`.

## 3. Lifecycle of a connector

When a user issues `CREATE CATALOG name PROPERTIES("type"="jdbc", …)`,
fe-core walks the following sequence:

```text
1. ServiceLoader<ConnectorProvider>  → enumerate META-INF/services
2. provider.supports(type, props)    → match on "type" property
3. provider.validateProperties(props)→ fail-fast on bad config
4. provider.getCatalogTypeProperty() → resolve backend variant (D2)
5. provider.create(props, ctx)       → returns Connector
6. connector.preCreateValidation(vc) → optional pre-flight checks
7. connector.testConnection(session) → if defaultTestConnection() or user opt-in
8. connector.getMetaCacheBindings()  → register with fe-core MetaCacheRegistry
9. connector.getCapabilities()       → cache for planner gating
10. connector.declaredDelegateTypes()→ warn on missing delegate (D10)
```

Per query the engine then calls:

```text
ConnectorMetadata md = connector.getMetadata(session);
ConnectorTableHandle h = md.getTableHandle(session, db, table).get();
ConnectorTableSchema s = md.getTableSchema(session, h);
md.applyFilter(...) / applyProjection(...) / applyLimit(...)   // optional
List<ConnectorScanRange> ranges = connector.getScanPlanProvider()
        .planScan(session, h, columns, filter);
```

Writes follow the two-phase `begin* … finish*/abort*` protocol described
in §6.1. Optional surfaces (events, MTMV, policy, audit, sys-tables,
actions, refs, dispatch) are accessed through the `Optional<*Ops>`
accessors on `ConnectorMetadata` (or on `Connector` for `Dispatch` and
sys-managed caches) and are only invoked when the corresponding
capability is declared.

## 4. Core SPI types

### 4.1 `ConnectorProvider` (spi)

`org.apache.doris.connector.spi.ConnectorProvider`

Plugin factory discovered via `ServiceLoader`. Extends
`org.apache.doris.extension.spi.PluginFactory` so the
`DirectoryPluginRuntimeManager` can load it from a plugin directory.

Required:

* `String getType()` — the lowercase type name matched against the
  `type=` property (e.g. `"jdbc"`, `"hive"`).
* `Connector create(Map<String,String> properties, ConnectorContext context)`
  — called once per catalog.

Optional / has defaults:

* `Optional<String> getCatalogTypeProperty()` — name of the property that
  selects a backend variant (D2). Single-backend providers leave this as
  `Optional.empty()`.
* `Set<String> getSupportedBackends()` — legal values for the property
  above; default `Collections.singleton(getType())`.
* `boolean supports(String catalogType, Map<String,String> props)` —
  default `getType().equalsIgnoreCase(catalogType)`.
* `void validateProperties(Map<String,String> properties)` — throw
  `IllegalArgumentException` for invalid configurations.
* `int apiVersion()` — defaults to `1`. Bump on incompatible change.
* `Optional<String> defaultAccessControllerFactoryName()` — D8: name of
  the engine `AccessControllerFactory` used by default for catalogs of
  this connector type (e.g. `"ranger-hive"`). Defaults to
  `Optional.empty()` so the engine falls back to its built-in controller
  and existing `jdbc` / `es` behaviour does not silently change.
* `String name()` — `PluginFactory` contract; defaults to `getType()`.
* `Plugin create()` — `PluginFactory` no-arg form is intentionally
  unsupported and throws `UnsupportedOperationException`.

### 4.2 `Connector` (api)

`org.apache.doris.connector.api.Connector`

The catalog-scoped runtime instance. Implements `Closeable`. Required
method:

* `ConnectorMetadata getMetadata(ConnectorSession session)`

Defaults that subclasses commonly override:

* `ConnectorScanPlanProvider getScanPlanProvider()` — required for any
  connector that participates in scans (default `null`).
* `Set<ConnectorCapability> getCapabilities()` — capabilities advertised
  to the planner; defaults to empty.
* `List<ConnectorPropertyMetadata<?>> getCatalogProperties()` /
  `getCatalogProperties(Map<String,String> rawProps)` — descriptors for
  catalog-level properties; second overload may return conditional
  descriptors that depend on raw user input (e.g. show
  `iceberg.rest.uri` only when `iceberg.catalog.type=rest`).
* `List<ConnectorPropertyMetadata<?>> getTableProperties()` /
  `getTableProperties(TableScope scope)` — table-level descriptors,
  optionally per `TableScope` (`CREATE` / `ALTER` / `READ`).
* `List<ConnectorPropertyMetadata<?>> getSessionProperties()`.
* `boolean defaultTestConnection()` — return `true` for connectors
  needing connectivity smoke tests at `CREATE CATALOG` time.
* `void preCreateValidation(ConnectorValidationContext context)` — pre-flight
  validation (driver checksum, BE reachability, …).
* `ConnectorTestResult testConnection(ConnectorSession session)`.
* `String executeRestRequest(String path, String body)` — REST passthrough
  for HTTP-backed connectors (e.g. ES); throws `UnsupportedOperationException`
  by default.
* `List<ConnectorMetaCacheBinding<?,?>> getMetaCacheBindings()` — caches
  the engine should manage on the plugin's behalf (D3).
* `void invalidateCache(InvalidateRequest req)` — escape hatch for caches
  the plugin manages itself.
* `List<CacheSnapshot> listSelfManagedCacheStats()` — surface plugin-owned
  cache stats to engine telemetry.
* `ConnectorDispatchOps getDispatchOps()` — D10 dispatch (default `null`).
* `List<String> declaredDelegateTypes()` — D10 capability negotiation;
  fe-core warns at startup if a declared delegate type is not loaded.

### 4.3 `ConnectorContext` (spi)

`org.apache.doris.connector.spi.ConnectorContext`

Engine services handed to the plugin in `ConnectorProvider.create(...)`.
Plugins MUST cache the reference and use it for any cross-cutting need
instead of reaching into fe-core directly. Defaults are conservative —
most accessors throw `UnsupportedOperationException` until the
corresponding milestone wires them in.

| Method | Purpose | Default |
| --- | --- | --- |
| `String getCatalogName()` | engine-side catalog name | required |
| `long getCatalogId()` | persistent id | required |
| `Map<String,String> getEnvironment()` | system-level env (`doris_home`, `jdbc_drivers_dir`) | empty |
| `ConnectorHttpSecurityHook getHttpSecurityHook()` | SSRF gate; call before/after every outbound HTTP | `NOOP` |
| `String sanitizeJdbcUrl(String url)` | engine URL allow-list; **MUST** be called before opening a JDBC connection | identity |
| `<T> T executeAuthenticated(Callable<T>)` | UGI.doAs wrapper. **Deprecated** — replaced by `getCredentialBroker().impersonation().runAs(...)`. Remove callers before M5 | direct call |
| `<K,V> MetaCacheHandle<K,V> getOrCreateCache(ConnectorMetaCacheBinding<K,V>)` | obtain engine-managed cache | throws |
| `void invalidate(InvalidateRequest req)` | propagate invalidation | no-op |
| `void invalidateAll()` | nuclear invalidate for this catalog | no-op |
| `CredentialBroker getCredentialBroker()` | unified credential surface (D4) | throws |
| `void registerResolver(CredentialResolver r)` | extend resolver chain | no-op |
| `MasterOnlyScheduler getMasterOnlyScheduler()` | D7 self-managed event loop scheduler | throws |
| `void publishExternalEvent(ConnectorMetaChangeEvent e)` | D7 self-managed publisher | no-op |
| `void publishAuditEvent(ConnectorAuditEvent e)` | D8 audit emit; events flow through engine `AuditEventChainListener` into the standard `AuditEventProcessor` (no parallel sink) | no-op |
| `ConnectorRegistry getConnectorRegistry()` | D10 host → delegate lookup | throws |

### 4.4 `ConnectorMetadata` (api)

`org.apache.doris.connector.api.ConnectorMetadata`

Central session-scoped metadata interface. It composes the fine-grained
per-feature interfaces plus optional `*Ops` accessors. Every method has
a default that returns empty / sentinel and never throws — this allows
the SPI to evolve without breaking existing connectors.

Composed interfaces (all under `o.a.d.connector.api`):

* `ConnectorSchemaOps` — `listDatabaseNames`, `databaseExists`,
  `getDatabase`, `createDatabase`, `dropDatabase`.
* `ConnectorTableOps` — `getTableHandle`, `getTableSchema`,
  `getColumnHandles`, `listTableNames`, `createTable`, `dropTable`,
  `getPrimaryKeys`, `getTableComment`, `executeStmt`,
  `getColumnsFromQuery` (for `SUPPORTS_PASSTHROUGH_QUERY`),
  `buildTableDescriptor` (Thrift `TTableDescriptor` for BE).
* `ConnectorPushdownOps` — `applyFilter`, `applyProjection`, `applyLimit`,
  `supportsCastPredicatePushdown`.
* `ConnectorStatisticsOps` — `getTableStatistics`.
* `ConnectorWriteOps` — see §6.1.
* `ConnectorIdentifierOps` — `fromRemote{Database,Table,Column}Name` for
  case-folding / quoting normalization.
* `SystemTableOps` — see §6.6.

Optional accessors (return `Optional.empty()` / `EventSourceOps.NONE`
unless overridden):

* `Optional<RefOps> refOps()`
* `EventSourceOps getEventSourceOps()`
* `Optional<ConnectorActionOps> actionOps()`
* `Optional<MtmvOps> mtmvOps()`
* `Optional<PolicyOps> policyOps()`
* `Optional<ConnectorAuditOps> auditOps()`
* `Map<String,String> getProperties()` — connector-level properties.

## 5. Capability model

`org.apache.doris.connector.api.ConnectorCapability` is the single
authoritative enum the engine consults to gate optional features. The
enum is **frozen at 47 ordinals as of M0** and is **append-only**:

* New capabilities are added by appending entries; never reorder, never
  remove.
* The ordinal participates in serialization and Thrift mapping; renaming
  is allowed at the Java level but reordering is not.
* The full freeze list is the M0 baseline used by D2/D6/D7/D8/D10
  reservation (see PR commits tagged `M0-01` for the cut).

Plugins declare capabilities by overriding
`Connector.getCapabilities()`; the planner queries them through
`ConnectorRegistry.supports(type, cap)` (D10) or via the loaded
`Connector` instance directly. A capability is **necessary but not
sufficient** — the matching `*Ops` implementation must also be present.

Capability domains:

| Domain | Capabilities |
| --- | --- |
| Read pushdown | `SUPPORTS_FILTER_PUSHDOWN`, `SUPPORTS_PROJECTION_PUSHDOWN`, `SUPPORTS_LIMIT_PUSHDOWN`, `SUPPORTS_PARTITION_PRUNING` |
| Statistics / passthrough | `SUPPORTS_STATISTICS`, `SUPPORTS_PASSTHROUGH_QUERY` |
| Snapshot / time travel | `SUPPORTS_MVCC_SNAPSHOT`, `SUPPORTS_TIME_TRAVEL`, `SUPPORTS_BRANCH_TAG`, `SUPPORTS_TIME_TRAVEL_WRITE` |
| Vended credentials | `SUPPORTS_VENDED_CREDENTIALS` |
| Write — basic | `SUPPORTS_INSERT`, `SUPPORTS_DELETE`, `SUPPORTS_UPDATE`, `SUPPORTS_MERGE`, `SUPPORTS_CREATE_TABLE`, `SUPPORTS_PARALLEL_WRITE` |
| Write — D1 extensions | `SUPPORTS_INSERT_OVERWRITE`, `SUPPORTS_PARTITION_OVERWRITE`, `SUPPORTS_DYNAMIC_PARTITION_INSERT`, `SUPPORTS_TXN_INSERT`, `SUPPORTS_UPSERT`, `SUPPORTS_ROW_LEVEL_DELETE`, `SUPPORTS_POSITION_DELETE`, `SUPPORTS_EQUALITY_DELETE`, `SUPPORTS_DELETION_VECTOR`, `SUPPORTS_MERGE_INTO`, `SUPPORTS_PROCEDURES`, `SUPPORTS_FAILOVER_SAFE_TXN`, `SUPPORTS_ACID_TRANSACTIONS` |
| System tables (D6) | `SUPPORTS_SYSTEM_TABLES`, `SUPPORTS_NATIVE_SYS_TABLES`, `SUPPORTS_TVF_SYS_TABLES` |
| Events (D7) | `SUPPORTS_METASTORE_EVENTS`, `SUPPORTS_PULL_EVENTS`, `SUPPORTS_PUSH_EVENTS`, `REQUIRES_SELF_MANAGED_EVENT_LOOP`, `SUPPORTS_PER_TABLE_FILTER`, `EMITS_REF_EVENTS`, `EMITS_DATA_CHANGED_WITH_SNAPSHOT` |
| Governance (D8) | `SUPPORTS_MTMV`, `SUPPORTS_RLS_HINT`, `SUPPORTS_MASK_HINT`, `EMITS_AUDIT_EVENTS` |
| Dispatch (D10) | `EMITS_DELEGATABLE_TABLES`, `ACCEPTS_DELEGATION_FROM_HMS`, `SUPPORTS_DELEGATING_LISTING` |

## 6. Domain interfaces

Each subsection covers a design domain (Dx). For every interface:
purpose, when a connector needs it, the key types / methods, the
default behaviour on `ConnectorMetadata`, integration touchpoints, and
a minimal example.

### 6.1 D1 — Write path & actions

#### `ConnectorWriteOps`

Two-phase write contract on `ConnectorMetadata`. All methods default to
`throw new DorisConnectorException("… not supported")` so unimplemented
operations fail fast with a clear message.

Capability queries: `supportsInsert()`, `supportsDelete()`,
`supportsMerge()`.

Configuration: `ConnectorWriteConfig getWriteConfig(session, handle, columns)`
returns the sink type, file format, compression, target location,
partition columns, static partition values, and connector-specific
properties forwarded to BE.

Per-statement lifecycle:

```java
ConnectorInsertHandle h = md.beginInsert(session, table, columns, intent);
//  … BE executes; collects byte[] commit fragments
md.finishInsert(session, h, fragments);   // or md.abortInsert(session, h)
```

Same shape for `beginDelete` / `finishDelete` / `abortDelete` and
`beginMerge` / `finishMerge` / `abortMerge`. The `WriteIntent`-aware
overload of `beginInsert` is **mandatory** for any connector that
declares `SUPPORTS_INSERT_OVERWRITE`, `SUPPORTS_UPSERT`,
`SUPPORTS_PARTITION_OVERWRITE`, `SUPPORTS_DYNAMIC_PARTITION_INSERT`,
`SUPPORTS_TXN_INSERT`, `SUPPORTS_TIME_TRAVEL_WRITE`, or
`SUPPORTS_BRANCH_TAG`; the legacy 3-arg overload preserves v1
behaviour.

#### `WriteIntent`

Immutable description of high-level write semantics:

* `OverwriteMode overwriteMode()` — `NONE`, `WHOLE_TABLE`, `PARTITIONS`.
* `boolean isUpsert()` — primary-key merge.
* `Optional<String> branch()` — D5 named branch.
* `Map<String,String> staticPartitions()` — `INSERT INTO p=…` literals.
* `DeleteMode deleteMode()` — `COPY_ON_WRITE`, `MERGE_ON_READ`,
  `POSITION`, `EQUALITY`, `DELETION_VECTOR`, `NONE`.
* `Optional<ConnectorTableVersion> writeAtVersion()` — time-travel write.

Build via `WriteIntent.simple()` for the common case, or
`WriteIntent.builder()` for the rest.

#### `ConnectorTransactionContext` and `Savepoint`

Both are `Serializable` marker interfaces under `api.write`. The engine
treats instances as opaque blobs persisted with the FE transaction
record and shipped to BE workers verbatim. Plugins extend these with
their own concrete implementations (e.g. an Iceberg snapshot id +
write-uuid). `ConnectorTxnCapability` enumerates which optional
behaviours are supported (declared via
`ConnectorWriteOps.txnCapabilities()`).

#### `RetryableCommitException`

Throw from `finishInsert/Delete/Merge` to ask the engine to retry the
commit (e.g. Iceberg snapshot conflict). Carries an optional
`Duration suggestedBackoff()`.

#### `CommitOutcome`

Returned by transactional commit paths once D1's commit phase lands —
exposes `Optional<ConnectorTableVersion> newVersion()`, written rows,
and written bytes for engine telemetry / audit.

#### `ConnectorUpdateHandle`

Marker interface in `api.handle` reserved for the row-level UPDATE flow
that pairs with `SUPPORTS_UPSERT` / `SUPPORTS_ROW_LEVEL_DELETE`. The
engine wires it once the UPDATE statement plumbing lands; today it has
no methods so plugins can extend it freely.

#### `ConnectorActionOps` + `ActionDescriptor` / `ActionInvocation` / `ActionResult`

Optional procedure / action surface (D1, capability
`SUPPORTS_PROCEDURES`). Actions are scoped at `CATALOG`, `SCHEMA`, or
`TABLE` (the `Scope` enum). Connectors implement:

```java
List<ActionDescriptor> listActions(Scope scope, Optional<ActionTarget> target);
ActionResult executeAction(ActionInvocation invocation);
```

`ActionDescriptor` carries name, scope, declared `ActionArgument`s
(name + `ActionArgumentType` + required + default), result schema
(`ResultColumn(name, type)`), and lifecycle flags (`transactional`,
`blocking`, `idempotent`). `ActionResult` is one of `COMPLETED`,
`STARTED_ASYNC` (returns a poll handle id), or `FAILED`. Use
`ActionResult.completed(rows)`, `completedEmpty()`, `startedAsync(id)`,
`failed(msg)` factories.

### 6.2 D2 — Multi-backend providers

D2 is implemented entirely on `ConnectorProvider`. Two methods drive the
flow:

* `Optional<String> getCatalogTypeProperty()` — name of the user-facing
  property whose value selects the backend (`iceberg.catalog.type`,
  `paimon.catalog.type`, …).
* `Set<String> getSupportedBackends()` — legal values, used to:
  (a) populate the error message when an unknown backend is requested,
  (b) reject `CREATE CATALOG` before instantiating the plugin.

Single-backend connectors (jdbc, es, mc, trino, hms, hive, hudi) leave
both methods at their defaults; the contract still holds because the
default `getSupportedBackends()` returns `{getType()}`.

The engine's resolver looks up the value of the property returned by
`getCatalogTypeProperty()` in the raw property map and routes
`create(...)` to the correct backend implementation. The chosen
property name **must** be declared by `Connector.getCatalogProperties()`
so it is visible in `SHOW CREATE CATALOG` and validated.

### 6.3 D3 — Metadata cache

All cache types live in `org.apache.doris.connector.api.cache`. Plugins
DO NOT instantiate caches directly — they declare bindings and let
fe-core's `ConnectorMetaCacheRegistry` (skeleton landed in M0-07) own
the caffeine instance.

#### `ConnectorMetaCacheBinding<K,V>`

Immutable description of a cache built via
`ConnectorMetaCacheBinding.builder("entry-name", keyType, valueType)`.
Carries:

* `CacheLoader<K,V>` — the load function.
* `ConnectorCacheSpec defaultSpec()` — `maxSize`, `ttl`, `refreshAfter`,
  soft values, `RefreshPolicy` (`NONE`, `WRITE`, `ACCESS`).
* `ConnectorMetaCacheInvalidation invalidationStrategy()` — predicate
  on `InvalidateRequest`. Helpers: `always()`, `byScope(scope)`.
* Optional `Weigher<K,V>` and `RemovalListener<K,V>` (with the
  `Cause` enum).

Connectors return their bindings from
`Connector.getMetaCacheBindings()` and obtain a live handle through
`ConnectorContext.getOrCreateCache(binding)`.

#### `MetaCacheHandle<K,V>`

Live cache surface: `getIfPresent`, `get`, `put`, `invalidate(K)`,
`invalidateAll()`, `stats()` returning `MetaCacheHandle.CacheStats`.

#### Invalidation

`InvalidateRequest` is built via the static factories:

* `ofCatalog()`, `ofDatabase(db)`, `ofTable(db, t)`,
  `ofPartitions(db, t, partitionKeys)`, `ofSysTable(db, t, sysName)`.

`InvalidateScope` is the matching enum (`CATALOG`, `DATABASE`, `TABLE`,
`PARTITIONS`, `SYS_TABLE`). The engine forwards an `InvalidateRequest`
to every binding whose `invalidationStrategy().appliesTo(req)` returns
true; plugins with caches outside the registry can also receive the
request through `Connector.invalidateCache(req)`.

`CacheSnapshot` is a `(bindingName, CacheStats)` pair for telemetry,
returned from `Connector.listSelfManagedCacheStats()` for
plugin-managed caches.

### 6.4 D4 — Credentials & auth

Types under `org.apache.doris.connector.api.credential`. Wired by
fe-core in M3 — `ConnectorContext.getCredentialBroker()` currently
throws by default and plugins MUST handle that gracefully (e.g. fall
back to legacy property-driven credentials) for forward compatibility.

#### `CredentialBroker`

Single facade exposing the four resolver groups plus runtime
impersonation:

```java
StorageCredentialOps   storage();      // S3, OSS, HDFS, …
MetastoreCredentialOps metastore();    // HMS principal / token resolution
JdbcCredentialOps      jdbc();         // username/password/secret rotation
HttpCredentialOps      http();         // request signing, token vending
RuntimeImpersonationOps impersonation();
```

Each resolver returns a `CredentialEnvelope { type, payload, scope,
expiresAt, refreshHint }` annotated with a `CredentialScope`
(`CATALOG`, `DATABASE`, `TABLE`, `PATH`).

Helper request types: `StorageRequest`, `MetastorePrincipal`,
`JdbcRequest`, `HttpEndpoint`, `HttpRequestSpec`, `StoragePath`,
`UserContext`. All immutable, builder-driven.

`StorageCredentialOps` notably exposes `Mode` (`SHARED` /
`PER_QUERY`), bulk `resolveAll(...)` for split-time credential
vending, and `toBackendProperties(env)` to translate envelopes into
flat key-value maps shipped to BE.

`BeDispatchableCredential` is the marker for credentials that the BE
side needs to receive over Thrift; `beType()` MUST return the BE-side
enum name and `serialize()` MUST emit a stable property map.

`CredentialResolver` is the pluggable chain entry registered via
`ConnectorContext.registerResolver(...)`. `ThrowingSupplier<T>` is the
small functional helper used by impersonation `runAs(...)` overloads.

### 6.5 D5 — Time travel

Types in `org.apache.doris.connector.api.timetravel`.

#### `ConnectorTableVersion`

Sealed interface with five permitted records. The engine produces one
of them from SQL `FOR { VERSION | TIMESTAMP | BRANCH | TAG } AS OF`
clauses:

* `BySnapshotId(long snapshotId)` — Iceberg snapshot id.
* `ByTimestamp(Instant ts)` — latest snapshot at or before `ts`.
* `ByRef(String name, RefKind kind)` — head of a branch / tag.
* `ByRefAtTimestamp(String name, RefKind kind, Instant ts)` — ref then
  timestamp.
* `ByOpaque(String token)` — plugin-specific token (e.g. Hudi instant).

#### `RefOps`

Implemented by connectors that declare `SUPPORTS_BRANCH_TAG`. Returned
from `ConnectorMetadata.refOps()`. Methods:

```java
Set<RefKind> supportedRefKinds();
List<ConnectorRef> listRefs(String database, String table);
void createOrReplaceRef(String database, String table,
                        ConnectorRefMutation mutation);
void dropRef(String database, String table, String name, RefKind kind);
```

Where `ConnectorRef` is the full ref descriptor (`name`, `kind`,
`snapshotId`, `createdAt`), `ConnectorRefMutation` is the
create/replace request, and `ConnectorRefSpec` describes the desired
target state for read-side resolution.

#### `RefKind`

`org.apache.doris.connector.api.timetravel.RefKind` has three values:
`BRANCH`, `TAG`, `UNKNOWN`. The intentionally distinct enum
`org.apache.doris.connector.api.event.ConnectorMetaChangeEvent.RefKind`
(`BRANCH`, `TAG`, `SNAPSHOT_REF`) lives in the event surface; mapping
between the two is an engine concern. **Do not import the wrong one.**

#### `ConnectorMvccSnapshot`

Plugin-supplied snapshot pin used for the duration of a query (see
`SUPPORTS_MVCC_SNAPSHOT`). Methods:

```java
Instant commitTime();
Optional<ConnectorTableVersion> asVersion();
String  toOpaqueToken();
```

The engine treats the instance as opaque; cross-FE/BE serialization
goes through the plugin-supplied nested `Codec` interface
(`encode(snapshot)` / `decode(bytes)`).

### 6.6 D6 — System tables

Types in `org.apache.doris.connector.api.systable`. The umbrella
capability is `SUPPORTS_SYSTEM_TABLES`; plugins additionally declare
`SUPPORTS_NATIVE_SYS_TABLES` and/or `SUPPORTS_TVF_SYS_TABLES` to
indicate the carriers they populate.

#### `SystemTableOps`

Mixed into `ConnectorMetadata`. All methods have empty defaults:

```java
List<SysTableSpec>     listSysTables(String db, String table);
Optional<SysTableSpec> getSysTable(String db, String table, String sysName);
boolean                supportsSysTable(String db, String table, String sysName);
```

`listSysTables` MUST be cheap and side-effect free — it is invoked
during planning and metadata discovery; plugins MUST NOT hit the
metastore inside it.

#### `SysTableSpec`

Immutable description of one sys table: `name`, `ConnectorTableSchema schema`,
`SysTableExecutionMode mode`, `boolean dataTable`, `RefreshPolicy
refreshPolicy`, `boolean acceptsTableVersion`, plus exactly one of:

* `Optional<NativeSysTableScanFactory> nativeFactory()` for `NATIVE`
  mode — plugin builds a `ConnectorScanPlanProvider` for the sys-table
  scan (allowed to reuse the parent table's cache).
* `Optional<TvfSysTableInvoker> tvfInvoker()` for `TVF` mode — plugin
  resolves the sys table to a `TvfInvocation(functionName, properties)`
  that fe-core routes through its existing metadata-TVF machinery.
* `Optional<ConnectorScanPlanProviderRef> scanPlanProviderRef()` for
  `CONNECTOR_SCAN_PLAN` mode — opaque id that fe-core resolves back to
  a provider. Build via `ConnectorScanPlanProviderRef.of(id)` or
  `of(uuid)`.

`SysTableExecutionMode`: `NATIVE`, `TVF`, `CONNECTOR_SCAN_PLAN`.

When `acceptsTableVersion()` is true, fe-core passes the active
`ConnectorTableVersion` into the factory; otherwise it passes
`Optional.empty()`. Sys-table identities use the `(database, table,
sysName)` triple — there is no `ConnectorTableId` yet.

### 6.7 D7 — Events

Types in `org.apache.doris.connector.api.event`. Wired by fe-core in
M2.

#### `EventSourceOps`

Returned from `ConnectorMetadata.getEventSourceOps()`. Default sentinel
`EventSourceOps.NONE` (a `NoOpEventSourceOps` instance — the class is
package-private; reach it only through `NONE`).

```java
Optional<EventCursor> initialCursor();
EventBatch poll(EventCursor cursor, int maxEvents,
                Duration timeout, EventFilter filter)
        throws EventSourceException;
EventCursor parseCursor(byte[] persisted);
byte[]      serializeCursor(EventCursor cursor);
default boolean isSelfManaged()      { return false; }
default void    shutdownSelfManaged(Duration grace) {}
```

* Pull-based plugins implement `poll(...)` and let the engine
  dispatcher drive cadence (capability `SUPPORTS_PULL_EVENTS`).
* Push-based / self-managed plugins set `isSelfManaged() = true`,
  acquire a `MasterOnlyScheduler` from the context, and call
  `ConnectorContext.publishExternalEvent(event)` directly
  (capabilities `SUPPORTS_PUSH_EVENTS`,
  `REQUIRES_SELF_MANAGED_EVENT_LOOP`). The scheduler exposes
  `scheduleAtFixedRate(task, initialDelay, period, taskName)` and
  `shutdown(grace)`.

#### `ConnectorMetaChangeEvent`

Sealed event hierarchy with **13 record permits**, all common fields
are `eventId`, `eventTime`, `catalog`, plus optional
`database` / `table` / `partitionSpec` and a free-form `cause`:

`DatabaseCreated`, `DatabaseDropped`, `DatabaseAltered`, `TableCreated`,
`TableDropped`, `TableAltered`, `TableRenamed`, `PartitionAdded`,
`PartitionDropped`, `PartitionAltered`, `DataChanged`, `RefChanged`,
`VendorEvent` (escape hatch carrying a free-form payload map).

`PartitionSpec(LinkedHashMap<String,String> values)` and
`TableIdentifier(database, table)` are the small value types referenced
by the records.

#### `EventBatch` / `EventCursor` / `EventFilter`

* `EventBatch(events, nextCursor, hasMore, recommendedNextPollDelay)` —
  build via `EventBatch.builder()` or `EventBatch.empty(cursor)`.
* `EventCursor extends Comparable<EventCursor>` — opaque, plugin-defined.
* `EventFilter(databases, tables, catchAll)` with
  `EventFilter.ALL` constant and a builder. `catchAll=true` means
  emit even un-matched events (e.g. for ref/MVCC bookkeeping).

`EventSourceException` is the checked exception thrown from `poll`.

#### Notes

* The `EMITS_REF_EVENTS` and `EMITS_DATA_CHANGED_WITH_SNAPSHOT`
  capabilities let the planner skip cache-invalidation paths it knows
  the plugin will not exercise.
* `SUPPORTS_PER_TABLE_FILTER` lets the engine narrow the
  `EventFilter` it passes; plugins without this capability must accept
  `EventFilter.ALL` and filter client-side.

### 6.8 D8 — Governance (MTMV / Policy / Audit)

Three independent surfaces; each gated by its own capability and
exposed through an `Optional<…Ops>` accessor on `ConnectorMetadata`.

#### MTMV

`org.apache.doris.connector.api.mtmv.MtmvOps` — capability
`SUPPORTS_MTMV`. Used by Doris materialized-views built on external
tables to compute partition mappings and freshness:

```java
Map<String, ConnectorPartitionItem> listPartitions(...);
ConnectorPartitionType getPartitionType(String db, String table);
Set<String>           getPartitionColumnNames(String db, String table);
List<ConnectorColumn> getPartitionColumns(String db, String table);
ConnectorMtmvSnapshot getPartitionSnapshot(String db, String table,
                                           String partitionName);
ConnectorMtmvSnapshot getTableSnapshot(String db, String table);
long    getNewestUpdateVersionOrTime(String db, String table);
boolean isPartitionColumnAllowNull(String db, String table);
boolean isValidRelatedTable(String db, String table);
default boolean needAutoRefresh(String db, String table) { … }
```

`ConnectorPartitionItem` is sealed with three permits:
`RangePartitionItem(lower, upper)`, `ListPartitionItem(values)`,
`UnpartitionedItem()`. `ConnectorPartitionType` enumerates the
partitioning shape (`UNPARTITIONED`, `RANGE`, `LIST`).
`ConnectorMtmvSnapshot` is sealed with four permits:
`VersionMtmvSnapshot(version)`, `TimestampMtmvSnapshot(epochMillis)`,
`MaxTimestampMtmvSnapshot(epochMillis)`,
`SnapshotIdMtmvSnapshot(snapshotId)`. `MtmvRefreshHint(mode,
partitionScope)` carries refresh advice with `RefreshMode` enum.

#### Policy

`org.apache.doris.connector.api.policy.PolicyOps` — capabilities
`SUPPORTS_RLS_HINT` and/or `SUPPORTS_MASK_HINT`. Strictly hint-only:
the engine's own access-control pipeline remains authoritative.

```java
default boolean supportsRlsAt(String db, String table,
                              ConnectorPolicyContext ctx) { return true; }
Optional<ColumnMaskHint> hintForColumn(String db, String table,
                                       String column, UserContext user);
default void onPolicyChanged(PolicyChangeNotification n) {}
```

`ConnectorPolicyContext` is a record `(catalog, database, table,
requestingUser, queryId)` passed in at plan time.

`ColumnMaskHint(maskKind, maskExpr)` — `MaskKind` enum is `NULLIFY`,
`REDACT`, `EXPRESSION`; convenience factories `nullify()`, `redact()`,
`expression(expr)`.

`PolicyChangeNotification(policyKind, catalog, database, table,
changedAt)` — fired when an engine-managed policy changes; plugins
override `onPolicyChanged` to invalidate plugin-side caches.
`PolicyKind` enum classifies engine policies (`ROW_FILTER`,
`COLUMN_MASK`, …).

Identity is `(database, table)` strings — `ConnectorTableId` is
deferred (M0-15).

#### Audit

`org.apache.doris.connector.api.audit.ConnectorAuditOps` — capability
`EMITS_AUDIT_EVENTS`. Declares which audit kinds the connector may
emit:

```java
Set<AuditEventKind> emittedEventKinds();
enum AuditEventKind { PLAN_COMPLETED, COMMIT_COMPLETED, MTMV_REFRESH, POLICY_EVAL }
```

The actual event flow goes through
`ConnectorContext.publishAuditEvent(ConnectorAuditEvent event)`. The
engine forwards the event through its existing
`AuditEventChainListener` into the standard `AuditEventProcessor`
— **plugins must not open a parallel audit sink**.

`ConnectorAuditEvent` is a sealed interface with four permits, each a
record:

* `PlanCompletedAuditEvent(catalog, eventTimeMillis, queryId, database,
  table, planTimeMillis, scanRangeCount)`
* `CommitCompletedAuditEvent(... commit-specific fields ...)`
* `MtmvRefreshAuditEvent(... mtmv-specific ...)`
* `PolicyEvalAuditEvent(... policy-specific ...)`

Common accessors: `catalog()`, `eventTimeMillis()`,
`Optional<String> queryId()`.

`ConnectorProvider.defaultAccessControllerFactoryName()` — the D8
provider hook that selects the engine's default access controller
(e.g. `ranger-hive`) for catalogs of this type. Returning
`Optional.empty()` keeps behaviour compatible with existing connectors.

### 6.9 D9 — Reserved / not yet implemented

D9 (advanced statistics surface) has no public types in this M0
baseline. Connector authors should expose statistics through
`ConnectorStatisticsOps.getTableStatistics(...)` for now.

### 6.10 D10 — Dispatch & delegation

Types in `org.apache.doris.connector.api.dispatch` plus
`ConnectorRegistry` in spi.

#### `ConnectorDispatchOps`

Implemented by **host** connectors (e.g. `hms`, `hive`) that can
delegate certain tables to a different connector type at metadata-load
time (e.g. an HMS table whose `table_type` is `ICEBERG` is served by
the iceberg connector). Wire into `Connector` by overriding
`getDispatchOps()`.

```java
Optional<DispatchTarget> resolveTarget(ConnectorTableHandle handle,
                                       RawTableMetadata raw);
```

The implementation MUST be cheap and side-effect free — the engine
calls it during table loading and MUST NOT issue extra metastore
RPCs. It works only on the `RawTableMetadata` the host already
fetched.

#### `RawTableMetadata`

```java
Map<String,String> tableParameters();   // HMS table params
String             inputFormat();        // Hive InputFormat class or ""
String             storageLocation();
Map<String,String> serdeParameters();
```

Built via `RawTableMetadata.of(tableParams, inputFormat, location,
serdeParams)`; the package-private record `RawTableMetadataImpl` is
the default carrier.

#### `DispatchTarget`

Result of dispatch: `connectorType`, optional `backendName` (D2 backend
selector), and a `normalizedProps` map merged on top of the host's
catalog properties for the delegate. Build via
`DispatchTarget.builder(connectorType)`.

#### `ConnectorRegistry` (spi)

Engine-side registry the host calls to resolve the actual delegate
`Connector` at execution time:

```java
Optional<Connector> lookup(String type, ConnectorContext ctx);
Set<String>         listLoaded();
boolean             supports(String type, ConnectorCapability cap);
```

Reachable from `ConnectorContext.getConnectorRegistry()` (wired by
fe-core in M4 alongside `DelegatingConnectorMetadata`). Hosts should
declare every type they may dispatch to via
`Connector.declaredDelegateTypes()` so the engine can warn at startup
when a delegate is missing.

Capabilities that interact with this surface:
`EMITS_DELEGATABLE_TABLES`, `ACCEPTS_DELEGATION_FROM_HMS`,
`SUPPORTS_DELEGATING_LISTING`.

### 6.11 D11 — Reserved / not yet implemented

D11 (catalog property aliases / typed `ConnectorTableId`) is reserved
for a follow-up PR. Today, plugin code addresses tables as
`(database, table)` strings throughout the api.

### 6.12 D12 — Property metadata

Types in `org.apache.doris.connector.api` (not in a sub-package):
`ConnectorPropertyMetadata`, `PropertyValueType`, `PropertyScope`,
`PropertyValidator`, `ConnectorPropertyException`,
`ConnectorValidationContext`.

#### `ConnectorPropertyMetadata<T>`

Immutable descriptor returned from the various
`Connector.get*Properties(...)` methods. Convenient factories:

```java
ConnectorPropertyMetadata.stringProperty(name, desc, defaultValue);
ConnectorPropertyMetadata.intProperty(name, desc, defaultValue);
ConnectorPropertyMetadata.booleanProperty(name, desc, defaultValue);
ConnectorPropertyMetadata.requiredStringProperty(name, desc);
ConnectorPropertyMetadata.builder(name, PropertyValueType.STRING)
        .description(...)
        .defaultValue(...)
        .required(true)
        .aliases("legacy_name")
        .sensitive(true)            // mask in SHOW
        .hidden(true)               // omit from SHOW
        .deprecated("replacement_property", "removed in 3.x")
        .experimental(true)
        .scope(PropertyScope.SESSION)
        .validator(myValidator)
        .enumValues(List.of("rest", "hms", "glue"))
        .build();
```

Accessors mirror the builder. `Deprecation` is a nested value with
`replacement` / `message`. `PropertyValueType` is the canonical enum
(`STRING`, `INT`, `LONG`, `BOOLEAN`, `DOUBLE`, …) with
`javaType()` and a reverse `fromJavaType(Class)` lookup. `PropertyScope`
is `CATALOG`, `TABLE`, `SESSION`. `TableScope` enumerates DDL/DML
contexts (`CREATE`, `ALTER`, `READ`) for the `getTableProperties(scope)`
overload.

`PropertyValidator<T>` exposes `validate(value, ValidationContext)`;
the nested `ValidationContext` carries the property bag plus engine
services. `ConnectorPropertyException(code, message[, cause])` is the
standardised failure type — use it instead of bare
`IllegalArgumentException` for property-level errors so the engine can
surface a stable error code.

`ConnectorValidationContext` is the engine-services bag handed to
`Connector.preCreateValidation(...)` so the plugin can request
infrastructure (e.g. driver checksum, BE reachability) without
reaching into fe-core.

## 7. Implementing a new connector — step by step

The recipe below produces a runnable plugin that fe-core will load.

1. **Create the module.** Copy `fe-connector-jdbc/pom.xml` as a starting
   point; adjust `artifactId`, dependencies, and ServiceLoader file.
   Add the module to `fe/fe-connector/pom.xml`.

2. **Implement `ConnectorProvider`.** Minimal example:

   ```java
   package org.apache.doris.connector.foo;

   import java.util.Map;
   import org.apache.doris.connector.api.Connector;
   import org.apache.doris.connector.spi.ConnectorContext;
   import org.apache.doris.connector.spi.ConnectorProvider;

   public final class FooConnectorProvider implements ConnectorProvider {
       @Override public String getType() { return "foo"; }

       @Override public void validateProperties(Map<String, String> props) {
           if (!props.containsKey("foo.endpoint")) {
               throw new IllegalArgumentException("foo.endpoint is required");
           }
       }

       @Override public Connector create(Map<String, String> props,
                                         ConnectorContext ctx) {
           return new FooConnector(props, ctx);
       }
   }
   ```

3. **Implement `Connector`.** Override `getMetadata`,
   `getCapabilities`, `getCatalogProperties`, `getScanPlanProvider`,
   and `defaultTestConnection` as appropriate. Stash the
   `ConnectorContext` in a field — it is the only entry point to engine
   services.

4. **Implement `ConnectorMetadata`.** Implement the read-side methods
   from `ConnectorTableOps` / `ConnectorSchemaOps` first; layer
   pushdown, statistics, and writes on top. Override only the
   `Optional<*Ops>` accessors corresponding to capabilities your
   plugin actually advertises. Do NOT add capability flags without the
   matching implementation — the planner will gate features it expects
   to find and crash loudly when they are missing.

5. **Declare capabilities.** Return the exact set your `*Ops`
   implementations honour. Pick from §5; use the smallest set that
   covers the plugin's behaviour today, then grow it as features land.

6. **Wire credentials, caches, events as needed.**
   * Credentials: build a `JdbcRequest` / `StorageRequest` /
     `MetastorePrincipal` / `HttpEndpoint` and call the matching
     `ctx.getCredentialBroker().…`. Until fe-core wires the broker
     (M3) catch the `UnsupportedOperationException` and fall back to
     property-driven credentials.
   * Caches: return `ConnectorMetaCacheBinding`s from
     `getMetaCacheBindings()` and call
     `ctx.getOrCreateCache(binding)` lazily.
   * Events: implement `EventSourceOps` and return it from
     `ConnectorMetadata.getEventSourceOps()`. Self-managed plugins
     publish through `ctx.publishExternalEvent(...)`.

7. **Register via ServiceLoader.** Create the file
   `src/main/resources/META-INF/services/org.apache.doris.connector.spi.ConnectorProvider`
   containing the FQCN of your provider:

   ```
   org.apache.doris.connector.foo.FooConnectorProvider
   ```

   See `fe-connector-jdbc/src/main/resources/META-INF/services/...`
   for an example.

8. **Tests.** Place plugin unit tests under
   `fe-connector-foo/src/test/java/...`. Naming follows the engine
   convention (`*Test.java` consumed by Surefire). Each public type
   that carries logic deserves a focused test; reach for the JUnit5
   stack and `Assertions` from `org.junit.jupiter.api`.

## 8. Conventions / style rules

* **api never depends on spi.** The Maven dependency from
  `fe-connector-spi` to `fe-connector-api` is one-way; reverse imports
  break the loader contract.
* **No `import static`.** The repository checkstyle bans static
  imports; spell out the class qualifier.
* **No defensive `if (valid)` guards.** Doris house style: assert
  correctness or crash. Constructor null checks that throw
  `IllegalArgumentException` (or `NullPointerException` from
  `Objects.requireNonNull`) are fine and pervasive in records.
* **Sealed interfaces + records are encouraged.** This module targets
  Java 17. Pattern matching for `switch` is **preview** in 17 and is
  not used.
* **Capability ordinals are frozen at 47 (M0).** Append-only. Never
  reorder, never delete.
* **`ConnectorMetadata` defaults must return `Optional.empty()` or a
  sentinel** (e.g. `EventSourceOps.NONE`). They MUST NOT throw — the
  whole point of the layered defaults is that an existing plugin
  recompiles cleanly when a new optional surface lands.
* **Plugins never reach into fe-core.** Use `ConnectorContext`. If
  something is missing, add it to the context, do not import fe-core
  symbols.
* **Engine-supplied `Ops` implementations may be missing pre-wiring.**
  Methods like `ctx.getCredentialBroker()`, `getMasterOnlyScheduler()`,
  `getConnectorRegistry()` throw `UnsupportedOperationException` until
  the corresponding milestone wires them. Plugins MUST handle that
  cleanly so they keep loading on older engine builds.
* **Identity uses `(String database, String table)`.** A typed
  `ConnectorTableId` is intentionally deferred — do not invent your
  own. When in doubt, mirror the closest existing call site.
* **Two distinct `RefKind` enums exist on purpose.** Use
  `api.timetravel.RefKind` for time-travel and ref CRUD; use
  `api.event.ConnectorMetaChangeEvent.RefKind` for event payloads.
* **Use `ConnectorPropertyException` for property errors.** It carries
  a stable error code that fe-core surfaces to users.

## 9. Build & test

The engine build script handles the full FE compile chain:

```bash
sh build.sh --fe
```

Per-module unit tests are run through Maven. The
`-Dmaven.build.cache.enabled=false` flag is **required**: the Maven
build-cache extension caches Surefire results and would silently skip
re-running tests after source changes, hiding regressions.

```bash
cd fe
mvn -q -pl fe-connector/fe-connector-api -am \
    -Dmaven.build.cache.enabled=false test
mvn -q -pl fe-connector/fe-connector-jdbc -am \
    -Dmaven.build.cache.enabled=false test
```

Checkstyle is wired into `mvn validate`; a plain `build.sh --fe` will
fail on style violations. See the `fe-code-style` skill for the auto-fix
flow when a violation surfaces.

## 10. References

Java packages (relative to the repository root):

* `fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/`
  — root package: `Connector`, `ConnectorMetadata`, `ConnectorCapability`,
  `ConnectorSession`, `ConnectorType`, `ConnectorColumn`,
  `ConnectorTableSchema`, `ConnectorDatabaseMetadata`,
  `ConnectorPartitionInfo`, `ConnectorTableStatistics`,
  `ConnectorTestResult`, `ConnectorValidationContext`,
  `ConnectorHttpSecurityHook`, `ConnectorPropertyMetadata`,
  `PropertyValueType`, `PropertyScope`, `PropertyValidator`,
  `TableScope`, `DorisConnectorException`,
  `ConnectorPropertyException`, plus the `*Ops` mixins
  (`ConnectorSchemaOps`, `ConnectorTableOps`, `ConnectorPushdownOps`,
  `ConnectorStatisticsOps`, `ConnectorWriteOps`,
  `ConnectorIdentifierOps`).
* `…/api/action/` — D1 procedures.
* `…/api/audit/` — D8 audit events.
* `…/api/cache/` — D3 cache bindings & invalidation.
* `…/api/credential/` — D4 credential broker.
* `…/api/dispatch/` — D10 dispatch.
* `…/api/event/` — D7 event source + 13 sealed permits.
* `…/api/handle/` — opaque table / column / partition / write handles.
* `…/api/mtmv/` — D8 MTMV.
* `…/api/policy/` — D8 policy hints.
* `…/api/pushdown/` — pushdown expression tree
  (`ConnectorExpression`, `ConnectorAnd`, `ConnectorOr`, `ConnectorNot`,
  `ConnectorComparison`, `ConnectorBetween`, `ConnectorIn`,
  `ConnectorIsNull`, `ConnectorLike`, `ConnectorRange`,
  `ConnectorDomain`, `ConnectorLiteral`, `ConnectorColumnRef`,
  `ConnectorFunctionCall`, `ConnectorFilterConstraint`,
  `ConnectorColumnAssignment`, `FilterApplicationResult`,
  `ProjectionApplicationResult`, `LimitApplicationResult`).
* `…/api/scan/` — `ConnectorScanPlanProvider`, `ConnectorScanRange`,
  `ConnectorScanRangeType`, `ConnectorDeleteFile`,
  `ScanNodePropertiesResult`.
* `…/api/systable/` — D6 sys tables.
* `…/api/timetravel/` — D5 time travel.
* `…/api/write/` — D1 write configuration & intent.
* `fe/fe-connector/fe-connector-spi/src/main/java/org/apache/doris/connector/spi/`
  — `ConnectorProvider`, `ConnectorContext`, `ConnectorRegistry`.

Reference plugins (read these for end-to-end examples):

* `fe/fe-connector/fe-connector-jdbc/` — most complete read +
  pushdown + capability-based gating.
  Key classes: `JdbcConnectorProvider`, `JdbcDorisConnector`,
  `JdbcConnectorMetadata`, `JdbcScanPlanProvider`,
  `JdbcConnectorProperties`, `JdbcUrlNormalizer` (uses
  `ConnectorContext.sanitizeJdbcUrl`).
* `fe/fe-connector/fe-connector-es/` — REST passthrough, scan-node
  properties, shard-based ranges.
  Key classes: `EsConnectorProvider`, `EsConnector`,
  `EsConnectorMetadata`, `EsScanPlanProvider`,
  `EsConnectorRestClient`, `EsMetadataFetcher`.
