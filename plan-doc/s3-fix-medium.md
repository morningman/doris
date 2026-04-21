# S3 FileSystem 修复总结 — MEDIUM 级别

本文档记录 `plan-doc/s3.md` 中 MEDIUM 级别 8 个问题（#9-#16）的修复。本组主要围绕 path-style URI 支持、批量删除、glob 引擎跨平台、`mkdirs` 幂等性、以及错误吞噬几个方面。

适用 `s3` / `cos` / `obs` / `oss` 四个共享后端。

---

## 修复矩阵

| # | 区域 | 修复 |
|---|------|------|
| #9  | `S3Uri.parse` | 当 `pathStyleAccess=true` 且 scheme 为 `http(s)` 时，把 host 后第一个 path 段作为 bucket、其余作为 key |
| #10 | `S3FileSystem.globListWithLimit` | URI 解析改用 `S3Uri.parse(uri, usePathStyle)` |
| #11 | `reconstructUri` 等私有助手 | 移除 hand-rolled `://` 拆分，统一通过 `S3Uri.parse` 派生 `parseUri` / `uriBase` / `reconstructUri`；`renameDirectory` / 列表迭代器路径都受益 |
| #12 | `S3FileSystem.deleteFiles` | 新增 override：按 bucket 分组后调用 `deleteObjectsByKeys`（已天然 1000-key 一批） |
| #13 | `S3FileSystem.deleteRecursive` | 改为收集 key 后单次 `deleteObjectsByKeys` 批量删除 |
| #14 | `S3ObjStorage.abortMultipartUpload` | 不再吞 `SdkException`；改为包装为 `IOException`（`S3Exception` 时附带 HTTP status / AWS error code） |
| #15 | `globListWithLimit` PathMatcher | 移除对 `Paths.get` + 默认 `FileSystem` 的依赖；新增 `globToRegex(...)` 把 glob 转成 `java.util.regex.Pattern` 后匹配原始 S3 key |
| #16 | `S3FileSystem.mkdirs` | HEAD 探测幂等：marker 已存在 → no-op；同名实文件存在 → 抛 IOException 拒绝覆盖；自顶向下补齐父级 marker |

---

## 关键设计点

### #9 path-style URI 处理

```text
https://endpoint/bucket/key/x  +  pathStyleAccess=true
  → bucket = "bucket", key = "key/x"

s3://bucket/key/x              (无关 pathStyleAccess)
  → bucket = "bucket", key = "key/x"

https://bucket.s3.amazonaws.com/key/x  +  pathStyleAccess=false
  → bucket = "bucket.s3.amazonaws.com" 维持原行为（virtual-hosted）
```

边界：`https://endpoint/bucket`（仅 bucket）、`https://endpoint/bucket/`（trailing slash → key 为空串）、`https://endpoint/`（缺 bucket → 抛异常）均覆盖单测。

### #11 统一 URI 解析

HIGH 组里临时引入的 `parseSchemeBucketKey` / `stripBucketPrefix` 已删除，新接口：

- `parseUri(uri)` —— 内部包装 `S3Uri.parse(uri, usePathStyle)`，所有 bucket/key 派生统一来源；
- `uriBase(uri)` —— 返回 `scheme://host(/bucket)` 前缀，用于 `renameDirectory` 拼接目的 key；
- `reconstructUri(prefix, key)` —— 由静态变成实例方法，借助 `parseUri` 正确处理 path-style。

`globListWithLimit`、`renameDirectory`、`deleteFiles`、`deleteRecursive`、列表迭代器的 URI 还原全部走这条新路径。

### #12 / #13 批量删除

- `deleteFiles(Collection<Location>)`：按 bucket 分组（path-style 场景下 host 部分相同 bucket 不同也能正确划分），每个 bucket 内一次 `deleteObjectsByKeys`，由其内部按 1000 一批分页；空集合直接 no-op。
- `deleteRecursive(prefix)`：从"逐对象 DELETE"改为"先收集 key 后单次批量"。失败聚合复用 `deleteObjectsByKeys` 现有的 `failedKeys` 路径。

### #14 abortMultipartUpload 不再吞错

- 普通 `SdkException` → `IOException("Failed to abort multipart upload ... ", e)`；
- `S3Exception` → 附带 `statusCode()` 与 AWS error code，便于排查；
- 保留原 logging（INFO/WARN）用于即时可见性。

调用方现在能感知 abort 失败 → 可主动重试或告警，从而避免孤儿 multipart parts 长期占用存储成本。

### #15 跨平台 glob 匹配

- 原实现：`Paths.get(pattern)` + `FileSystems.getDefault().getPathMatcher("glob:..." )`；
- 问题：Windows 用 `\` 作分隔符；Linux 上 `:`、`\` 等字符也会让 `Paths.get` 拒绝合法 S3 key；
- 新实现：纯字符串级 `globToRegex(...)` 把以下 glob 元素翻译为正则：
  - `*` → `[^/]*`；`**` → `.*`（匹配跨目录）；
  - `?` → `[^/]`；
  - `[abc]` / `[!abc]` → `[abc]` / `[^abc]`；
  - `{a,b,c}` → `(?:a|b|c)`；
  - 其它字符按需转义；`/` 始终字面量。
- 匹配时直接对 S3 key 字符串 `Pattern.matcher(key).matches()`；不再访问任何 OS FS。

#### 行为差异

`**/*.csv` 不再匹配根目录的 `a.csv`（必须至少一个 `/`）。这与多数 glob 实现（含 bash `globstar`）一致，但与 JDK PathMatcher 的"`**` 可空"细微差异不同。考虑到当前调用方实际形态，影响面可忽略；若后续有反馈可再加一条特殊规则把 `**/` 折叠成可空。

### #16 mkdirs 幂等 + 父 marker

```text
mkdirs("s3://b/a/b/c/")
  ① HEAD "s3://b/a/b/c/" → 命中则直接返回
  ② HEAD "s3://b/a/b/c"  → 命中则抛 IOException（拒绝把实文件改写为 0 字节 marker）
  ③ 依次为 "a/", "a/b/", "a/b/c/" 各做一次 HEAD；不存在才 PutObject
```

并发：S3 没有原生 atomic create-if-absent（在不依赖 If-None-Match 的情况下），HEAD-then-PUT 的窗口期残留风险已在 Javadoc 标注。

---

## 单元测试

`S3UriTest` 新增（path-style）：

- `parsePathStyleHttpsTreatsFirstPathSegmentAsBucket`
- `parsePathStyleHttpAlsoSupported`
- `parsePathStyleBucketOnly`
- `parsePathStyleTrailingSlashIsEmptyKey`
- `parsePathStyleMissingBucketThrows`
- `parseS3SchemeIgnoresPathStyleFlag`
- `parseHttpsWithoutPathStyleKeepsHostAsBucket`

`S3FileSystemTest` 新增 / 更新：

- `mkdirs_putsZeroByteMarkerWithTrailingSlashAndParentMarkers`
- `mkdirs_doesNotDoubleSlashIfAlreadyPresent`
- `mkdirs_isIdempotentWhenMarkerAlreadyExists`
- `mkdirs_skipsParentMarkerWhenAlreadyPresent`
- `mkdirs_rejectsWhenRealFileExistsAtSamePath`
- `delete_recursiveBatchDeletesAllObjectsUnderPrefix`
- `deleteFiles_groupsByBucketAndCallsBatchDeleteOncePerBucket`
- `deleteFiles_emptyInputIsNoOp`
- `globToRegex_*`（9 用例：`*.csv`、`dir/*.csv` 非递归、`**/*.csv`、`[abc]`、`{x,y}`、含空格、含 `:` / `\`、`?`、转义）

`S3ObjStorageMockTest` 新增：

- `abortMultipartUpload_throwsIOExceptionOnSdkException`
- `abortMultipartUpload_throwsIOExceptionOnS3ExceptionWithStatusCode`

### 运行结果

```
cd fe/fe-filesystem/fe-filesystem-s3 && mvn -q test
# Tests run: 106, Failures: 0, Errors: 0, Skipped: 0
# BUILD SUCCESS
```

`fe/fe-filesystem` reactor 全量通过，0 checkstyle violation。

---

## 行为变更（用户可见）

1. `S3Uri.parse(uri, true)` 对 `http(s)://endpoint/bucket/key` URI 现在返回正确的 bucket（之前是 host）；之前依赖错误行为的代码会被纠正。
2. `S3FileSystem.deleteFiles` / `deleteRecursive` 网络往返从 N 次降为 ⌈N/1000⌉ 次。
3. `mkdirs` 不再覆盖同名实文件；不再产生重复 PUT；额外为父级写 marker，便于后续 `exists(parent)` 正确返回。
4. `abortMultipartUpload` 现在抛出 `IOException`，调用方需要相应处理（先前是静默 logging）。
5. glob 匹配现在与 OS 无关，且能匹配 key 中含 `:` / `\` / 空格的对象。

---

## 已知遗留 / 后续

- `globToRegex("**/*.csv")` 不匹配根目录裸文件；如需对齐 JDK PathMatcher 的"`**` 可空"语义，可在后续按需补充；
- `mkdirs` 的 HEAD-then-PUT 仍非原子；如启用 S3 2024 新增的 `If-None-Match: *` 可消除窗口；
- LOW / NIT 组中的 `containsGlob`、`listFiles(loc)` 边缘语义、`deleteObjectsByKeys` 失败信息聚合等留待下一组。
