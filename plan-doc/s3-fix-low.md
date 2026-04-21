# S3 FileSystem 修复总结 — LOW 级别

本文档记录 `plan-doc/s3.md` 中 LOW 级别 5 个问题（#17-#21）的修复。本组聚焦于"行为细节与契约一致性"，没有破坏性变更。

适用 `s3` / `cos` / `obs` / `oss` 共享后端。

---

## 修复矩阵

| # | 区域 | 修复 |
|---|------|------|
| #17 | `S3FileSystem.containsGlob` | 仅识别 `*` / `?` 为 glob 字符；`[` / `{` 视作合法 key 字符 |
| #18 | `S3FileSystem.listFiles(loc)` glob 分支 | 调用 `globListWithLimit(..., 0L, 0L)`（契约：0 = unlimited），不再用 `-1L` |
| #19 | `S3ObjStorage.deleteObjectsByKeys` | 部分失败抛出的 `IOException` 包含失败总数、bucket、最多 10 个失败 key 样本，溢出附 `(and N more, see WARN log...)` |
| #20 | `S3ObjStorage.getRelativePath` | 改为内部委托 `getRelativePathSafe`，使 `listObjects` / `listObjectsNonRecursive` / `listObjectsWithPrefix` 在前缀是否带 `/` 时返回一致的 `relativePath` |
| #21 | `S3FileSystem.S3InputFile` | 引入 `ensureMetadata()` 缓存首次 HEAD 结果；`length()` / `exists()` / `lastModifiedTime()` / `newStream()` 之间共享同一次 HEAD；`NoSuchKey` 缓存为 `exists=false` |

---

## 关键设计点

### #17 / #18 — 契约一致性

- `containsGlob` 之前把 `[`、`{` 当 glob 元字符，但 S3 key 完全允许它们做字面量。例如 `s3://b/raw[2024-01-01]/file` 会被误送进 `globListWithLimit`，再被 PathMatcher 解析失败 → 返回空集。收紧到 `*` / `?` 后行为更接近用户直觉，也与多数对象存储 SDK 一致。
- `listFiles` glob 分支由 `-1L` 改成 `0L`，与 `FileSystem.listFiles` 文档"0 = unlimited"对齐；`globListWithLimit` 内部对两者行为相同（都按"无上限"处理），无运行时改变，仅消除契约模糊。

### #19 — 失败信息更可用

旧版仅在异常消息里暴露第一个失败 key，运维侧无法从单条日志里判断"是否还有更多失败"。新版：

```
Deleted N out of M objects from bucket=<bucket>; failed=K; sample=[k1, k2, ..., k10] (and X more, see WARN log...)
```

每个失败 key 仍按原逻辑独立 WARN 日志，便于补救。

### #20 — 统一相对路径归一化

- 旧 `getRelativePath`：直接用 `key.substring(prefix.length())`，要求调用方手动确保 `prefix` 末尾带 `/`，否则会在 `relativePath` 里出现前导 `/`。
- 旧 `getRelativePathSafe`：先 `prefix + "/"`（若缺）再裁剪，结果稳定。
- 现在 `getRelativePath` 全权交给 `getRelativePathSafe`，所有列表入口走同一规范化。`listObjects("bucket-root")` 与 `listObjects("bucket-root/")` 现在返回完全一致的 `relativePath`。

### #21 — `S3InputFile` 单次 HEAD 缓存

字段：

```java
private boolean metadataLoaded;
private boolean cachedExists;
private long cachedLength;
private Instant cachedLastModified;
```

`ensureMetadata()` 首次调用做 HEAD：

- 命中：`cachedExists=true`，缓存 length / lastModified；
- `NotFound`：`cachedExists=false`，length / lastModified 留默认；
- 其它 IOException：直接外抛，不留半缓存状态。

后续 `length()` / `exists()` / `lastModifiedTime()` / `newStream()` 全部读缓存。Javadoc 明确标注"snapshot 语义：缓存仅对单一 `S3InputFile` 实例生效，对象在远端被修改不会反映出来"。

> 未实现：`S3FileSystem.newInputFile(loc, length)` override（用 length hint 直接初始化缓存）。属审计 #10 NIT，按计划留待后续。

---

## 单元测试

`S3FileSystemTest`：

- `listFiles_doesNotTreatBracketsOrBracesAsGlob`（5 ValueSource 用例）
- `listFiles_starAndQuestionMarkRouteThroughGlob`（2 ValueSource 用例）
- `listFiles_globBranchRequestsUnlimitedByteAndFileCount`
- `s3InputFile_singleHeadAcrossLengthExistsAndNewStream`
- `s3InputFile_existsCachedAsFalseOnNotFound`

`S3ObjStorageMockTest`：

- `deleteObjectsByKeys_partialFailure_messageCarriesCountAndSampleKeys`
- `listObjects_andListObjectsWithPrefix_relativePathsMatch_noTrailingSlash`
- `listObjects_relativePathAtBucketRoot_returnsFullKey`

### 运行结果

```
cd fe/fe-filesystem/fe-filesystem-s3 && mvn -q test
# Tests run: 119, Failures: 0, Errors: 0, Skipped: 0
# 0 Checkstyle violations
# BUILD SUCCESS
```

---

## 行为变更（用户可见）

1. 含 `[` / `{` 的字面 key（不含 `*` / `?`）现在能被 `listFiles` 正确直查，不再走 glob 路径而返回空集。
2. `S3InputFile` 同一实例上的多次元数据访问只会发起 1 次 HEAD（之前是 N 次）。
3. `deleteObjectsByKeys` 部分失败时的 `IOException` 消息更详细。
4. `listObjects` 与 `listObjectsWithPrefix` 在 prefix 是否带 `/` 的情况下返回完全一致的 `relativePath`。

无任何已有契约的破坏性变更。

---

## 已知遗留 / 后续

- 审计 #10 NIT：`newInputFile(loc, length)` 利用 length hint 跳过 HEAD —— 留待 NIT 组；
- 审计 #6 TODO：`listDirectories` 暴露 `CommonPrefixes` —— 仍保留 `TODO(audit#6)`；
- `S3InputFile` 缓存有"远端对象被修改后本地仍读旧 metadata"的固有限制（snapshot 语义已在 Javadoc 标注）。
