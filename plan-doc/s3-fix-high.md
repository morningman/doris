# S3 FileSystem 修复总结 — HIGH 级别

本文档记录 `plan-doc/s3.md` 中 HIGH 级别 6 个问题（#3 / #4 / #5 / #6 / #7 / #8）的修复。这些问题相互交织，根因都是"S3 没有真目录"，因此一次性协调修复。

适用于 `s3` / `cos` / `obs` / `oss` 四个共享 `S3FileSystem` + `S3ObjStorage` 的后端。

---

## 修复矩阵

| # | 文件 / 方法 | 修复策略 |
|---|-------------|----------|
| #3 | `S3FileSystem.rename` | HEAD `dst` → 存在则抛 `FileAlreadyExistsException`；HEAD `src` → 不是 key 但前缀有子项时拒绝并提示使用 `renameDirectory` |
| #4 | `S3FileSystem.exists` + `renameDirectory` | `exists` 在 HEAD 404 后 fallback 到 `ListObjectsV2(prefix=loc+"/", maxKeys=1)`；新增 `renameDirectory` override（list → 空时回调 → 检查 dst 前缀空 → 逐 key copy → batch delete） |
| #5 | `S3FileIterator` | 跳过 key 等于前缀或以 `/` 结尾的 marker 占位对象 |
| #6 | `S3FileSystem.listFiles(Location)` | 改为 delimiter-mode 列表（`listObjectsNonRecursive`），仅返回直接子文件，过滤 marker。Strategy (a) |
| #7 | `FileSystem` / `GlobListing` Javadoc | 校正 `getMaxFile()` 文档使其与实现（页限游标）一致；无行为变更 |
| #8 | `S3FileSystem.delete(loc, false)` | 先 `ListObjectsV2(prefix=loc+"/", maxKeys=2)` 探测，若有非 marker 子项则抛 `IOException("Directory not empty")` |

---

## 关键设计决策

### #6 选用 Strategy (a)（修正契约）而非 Strategy (b)（仅文档说明）

调用面审计：FE 中只有一处非测试代码调用 `FileSystem.listFiles(Location)`：

- `fe/fe-core/.../HMSTransaction.java:1721` — `deleteTargetPathContents(...)` 内部分别调用 `listDirectories(...)` 和 `listFiles(...)`，明显期望 **non-recursive** 语义；若沿用旧的递归行为，这里实际上是潜在的"误删整棵子树"漏洞。

因此选 (a) 既符合契约又顺手堵掉一个潜在过删风险，对单一调用方更安全。

### `renameDirectory` 实现要点

1. 分页拉取所有 src key（使用 `S3ObjStorage.listObjects(prefix, continuation)`）；
2. 列表为空 → 调用 `whenSrcNotExists.run()` 直接返回（与默认契约一致）；
3. 任何 copy 之前先 `listObjects(dstPrefix, null, 1)` 确认 dst 为空；非空抛 `FileAlreadyExistsException`，避免半写状态；
4. 逐 key 调用 `objStorage.copyObject(...)`（已使用 #2 修复后的 percent-encoded copySource）；
5. 全部源 key 通过 `deleteObjectsByKeys(bucket, srcKeys)` **批量** 删除（按 1000 一批）。

注意：实现非原子；崩溃可能留下部分残余对象。S3 没有原生 atomic move 能力，已在 Javadoc 明确说明。

### `S3FileSystem.exists` 二次探测

- 优先走父类 `headObject`；
- 仅在 404 时再做 1-key `ListObjectsV2(prefix=uri+"/", maxKeys=1)`；
- 文档显式说明："on object stores, exists returns true for any non-empty prefix"。

这把 #4 中"Hive/Spark 写入无 marker 目录被 `exists` 误判为不存在"的根因消除。

### #7 文档与实现的对齐方向

实际实现把 `maxFile` 当作"page-limit 之后的下一个 cursor"用，远比"全量扫描的最后一个 key"更省 RTT。修订后文档：

- 触发页限：`maxFile` = 页限之后的下一个匹配 key；
- 否则：`maxFile` = 当前页最后一个匹配 key；空匹配 → 空串。

调用方判断"是否还有更多"：比较 `maxFile` 与 `getFiles()` 末项是否相等。

---

## 新增 / 修改的工具方法

`S3ObjStorage`：

- `listObjects(remotePath, continuationToken, maxKeys)` — 带上限的 probe 变体；
- `listObjectsNonRecursive(remotePath, continuationToken)` — 使用 `delimiter="/"` 仅列出直接子项。

`S3FileSystem` 私有助手：

- `parseSchemeBucketKey(uri)` / `stripBucketPrefix(uri)` — 拆解 `scheme://bucket/key`，用于在 `renameDirectory` / `delete` 内部计算 key 前缀；
- `prefixHasChildren(uri)` — `exists` / `rename` 共用的前缀探测。

> 注：`parseSchemeBucketKey` 仍是手卷字符串解析（与原 `reconstructUri` 一致），`S3Uri.parse` 的 path-style 修复留待 MEDIUM 组（#9-#11）一并处理。

---

## 单元测试

文件：`fe/fe-filesystem/fe-filesystem-s3/src/test/java/org/apache/doris/filesystem/s3/S3FileSystemTest.java`

新增：

- `delete_nonRecursiveOnEmptyDirSucceeds`
- `delete_nonRecursiveOnNonEmptyDirThrows`
- `rename_throwsFileAlreadyExistsWhenDstExists`
- `rename_rejectsDirectoryPrefixSrc`
- `exists_returnsTrueForMarkerlessPrefixWithChildren`
- `exists_returnsFalseWhenNoKeyAndNoChildren`
- `renameDirectory_copiesEachChildAndBatchDeletes`
- `renameDirectory_runsWhenSrcNotExistsCallback`
- `renameDirectory_abortsWhenDstHasObjects`
- `list_iteratorSkipsDirectoryMarkerEntries`
- `listFiles_returnsOnlyDirectChildrenAndSkipsMarker`

更新：4 个既有用例适配新的探测 / 列表调用形态。

### 运行结果

```
cd fe/fe-filesystem/fe-filesystem-s3 && mvn -q test
# Tests run: 83, Failures: 0, Errors: 0, Skipped: 0
# BUILD SUCCESS
```

cos / obs / oss 复用模块、api 模块同步通过；checkstyle 0 violation。

---

## 行为变更（用户可见）

1. `S3FileSystem.rename`：dst 存在时由"静默覆盖"改为抛 `FileAlreadyExistsException`；src 是无 key 的目录前缀时由"静默成功"改为抛 IOException 并提示使用 `renameDirectory`。
2. `S3FileSystem.delete(loc, false)`：非空目录由"静默 no-op"改为抛 `IOException("Directory not empty: ...")`。
3. `S3FileSystem.listFiles(loc)`：由"返回整棵子树"改为"只返回直接子文件"，与契约一致。
4. `S3FileSystem.exists(loc)`：marker-less 目录现在正确返回 `true`。
5. `S3FileSystem.renameDirectory`：从依赖默认实现（在 S3 上不安全）改为完整目录搬移。

---

## 已知遗留 / 后续

- `listDirectories` 仍返回空集（未利用 CommonPrefixes），代码中已加 `TODO(audit#6)` 注释；
- `parseSchemeBucketKey` 的手卷解析在 path-style URI 下仍然不完全正确，等 MEDIUM 组 #9-#11 一并改造为基于 `S3Uri.parse`；
- `rename` 现在多了 1 次 dst HEAD + 1 次 src HEAD（最坏 + 1 次 list）的成本，为契约正确性付出的可接受代价；
- `renameDirectory` 非原子，崩溃可能残留部分对象（受 S3 原语限制）。
