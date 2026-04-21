# Azure FileSystem CRITICAL 修复总结

本文档对应 `plan-doc/azure.md` 中标记为 CRITICAL 的三个问题的修复，全部位于
`fe/fe-filesystem/fe-filesystem-azure/`。

## 修复矩阵

| 编号 | 问题 | 处理方式 |
| --- | --- | --- |
| F01 | `delete(loc, recursive=true)` 在递归清理 `<uri>/` 之后又对无斜杠的 `<uri>` 发起 `deleteObject`，会误删与目录同名的兄弟 blob | 递归分支只调用 `deleteRecursive(prefix)` 后 `return`，不再触碰无斜杠键。目录标记 `<uri>/` 由前面的递归列举自然覆盖 |
| F02 | `delete(loc, recursive=false)` 对非空虚目录静默成功（仅 `deleteObject(<uri>)`，404 被吞掉，子 blob 残留） | 新增 `hasChildUnder(prefix)` 单页探测：若存在 key 长度严格大于 prefix 的 blob，则抛 `IOException("Directory not empty: ...")`；否则保持原有"删除目标键、404 视为幂等成功"语义并补 Javadoc 说明 |
| F03 | 未实现 `globListWithLimit`，命中即抛 `UnsupportedOperationException`，导致 broker-load 等任何 glob 路径在 Azure 上彻底不可用 | 新增 `globListWithLimit` 实现，移植 `S3FileSystem` 的算法：解析 `AzureUri` → 取 `longestNonGlobPrefix` 作为列举前缀 → 分页 `objStorage.listObjects` → 对每个 blob 跳过目录标记、按 `startAfter` 跳过、用 `globToRegex` 编译的全 key 正则过滤 → 命中页/字节上限时把当前 key 记为 `maxFile` 并退出；否则把 `maxFile` 推进到最后一个匹配 key。同时新增 `containsGlob` / `longestNonGlobPrefix` / `globToRegex` 三个本地静态辅助方法，**不引入对 fe-filesystem-s3 模块的依赖** |

## 关键设计点

1. **F01/F02 的"目录"判定**：Azure 是扁平命名空间，`hasChildUnder` 通过比较 key 长度 (`obj.getKey().length() > prefixKey.length()`) 排除目录标记自身，只判断真正的子条目。这避免了把 `foo/` 标记 blob 当作"非空"导致的误报。
2. **F02 的 404 语义**：仅在确认无子条目后才发起 `deleteObject(<uri>)`。对于不存在的目标，保留原有的吞 404 行为，使删除幂等，与 `S3FileSystem.delete(loc, false)` 在缺失目标时的契约一致。Javadoc 显式声明了这一行为。
3. **F03 与现有契约对齐**：
   - `maxFile` 语义遵循已经在 S3 MEDIUM 提交里更新过的 `FileSystem.globListWithLimit` Javadoc——分页耗尽时返回最后一个匹配 key、命中限额时返回越界的下一个匹配 key、无任何匹配时为空串。
   - `startAfter` 在限额检查之前生效，避免误把"上一页边界键"算入下一次的 page 计数。
   - 仅 `*` / `?` 视作 glob meta，`[` / `{` 在 Azure blob key 中合法；这与 S3 实现一致，保持跨实现行为一致性。
4. **避免跨模块耦合**：故意不依赖 `fe-filesystem-s3`。所有 helper（`globToRegex` 等）都在 `AzureFileSystem` 内部以 `static` 形式重新实现。

## 单元测试

`AzureFileSystemTest` 新增 8 个 case：

- `delete_recursive_doesNotTouchSiblingBlobWithSameName`
- `delete_nonRecursive_throwsIfDirectoryNotEmpty`
- `delete_nonRecursive_silentlyAcceptsMissingTarget`
- `globListWithLimit_returnsMatchingBlobs`
- `globListWithLimit_skipsDirectoryMarkers`
- `globListWithLimit_maxFileIsCursorWhenLimitHit`
- `globListWithLimit_maxFileIsLastKeyWhenExhausted`
- `globListWithLimit_startAfterAppliedBeforeLimit`

执行：

```
cd fe/fe-filesystem/fe-filesystem-azure
mvn test -Dmaven.build.cache.enabled=false
```

结果：`Tests run: 35, Failures: 0, Errors: 0, Skipped: 0`，0 Checkstyle 违规，BUILD SUCCESS。

## 行为变更

- `delete(loc, recursive=true)` 不再误删与目录同名的兄弟 blob。
- `delete(loc, recursive=false)` 对非空虚目录由"静默成功"改为抛 `IOException("Directory not empty: ...")`，与 S3 行为对齐；这是一处**对调用方可见的契约变更**。对缺失目标继续维持幂等成功。
- `globListWithLimit` 由"抛 UOE"改为"返回正确分页结果"，恢复 broker-load 等 glob 调用路径在 Azure 上的可用性。

## 已知遗留

- `AzureUri` 的百分号编码、特殊字符处理等问题（F18 LOW）未在本批修复，留待 LOW 组处理。
- HIGH 组中的 `listFiles(glob)` 单层匹配 (F04)、`rename` 系列 (F05/F06/F07)、`mkdirs` overwrite (F08)、`create` vs `createOrOverwrite` (F09)、`exists` 虚目录 (F10) 不在本批范围。
