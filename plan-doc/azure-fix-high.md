# Azure FileSystem HIGH 修复总结

本文档对应 `plan-doc/azure.md` 中标记为 HIGH 的 7 个问题（F04–F10），
全部位于 `fe/fe-filesystem/fe-filesystem-azure/`。基线为 CRITICAL 修复
后的提交 `870b7508b7b`。

## 修复矩阵

| 编号 | 问题 | 处理方式 |
| --- | --- | --- |
| F04 | `listFiles(loc)` 不识别 glob 字符，对带 `*?[{` 的路径直接当字面前缀，返回空列表 | 新增 `listFiles(Location)` override，三段分发：无 glob → 沿用默认 `list()` 过滤非目录；单层 glob (`isSingleLevelGlob`) → 走 `listFilesSingleLevelGlob` 单层匹配；跨段 glob → 复用 CRITICAL 已实现的 `globListWithLimit`。新增 `isSingleLevelGlob` 本地静态方法，**不引入 fe-filesystem-s3 依赖** |
| F05 | `rename(src, dst)` 仅按字面 `/` 后缀拒绝，对无 marker 的虚目录"成功"返回但子项残留 | `rename` 入口在原有斜杠检查之外新增 `hasChildUnder(src + "/")` 探测，命中则抛 `IOException("Renaming directories is not supported in Azure Blob Storage: " + src)` |
| F06 | `renameDirectory` 默认实现依赖错误的 `exists` + `rename`，几乎不可用 | override `renameDirectory(src, dst, whenSrcNotExists)`：先按 marker + `hasChildUnder` 双重判定目录存在；不存在 → 调 `whenSrcNotExists`；存在 → 抛 `UnsupportedOperationException`（Azure Blob 无原子目录重命名；交由调用方分支处理） |
| F07 | `copyObject` + `deleteObject` 非原子；删除失败时 dst 残留且无补偿 | 拆出 `deleteObject(src)` 的 try/catch，捕获后尝试 `deleteObject(dst)` 进行补偿；补偿过程中的二次异常通过 `addSuppressed` 链入主异常并整体重抛 |
| F08 | `mkdirs(path)` 调用 `putObject` 时底层 `BlobClient.upload(..., overwrite=true)`，会把同名真实 blob 截为 0 字节 | 先 `headObject(<path>/)`：存在 → 直接返回（幂等）；404 → 走原 `putObject` 逻辑写空 marker。404 判别复用既有 `isNotFoundError(...)` |
| F09 | `AzureOutputFile.create()` 直接转发 `createOrOverwrite()`，导致每次 `create` 都是覆盖 | 拆出 `create()`：先 `headObject` 探测，存在则抛 `IOException("File already exists: " + location)`；不存在则继续既有写流程。`createOrOverwrite()` 行为不变 |
| F10 | `exists(loc)` 仅 HEAD 字面 key，对无 marker 的虚目录返回 `false`，污染上层 `renameDirectory`、预检逻辑 | override `exists(Location)`：HEAD 命中 → true；命中 404 → 退到 `hasChildUnder(uri + "/")`；其他 IOException → 重抛 |

## 关键设计点

1. **F04 单层与多层 glob 的拆分**：
   - `containsGlob` 仅识别 `*` 与 `?`，`[` / `{` 在 Azure key 中合法（与 CRITICAL 修复保持一致）。
   - `isSingleLevelGlob`：glob meta 全部出现在最后一个 segment 时返回 true。这与 HDFS `globStatus` 的语义、以及 S3 在 `listFiles` 中的分发策略对齐。
   - 单层分支额外做"路径深度过滤"——跳过 `parent + key` 中再含 `/` 的条目。这弥补了当前 `AzureObjStorage.listObjects` SPI 仅支持递归列举的不足，使语义等价于 HDFS 的单层 glob。
2. **F06 选择"明确抛 UOE"而非伪原子拷贝**：
   - Azure Blob 没有原生的目录重命名。在 SPI 层用 N 次 `copyObject + deleteObject` 模拟既无原子性、又会让中间状态对调用方不可见。
   - 给出明确异常允许上层（例如 broker-load 的提交逻辑）显式处理或上抛，避免静默成功后语义错位。
3. **F07 补偿策略**：删除 src 失败时，**最多尝试一次** `deleteObject(dst)`；任何补偿过程中的异常都通过 `Throwable.addSuppressed` 挂在主 IOException 上，保证不丢诊断信息。
4. **F08 / F09 复用 `isNotFoundError`**：本批不修复 F14（`isNotFoundError` 字符串匹配脆弱）的根因；后续 MEDIUM 组若把 `isNotFoundError` 改为类型化判断，本批两处调用点会自动受益。
5. **F10 退化路径**：先 HEAD 再 list，第二步在 HEAD 已 404 时才执行；常见场景（命中真实文件）只多花一次 HEAD 的成本。

## 单元测试

`AzureFileSystemTest` 新增 14 个 case：

- `listFiles_noGlob_delegatesToDefault`
- `listFiles_singleLevelGlob_filtersBasenamesAndSkipsMarkers`
- `listFiles_multiSegmentGlob_usesGlobListWithLimit`
- `rename_virtualDirectory_throws`
- `rename_compensatesWhenDeleteFails`
- `renameDirectory_runsWhenSrcNotExistsCallback_whenNoMarkerAndNoChildren`
- `renameDirectory_throwsUnsupported_whenChildrenPresent`
- `mkdirs_idempotent_doesNotOverwriteExistingMarker`
- `mkdirs_putsWhenMarkerMissing`
- `create_throwsIfDestinationExists`
- `create_writesWhenDestinationMissing`
- `exists_returnsTrueWhenExactKeyExists`
- `exists_returnsTrueForVirtualDirectoryWithChildren`
- `exists_returnsFalseWhenNeitherKeyNorChildren`

执行：

```
cd fe/fe-filesystem/fe-filesystem-azure
mvn test -Dmaven.build.cache.enabled=false
```

结果：`Tests run: 49, Failures: 0, Errors: 0, Skipped: 0`，0 Checkstyle 违规，BUILD SUCCESS。

## 行为变更

- `listFiles(Location)` 现可正确处理 glob 路径（与 S3、HDFS 行为对齐）。
- `mkdirs(Location)` 不再覆盖已有同名 marker / blob。
- `AzureOutputFile.create()` 在目标已存在时抛 `IOException`，不再静默覆盖。
- `exists(Location)` 对虚目录返回 true（之前为 false），影响所有依赖该返回值的上层逻辑。
- `rename(virtualDir, …)` 由"静默成功 + 子项残留"变为抛 `IOException`。
- `rename(src, dst)` 删除失败时尝试补偿删除 dst，并以包含 suppressed cause 的 IOException 重抛。
- `renameDirectory(src, dst, whenSrcNotExists)`：src 不存在时仍走 callback；存在时由"静默无操作 / 抛斜杠错误"变为明确 `UnsupportedOperationException`。

## 已知遗留

- F14（`isNotFoundError` 基于字符串匹配 404）在 MEDIUM 组处理。
- F11（`AzureFileIterator.isDirectory` 永远 false）、F12（`AzureOutputStream` 全量内存缓冲）、F15、F16 等 MEDIUM 项不在本批范围。
- F17（rename 默认覆盖 dst 的策略选择）、F18（`AzureUri` 编码）、F19/F20/F21 等 LOW 项不在本批范围。
