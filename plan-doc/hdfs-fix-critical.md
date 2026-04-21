# HDFS FE 文件系统 CRITICAL 问题修复总结

本批次针对 `plan-doc/hdfs.md` 中 **CRITICAL** 级别的 1 项审计发现进行修复。

## 修复矩阵

| # | 严重程度 | 位置 | 问题 | 修复策略 |
|---|---------|------|------|---------|
| 1 | CRITICAL | `DFSFileSystem.globListWithLimit` | `maxFile` 被设置为返回页最后一条记录的 URI，未遵循"分页游标"语义 | 循环内跟踪"已加入结果的最后一个匹配键"；命中 `maxFiles`/`maxBytes` 限制后继续向后扫描，将第一个未入页的匹配键作为 `maxFile` 游标；若后续无匹配键，则 `maxFile` 保持为页内最后一条；无任何匹配时为 `""` |

## 关键设计点

- 与已在 S3 MEDIUM 批次中敲定的 `FileSystem.globListWithLimit` / `GlobListing.maxFile` 契约保持完全一致：
  - 当页面被限额截断且后续仍有匹配键：`maxFile` 指向被截断后的下一匹配键（可作为下一次 `startAfter` 使用）。
  - 当全量匹配集已被返回：`maxFile` 等于页内最后一条匹配键。
  - 无任何匹配：`maxFile` 为空字符串。
- 仅调整 `globListWithLimit` 方法，未触及同方法涉及的 MEDIUM 级问题（#11 非 glob 路径入参、#12 `startAfter` 顺序 bug），保持最小化改动；留待 MEDIUM 批次处理。
- 未修改 fe-filesystem-api 模块契约，S3 批次已将 Javadoc 更新到位，HDFS 实现只需对齐。

## 单元测试

新增 3 个单测 (`DFSFileSystemTest`)：

- `globListWithLimit_maxFileIsNextCursorWhenPageLimitHit`：glob 命中 10 个文件，`maxFiles=3` → 页内 3 条，`maxFile` 指向第 4 个文件 URI。
- `globListWithLimit_maxFileIsLastKeyWhenListingExhausted`：glob 命中 3 个文件，`maxFiles=10` → 页内 3 条，`maxFile` 为最后一条的 URI。
- `globListWithLimit_maxFileIsEmptyWhenNoMatches`：glob 无匹配 → 页空且 `maxFile == ""`。

运行结果：

```
Tests run: 12, Failures: 0, Errors: 0, Skipped: 0    (DFSFileSystemTest)
Tests run: 36, Failures: 0, Errors: 0, Skipped: 0    (fe-filesystem-hdfs 全模块)
Checkstyle violations: 0
BUILD SUCCESS
```

## 行为变更

- 上层分页调用方在触顶分页时可拿到真正的"下一个键"作为续传游标，避免重复返回最后一条或漏读紧邻下一条的数据。
- 未触顶时语义不变（仍为最后一条匹配键的 URI），与 S3 模块一致。

## 已知遗留

- `globListWithLimit` 中的 MEDIUM 级问题（非 glob 入参路径形状、`startAfter` 排序 bug）将在 MEDIUM 批次统一处理。
