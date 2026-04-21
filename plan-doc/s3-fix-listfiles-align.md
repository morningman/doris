# S3 listFiles 单层 glob 与 HDFS 对齐修订

本次修订不在 `plan-doc/s3.md` / `hdfs.md` 原审计列表内，源于 HDFS MEDIUM 批次 review 时发现 S3 与 HDFS `listFiles(glob)` 行为不一致：

| FS | 旧行为 |
|----|------|
| HDFS | `globStatus` 按目录段评估，单层 glob 仅返回父目录下匹配 basename 的非目录项 |
| S3 | 任意 glob 一律走 `globListWithLimit` 的递归前缀扫描 |

为消除"同一表达式跨 FS 含义不同"的隐患，按用户决策（C1 方案）将 S3 的**单层 glob**对齐 HDFS；**跨段 glob**因 S3 扁平模型代价过大，明确保留递归实现。

## 修复矩阵

| 区域 | 问题 | 修复策略 |
|------|------|---------|
| `S3FileSystem.listFiles(glob)` | 单层 glob 行为与 HDFS 不一致 | 新增 `isSingleLevelGlob` 判定与 `listFilesSingleLevelGlob` helper：在父前缀做 delimiter-mode 列举，basename 用 `globToRegex` 匹配，过滤目录标记 |
| 同上 | 跨段 glob 在 S3 扁平 namespace 下没有低成本的"按段递归"路径 | 显式保留对 `globListWithLimit` 的调用；Javadoc 注明分支选择规则 |

## 关键设计点

- `isSingleLevelGlob`：仅当 basename 含 `*`/`?` 且父路径不含 glob 元字符时返回 true。
- 复用既有 `globToRegex`（与 `globListWithLimit` 同一套语义），避免引入第二种 glob 解释。
- 单层分支不再次排序，沿用 `listObjectsNonRecursive` 返回的 S3 字典序。
- 仅 `*`/`?` 视为 glob 元字符，与既有 `containsGlob` 一致；`[`、`{` 在 S3 key 中合法不应误判。

## 单元测试

`S3FileSystemTest` 新增 / 调整：

- `listFiles_singleLevelGlobMatchesBasenameAndSkipsSubdirs`
- `listFiles_singleLevelGlobReturnsEmptyWhenNoMatches`
- `listFiles_singleLevelGlobIsNonRecursive`
- `listFiles_singleLevelGlobPaginatesAcrossContinuationTokens`
- `listFiles_crossSegmentGlobStillRecursive`（spy 验证仍走 `globListWithLimit`）
- `isSingleLevelGlob_lastSegmentWildcardIsTrue`
- `isSingleLevelGlob_noWildcardIsFalse`
- `isSingleLevelGlob_wildcardInNonLastSegmentIsFalse`
- `isSingleLevelGlob_wildcardInBothSegmentsIsFalse`

原两个把"单层 glob 当作递归 glob"测试改名为 `listFiles_crossSegmentGlobRoutesThroughRecursiveScan` / `listFiles_crossSegmentGlobBranchRequestsUnlimitedByteAndFileCount`，并替换 URI 为跨段 glob，准确锁定保留行为。

运行结果：

```
S3FileSystemTest:  Tests run: 70, Failures: 0, Errors: 0, Skipped: 0
fe-filesystem-s3 模块: 134, 0, 0, 0
Checkstyle violations: 0
BUILD SUCCESS
```

## 行为变更

- 调用方传入"单层 glob"（如 `s3://b/dir/file*.parquet`）时，S3 不再越级返回 `s3://b/dir/sub/...` 下的对象；行为与 HDFS 同名调用一致。
- 跨段 glob（如 `s3://b/a/*/b.parquet`）保持原有递归行为；调用方若依赖此语义不受影响。

## 已知遗留

- 跨段 glob 的"按段递归"对齐留作未来工作；如需完整一致，需要按 `CommonPrefixes` 逐段下钻，会显著增加 RPC 数。
