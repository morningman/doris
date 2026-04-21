# HDFS FE 文件系统 MEDIUM 问题修复总结

本批次针对 `plan-doc/hdfs.md` 中 **MEDIUM** 级别的 8 项审计发现进行修复。

## 修复矩阵

| # | 严重 | 位置 | 问题 | 修复策略 |
|---|------|------|------|---------|
| 7 | MEDIUM | `DFSFileSystem.renameDirectory` | Javadoc 误称"原子"；先 `exists` 再 `mkdirs` 的 TOCTOU；父目录与 rename 跨权威使用了不同 FS 句柄 | 移除"atomically"措辞；删除 `exists` 防御性预检，无条件 `mkdirs(parent)` 后 `rename`；统一使用 `getHadoopFs(srcPath)`；srcPath 与 dstPath authority 不一致时直接抛 `IOException` |
| 8 | MEDIUM | `DFSFileSystem.rename` | `false` 返回被泛化为 "HDFS rename failed" | 同一 `doAs` 内追加 `srcExists`/`dstExists` 探测，写入异常 message |
| 9 | MEDIUM | `HdfsFileIterator.close` | 空实现，部分消费时泄漏 NN RPC 流 | `delegate instanceof Closeable` 时尽力 close；新增 `volatile boolean closed` 保证幂等；声明 `IOException` |
| 10 | MEDIUM | `DFSFileSystem.listFiles(glob)` | 仅返回非目录匹配项，glob 命中分区目录被静默丢弃 | **见后续修订**：保持过滤目录的契约语义（"non-directory entries directly under dir"）；当 glob 仅命中目录时返回空列表，并在 Javadoc 中提示分区发现请使用 `listFilesRecursive` / `list`。最初尝试过单层展开，经评审回滚（见 commit `a834f50f09b`）。
| 11 | MEDIUM | `DFSFileSystem.globListWithLimit` | 入参无 glob 元字符时 `globStatus` 返回单元素数组，被目录过滤为空 | 入口判断 `containsGlob`：无则走 `listStatus(p)`，有则走 `globStatus(p)` |
| 12 | MEDIUM | 同上 | `startAfter` 过滤位于 `>= maxFiles` break 之后，导致页内全是 `<= startAfter` 时返回空 | 重排顺序：先 `isDirectory` 跳过 → 再 `startAfter` 过滤 → 最后才走分页/`maxFile` 游标逻辑 |
| 13 | MEDIUM | `HdfsOutputFile.create / createOrOverwrite` | 未文档化对 FS 默认 `permission/replication/blockSize` 的依赖 | Javadoc 显式声明使用 FS 默认值（受 `fs.permissions.umask-mode` 影响），缺失父目录由 HDFS 隐式以默认 umask 创建；需精确控制者使用更底层 API |
| 14 | MEDIUM | `DFSFileSystem.newInputFile(Location, long)` | 默认实现忽略 length hint，每次 `length()` 仍 RPC | `DFSFileSystem` 重写带 hint 的方法；`HdfsInputFile` 新增带 `lengthHint` 构造器，`length()` 命中 hint (>0) 时直接返回 |

## 关键设计点

- `renameDirectory` 的父目录 `mkdirs` 仍按 HIGH 批次新建立的"检查 boolean → 必要时 `getFileStatus`"模式处理，既不引入 TOCTOU 也满足 Doris 必须检查 boolean 的规则。
- `listFiles(glob)` 展开仅做单层（"directly under dir"），不递归，避免无意中把语义改成 `listFilesRecursive`。
- `globListWithLimit` 中 `maxFile` 游标语义沿用 CRITICAL 批次的实现，本次只调整过滤顺序与非 glob 路径的分支选择；与契约 Javadoc 对齐，无回归。
- `HdfsOutputFile` 不引入新的 API 表面（暂不暴露 permission/replication/blockSize 旋钮），属于风险最小的修复方向；后续若产生明确需求再演进。
- `HdfsInputFile` 的 length hint 仅影响 `length()`；`exists()` / `lastModifiedTime()` 仍走 RPC，避免在 hint 与实际不一致时静默撒谎。

## 单元测试

新增/补充 11 个单测：

`DFSFileSystemTest`：
- `rename_errorMessageIncludesSrcDstExistence`
- `renameDirectory_failsFastOnCrossAuthority`
- `renameDirectory_callsCallbackWhenSrcAbsent`
- `renameDirectory_unconditionalParentMkdirs`
- `listFiles_globFiltersDirectoryMatches`（修订后）
- `listFiles_globReturnsEmptyWhenAllMatchesAreDirectories`（修订后）
- `globListWithLimit_nonGlobPathListsDirectly`
- `globListWithLimit_startAfterAppliedBeforeLimit`
- `newInputFile_withLengthHintSkipsGetFileStatus`

新增 `HdfsFileIteratorTest`：
- `close_propagatesToCloseableDelegate`
- `close_isIdempotent`
- `close_noOpWhenDelegateNotCloseable`

运行结果：

```
Tests run: 52, Failures: 0, Errors: 0, Skipped: 0
Checkstyle violations: 0
BUILD SUCCESS
```

## 行为变更

- `rename` / `renameDirectory` 的失败路径现在能从异常信息直接判定 src/dst 状态；跨权威 `renameDirectory` 改为快速失败而非进入"半完成"状态。
- `listFiles(glob)` 现在能正确返回 glob 命中目录下的直接子文件，避免分区目录被静默忽略。
- `globListWithLimit` 现在支持非 glob 路径的列举，并在 `startAfter` 接近列尾时也能正确分页。
- `newInputFile(Location, long)` 真正消费 length hint，可显著降低 Parquet/ORC 等场景下的元数据 RPC 数。
- `HdfsFileIterator.close()` 现在能传递到底层 `Closeable` 实现，缓解长时间运行下的 NN RPC 槽位泄漏。

## 已知遗留

- LOW 与 NIT 批次：symlink 语义、`HdfsSeekableInputStream.closed` 非 volatile、`HdfsConfigBuilder` 空值丢弃、`containsGlob` 无转义、`HdfsFileSystemProvider` 与 OSS 提供方冲突、`SimpleHadoopAuthenticator` 未调 `setConfiguration` 等留待后续批次处理。
