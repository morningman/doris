# Azure FileSystem MEDIUM 修复总结

本文档对应 `plan-doc/azure.md` 中标记为 MEDIUM 的 6 个问题（F11–F16），
全部位于 `fe/fe-filesystem/fe-filesystem-azure/`。基线为 HIGH 修复后的提交
`5ea85a94b4a`。

## 修复矩阵

| 编号 | 问题 | 处理方式 |
| --- | --- | --- |
| F11 | `AzureFileIterator` 把每个条目都当作 `isDirectory=false`，目录 marker（key 以 `/` 结尾）会被当作 0 字节文件返回 | `fetchNextPage()` 直接跳过 key 以 `/` 结尾的条目；与 HDFS / S3 的 `list()` 行为对齐，下游 `listFiles` / `listDirectories` 不再被 marker 污染 |
| F12 | `AzureOutputStream` 把整段写入累积在 `ByteArrayOutputStream` 中，多 GB 写入易 OOM；已有的 multipart SPI 完全未被使用 | 重写为流式 multipart：8 MiB 复用 `byte[]` buffer + 懒生成 `UUID` uploadId；buffer 写满则 `uploadPart` 上传一段；`close()` 时若一段都没传过走单次 `putObject` 快路径，否则刷出尾段并 `completeMultipartUpload`；首段之后任何 IOException 都会触发 best-effort `abortMultipartUpload`，并把次生异常通过 `addSuppressed` 链接到主异常；`flush()` 显式 no-op（每次 stageBlock 成本过高） |
| F13 | `AzureSeekableInputStream` 关闭后在 EOF 处的 `read` 不抛异常 | 复核后确认 `checkOpen()` 已是两个 `read` 重载的第一行（关闭后即抛 `IOException`）。本次只加测试 `read_afterClose_throwsEvenAtEndOfFile` 锁定行为，避免回归 |
| F14 | `isNotFoundError` 用 `getMessage().contains("404")` 字符串匹配，对任何含 "404" 字样的无关异常都会误判 | 全面改为类型化判断：`return e instanceof FileNotFoundException;`。在改动前确认了 `AzureObjStorage` 中所有可能 404 的入口（`headObject`/`openInputStream*`/`headObjectLastModified`）都已抛 `FileNotFoundException`；`deleteObject` 自身吞 404，对外不可见 |
| F15 | `AzureObjStorage.listObjects` 通过 `pagedBlobs.iterableByPage().iterator().next()` 取第一页，遇到空 `PagedIterable`（例如 SDK 对畸形 continuation token 的响应）会抛 `NoSuchElementException` | 改为先 `hasNext()` 判空：空则返回空 `RemoteObjects`；并加防御性 `catch (NoSuchElementException) → IOException` 进一步兜底 |
| F16 | `AzureFileIterator.next()` 不调用 `hasNext()`，越界时抛 `IndexOutOfBoundsException` 而非 `NoSuchElementException` | `next()` 入口先调用 `hasNext()`；若返回 false 抛 `NoSuchElementException("AzureFileIterator exhausted")` |

## 关键设计点

1. **F12 的小写优化路径**：未跨过 8 MiB 的写入仍走单次 `putObject`，避免对小对象支付多版本 `commitBlockList` 的额外往返；只有真正 ≥ 1 个 staged part 之后的失败才会触发 `abortMultipartUpload`。
2. **F12 的 `flush` 语义**：刻意保持 no-op，并在 Javadoc 中说明——在 Azure 场景里把每个 `flush` 都映射成 `stageBlock` 不仅无业务价值，还会显著膨胀块数和提交时间。
3. **F14 的契约确认**：在 `AzureObjStorage` 中扫了一遍 404 入口（`headObject`、`openInputStream`、`openInputStreamAt`、`headObjectLastModified`），都已抛 `FileNotFoundException`，因此把 `isNotFoundError` 收紧为类型判定不会破坏任何调用方。`deleteObject` 内部已经吞 404，外部不可见。
4. **F11 的"跳过"策略**：与 audit 推荐一致——不把 marker 转换成 `isDirectory=true` 透出，而是直接过滤。这样 `list()` / `listFiles` / `listDirectories` 三条接口都不会被 marker 干扰；后续若需要让 `listDirectories` 真正返回目录，可以在 `listFilesSingleLevelGlob` 之类的位置基于 hierarchy listing 重新实现，与本次修复正交。
5. **F15 的双层兜底**：先 `hasNext()` 显式分支处理空页；再加 `catch (NoSuchElementException)` 防御任何潜在 SDK 实现差异。两层都映射到契约友好的返回值。
6. **F13 的"无改动 + 加测"**：审计作者自己在文档里就给出了"on review OK"的结论，这里通过新增 `read_afterClose_throwsEvenAtEndOfFile` 用例把行为变成不可回归的合约。

## 单元测试

- `AzureFileSystemTest` 新增 8 个 case：
  - `list_skipsDirectoryMarkerEntries`
  - `next_throwsNoSuchElementWhenExhausted`
  - `read_afterClose_throwsEvenAtEndOfFile`
  - `outputStream_smallPayload_usesSinglePut`
  - `outputStream_largePayload_usesMultipart`
  - `outputStream_uploadFailure_callsAbort`
  - `isNotFoundError_returnsTrueForFileNotFoundException`
  - `isNotFoundError_returnsFalseForGenericIOExceptionWith404Message`
- `AzureObjStorageExtensionTest` 新增 1 个 case：
  - `listObjects_emptyPagedResponseIterator_returnsEmptyResultsAndDoesNotThrow`
- 既有 `delete_nonRecursive_silentlyAcceptsMissingTarget` 配合 F14 的契约调整，把 stub 从"任意带 404 字样的 IOException"改为 `FileNotFoundException`。

执行：

```
cd fe/fe-filesystem/fe-filesystem-azure
mvn test -Dmaven.build.cache.enabled=false
```

结果：`Tests run: 58, Failures: 0, Errors: 0, Skipped: 0`，0 Checkstyle 违规，BUILD SUCCESS。

## 行为变更

- `AzureFileSystem.list(Location)` 不再返回 directory marker（key 以 `/` 结尾的 0 字节 blob）。
- `AzureFileIterator.next()` 越界时抛 `NoSuchElementException`，而不再是 `IndexOutOfBoundsException`。
- `isNotFoundError` 不再匹配字符串 "404"；调用 `objStorage.*` 时若需要 404 语义，必须以 `FileNotFoundException` 表达（`AzureObjStorage` 已满足该约束）。
- `AzureOutputStream` 大对象写入由"全量内存缓冲 + 单次 PUT"切换为"8 MiB 分段 + commitBlockList"；`close()` 失败时尽力 `abortMultipartUpload`，避免泄漏未提交块。
- `AzureObjStorage.listObjects` 在空页 / 异常分页迭代器场景下返回空 `RemoteObjects`，而不是抛 `NoSuchElementException`。

## 已知遗留

- F17（rename 默认覆盖 dst 的策略选择）、F18（`AzureUri` 编码 / 特殊字符）、F19（`deleteObjectsByKeys` 异常诊断弱）、F20（`abortMultipartUpload` 主动 no-op + 日志）、F21（`AzureFileSystemProvider` 对 sovereign cloud endpoint 不识别）等 LOW 项不在本批范围。
- F22（`AzureFileIterator.close()` 显式注释）、F23（`AzureObjStorage.close()` 注释）等 NIT 项不在本批范围。
- 关于 `AzureOutputStream` 的更激进改造（例如把多 part 上传改为后台并发、提供 `flush=>uploadPart` 的策略开关）保留为后续 perf 工作。
