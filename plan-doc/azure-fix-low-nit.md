# Azure FileSystem LOW + NIT 修复总结

本文档对应 `plan-doc/azure.md` 中标记为 LOW (F17–F21) 与 NIT (F22–F23)
的 7 个问题，全部位于 `fe/fe-filesystem/fe-filesystem-azure/`。基线为
MEDIUM 修复后的提交 `9566d9dbdc9`。

## 修复矩阵

| 编号 | 严重度 | 问题 | 处理方式 |
| --- | --- | --- | --- |
| F17 | LOW | `rename(src, dst)` 静默覆盖 dst | `rename` 入口在虚目录检查之后增加 `headObject(dst)` 探测；存在则抛 `IOException("Rename destination already exists: " + dst)`。Javadoc 显式声明 no-clobber 策略。比 audit 建议的"仅文档"更保守 |
| F18 | LOW | `AzureUri` 不做百分号编/解码、不校验 container 名称 | `parse()` 先剥离 `?…` / `#…`；container 名按 Azure 规则正则校验（不合法即抛 IO）；三个 sub-parser 都对 key 做 `URLDecoder.decode(..., UTF-8)`。`toString()` 用 `URLEncoder.encode(...)` 后将 `+` 还原为 `%20`、`%2F` 还原为 `/`，保留路径分隔符语义 |
| F19 | LOW | `deleteObjectsByKeys` 把异常压成字符串，丢失 cause chain | 同时收集 per-key 摘要与原始 `Throwable`；最终抛 `IOException` 时通过 `addSuppressed` 把所有原始异常挂上去，便于诊断 |
| F20 | LOW | `abortMultipartUpload` 仅打日志、staged blocks 实际等 7 天 GC | 新策略：先 `headObject(remotePath)`；若已有 committed blob → 仅 log warning 跳过（避免破坏既有内容）；若 404 → 调 `commitBlockList(emptyList())` 后立即 `delete()`，原子性丢弃所有 staged blocks。任何二次异常 best-effort 吞掉并 log |
| F21 | LOW | `AzureFileSystemProvider.supports(...)` 仅识别 `blob.core.windows.net` | 新增 `AZURE_BLOB_HOST_SUFFIXES` 常量列表，覆盖 Public / China / USGov / Germany 四个主权云后缀；`supports()` 遍历 `endpoint.contains(suffix)` |
| F22 | NIT | `AzureFileIterator.close()` 仅 `// no-op` | 注释扩展为说明：当前页已全量读入内存，无需释放底层资源 |
| F23 | NIT | `AzureObjStorage.close()` 注释模糊 | 注释扩展为说明：`BlobServiceClient` 共享 JVM 级 HTTP 连接池，主动关闭会拆掉共享池；故此处显式 no-op |

## 关键设计点

1. **F17 选择"refuse"而非"document only"**：silent overwrite 是 Doris/HDFS 风格里典型的"长期吃亏"型缺陷。在 SPI 层强制拒绝，让需要覆盖的调用方显式 `delete + rename`。
2. **F18 编/解码边界**：
   - container 部分**不**做 URL 解码（Azure 命名规则本身只允许 `[a-z0-9-]`，无可解码字符）。
   - key 解码后再使用，避免下游 SDK 把 `%20`/`+` 当作字面字符传出去。
   - `toString` 重新做了"URL-encode → 还原 `+` 与 `%2F`"的两步处理，保证在 path-style URL 里目录分隔符仍是 `/`、空格按 RFC3986 编为 `%20`。
   - container 校验保留 1 字符的下限以兼容现有测试 fixture（`wasbs://c@a.host/...`）；与 Azure 实际服务端 ≥3 字符要求略宽，已在文档中标注。
3. **F19 双信道诊断**：摘要字符串便于人类读，suppressed 异常便于排查 SDK 层细节，两者并存。
4. **F20 安全前置**：核心顾虑是 "commit-empty 会覆盖已 committed 内容"。HEAD 探测把这个不可逆破坏限制为只发生在"实际只剩 staged blocks"的安全场景。在生产环境 Doris 调用 `abortMultipartUpload` 都是因为 `AzureOutputStream` 上传失败，此时既有内容确实不存在，HEAD 返回 404 → 走清理路径；如果用户用同名做了别的事 → 我们什么都不动，更安全。
5. **F21 用 `contains` 而非 `endsWith`**：`endpoint` 里可能带 `https://` 前缀和 `/path` 后缀，`endsWith` 失败率高；`contains` 与现有 `supports()` 行为一致，且四个后缀已唯一。

## 单元测试

- `AzureUriTest` 新增 5 个：
  - `parse_decodesPercentEncodedKey`
  - `parse_stripsQueryAndFragment`
  - `parse_rejectsInvalidContainerName`
  - `parse_emptyKeyWithTrailingSlash`
  - `toString_percentEncodesKey_keepsSlashes`
- `AzureFileSystemTest` 新增 1 个 + 调整 1 个：
  - `rename_throwsIfDestinationExists`
  - `rename_compensatesWhenDeleteFails` 同步加 `headObject(dst) → FileNotFoundException` 的桩，配合新的 no-clobber 守卫
- `AzureObjStorageExtensionTest` 新增 3 个：
  - `deleteObjectsByKeys_attachesPerKeyExceptionsAsSuppressed`
  - `abortMultipartUpload_safeNoopWhenCommittedBlobExists`
  - `abortMultipartUpload_commitsEmptyAndDeletesWhenNoCommittedBlob`
- 新增 `AzureFileSystemProviderTest`：
  - `supports_recognizesAccountNameProperty`
  - `supports_returnsFalseWhenNoAccountAndNoEndpoint`
  - `supports_returnsFalseForUnknownEndpointSuffix`
  - `supports_recognizesSovereignCloudEndpoints`（参数化覆盖 4 个后缀）

执行：

```
cd fe/fe-filesystem/fe-filesystem-azure
mvn test -Dmaven.build.cache.enabled=false
```

结果：`Tests run: 74, Failures: 0, Errors: 0, Skipped: 0`，0 Checkstyle 违规，BUILD SUCCESS。

## 行为变更

- `rename(src, dst)` 在 dst 已存在时由"静默覆盖"改为抛 `IOException`。**对调用方可见**，需要覆盖的场景必须显式先 `delete(dst)`。
- `AzureUri.parse(...)` 对非法 container 抛 `IOException`，对带 `?` / `#` / 百分号编码的 path 行为更接近 RFC 3986。
- `AzureUri.toString()` 输出会对特殊字符做百分号编码；既有调用拼接处（用作 SDK 输入）受益，但若有调用方依赖 `toString()` 的字面形式做字符串 join 需注意。
- `deleteObjectsByKeys(...)` 抛出的 `IOException` 现在带 `getSuppressed()`，日志体积可能略涨。
- `abortMultipartUpload(...)` 在能安全清理时会真正释放 staged blocks（不再仅依赖 7 天 GC），节省存储。
- `AzureFileSystemProvider.supports(...)` 现在能识别 Azure China / USGov / Germany 主权云端点。

## 已知遗留 / Azure 模块完成情况

至此 `plan-doc/azure.md` 中 F01–F23 全部 23 条 finding 已按 CRITICAL → HIGH → MEDIUM → LOW + NIT 四个批次完成修复，Azure 模块本轮审计闭环。
