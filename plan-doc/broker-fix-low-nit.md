# Broker FE 文件系统 LOW + NIT 修复总结

对应审计文档 `plan-doc/broker.md` 中编号 **#13 / #14 / #15 / #16 / #17** 的五条 LOW + NIT 级别问题。本次合并提交。

## 修复矩阵

| 编号 | 文件 | 问题 | 修复方案 |
| ---- | ---- | ---- | -------- |
| #13 LOW | `BrokerSpiFileSystem.java` | `listFilesRecursive` 走默认实现，每层目录一次 RPC（O(depth)）。 | 重写 `listFilesRecursive(Location)`：直接调用现有 `listPath(uri, true)` 一次拉全部，过滤 `isDir`，返回 `FileIterator`。 |
| #14 LOW | `BrokerSpiFileSystem.java` | `globListWithLimit` 默认抛 UOE。 | 重写：`listPath(uri, false)` 单次拉取后在内存里按 glob 表达式匹配 + 应用 limit；游标语义对齐 S3/Azure：limit 命中 → `maxFile` 取下一个匹配项；耗尽 → `maxFile` 为最后一个匹配；空集 → `maxFile=""`。过滤目录。 |
| #15 LOW | `BrokerInputStream.java#close` | `closeReader` 非 OK 仅 warn，可疑客户端被 `returnGood` 回池。 | 保留 warn，调用 `clientPool.invalidate(endpoint, client)` 替代 `returnGood`；HIGH 阶段已设置的 `closed` 标志保证二次 close 仍 no-op；method 仍不抛（关闭语义不变）。 |
| #16 LOW | `BrokerClientPool.java` | `maxWaitMillis=500` 太短易超时；`maxTotalPerKey=-1` 可耗尽 broker。 | 引入常量 `DEFAULT_MAX_WAIT_MILLIS=5000`、`DEFAULT_MAX_TOTAL_PER_KEY=64`，构造器使用，并加注释说明取值理由。配置外露不在本次范围内。 |
| #17 NIT | `BrokerSpiFileSystem.java` | `LOG` 字段从未使用。 | 删除字段 + `LogManager`/`Logger` import。 |
| #17 NIT | `BrokerSpiFileSystem.java` | `brokerParams()` 访问器是否冗余。 | 保留 —— 已被 `BrokerSpiFileSystemTest` 引用，且属于内部 API。 |
| #17 NIT | `BrokerClientFactory.java#create` | 抛裸 `Exception`，丢失诊断类型。 | 父类签名要求 `throws Exception`，无法收紧；改为 `LOG.warn(..., e)` 输出 cause，并显式抛 `TTransportException` 以保留类型与 cause 链。 |
| #17 NIT | `BrokerInputStream.fillBuffer` 重复分配 | 审计明示非正确性问题。 | 跳过。 |
| #17 NIT | `BrokerFileSystemProvider.extractBrokerParams` 双拷贝 | 审计明示 trivial。 | 跳过，避免引入风险。 |

## 关键设计点

1. **`listFilesRecursive` 单 RPC**：依赖 IDL 已暴露的 `recursive=true`，在深目录树上将延迟由 O(depth) 降为 O(1)；过滤逻辑保留与 `listFiles` 一致的 `FileEntry` 转换。
2. **`globListWithLimit` 游标语义对齐**：与 `S3FileSystem` / `AzureFileSystem` 的实现一致，避免新接入的调用方在不同存储下看到不一致 `maxFile`。
3. **`closeReader` 非 OK 处理**：HIGH 阶段已让 `close()` 幂等且 broker-side 写入路径走 invalidate，本次将读取路径补齐 invalidate。
4. **`BrokerClientFactory.create` 类型化**：commons-pool2 的 `BaseKeyedPooledObjectFactory.create` 强制 `throws Exception`，无法在签名层面收紧；通过 `TTransportException` 保留类型与 cause，pool 包装时仍能定位根因。

## 单元测试

- `BrokerSpiFileSystemTest`
  - `listFilesRecursive_returnsAllFilesInOneRpc`
  - `listFilesRecursive_filtersDirectories`
  - `listFilesRecursive_emptyResultWhenPathMissing`
  - `globListWithLimit_returnsAllWhenUnderLimit`
  - `globListWithLimit_truncatesAtLimitAndSetsMaxFile`
  - `globListWithLimit_filtersDirectories`
  - `globListWithLimit_returnsEmptyMaxFileWhenNoMatches`
- `BrokerInputStreamTest`
  - `close_invalidatesClientOnCloseReaderNonOk`
- `BrokerClientPoolTest`（新增）
  - 反射断言 `maxTotalPerKey=64` / `maxWaitMillis=5000` / `testOnBorrow=true`

执行：`mvn test -Dmaven.build.cache.enabled=false` → `Tests run: 65, Failures: 0, Errors: 0, Skipped: 0`，0 checkstyle 违规。

## 行为变更

- `listFilesRecursive` 改为单次 RPC，深目录访问明显加速；语义与默认实现一致。
- `globListWithLimit` 由 UOE 变为可用，`maxFile` 游标语义对齐 S3/Azure。
- `BrokerInputStream.close()` 在 `closeReader` 非 OK 时不再把可疑 client 放回池。
- `BrokerClientPool` 默认 `maxWaitMillis=5s`、`maxTotalPerKey=64`（之前 500ms / 无限）。

## 已知遗留

- `BrokerClientPool` 的两个新默认尚未对外配置化；如需场景化调优，后续按需求加 conf。
- `BrokerInputStream.fillBuffer` 的重复分配、`BrokerFileSystemProvider.extractBrokerParams` 的双拷贝按审计建议保持原状。
- 至此 `plan-doc/broker.md` 的 17 条 finding 全部闭环。
