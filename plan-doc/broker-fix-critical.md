# Broker FE 文件系统 CRITICAL 修复总结

对应审计文档 `plan-doc/broker.md` 中编号 **#1 / #2 / #3** 的三条 CRITICAL 级别问题。

## 修复矩阵

| 编号 | 文件 | 问题 | 修复方案 |
| ---- | ---- | ---- | -------- |
| #1 | `BrokerSpiFileSystem.java` | `delete(loc, recursive=false)` 完全忽略 `recursive` 参数；Broker 服务端 `deletePath` 始终递归删除，违反 `FileSystem.delete` 契约。 | 在 `recursive=false` 分支先用 `listPath(loc.uri(), false)` 探测：若任一返回项的 path 与 `loc.uri()` 不相等，说明是非空目录，直接抛 `IOException("Directory not empty: ...")`；其余情况（空目录 / 普通文件 / 不存在）继续走原有 `deletePath` 调用。`recursive=true` 路径行为不变。 |
| #2 | `BrokerOutputFile.java` | `create()` 与 `createOrOverwrite()` 都直接以 `TBrokerOpenMode.APPEND` 打开，与 SPI 语义不符：`create` 不应覆盖已存在文件，`createOrOverwrite` 应当截断。 | 在 `BrokerOutputFile` 构造器新增 `BrokerSpiFileSystem fs` 反向引用（由 `BrokerSpiFileSystem.newOutputFile` 注入）。<br/>· `create()`：先 `fs.exists(location)`；若存在则抛 `IOException("File already exists: ...")`，否则以 `APPEND` 打开。<br/>· `createOrOverwrite()`：先 `fs.delete(location, false)`（Broker 的 FILE_NOT_FOUND 已被吞掉，不存在亦安全），再以 `APPEND` 打开，模拟截断写。<br/>两个方法的 Javadoc 都注明 exists/delete 与 open 之间的竞态窗口。 |
| #3 | `BrokerOutputStream.java` | `close()` 在 `closeWriter` 返回非 OK 状态时仅打印 `LOG.warn` 并继续把 client 归还连接池，调用方误以为写入已经持久化；同时连接池被污染。 | 非 OK 分支：保留原 warn 日志，调用 `clientPool.invalidate(endpoint, client)` 丢弃可疑客户端，再抛 `IOException("Failed to close broker writer for fd ... : ...")`。`closed` 幂等标记位原本已存在，复用即可。 |

## 关键设计点

1. **依赖现有 Thrift IDL**：`gensrc/thrift/PaloBrokerService.thrift` 中 `TBrokerOpenMode` 仅定义 `APPEND = 1`，没有 `WRITE` 截断模式。修复必须只改 `fe-filesystem-broker` 模块，因此 `createOrOverwrite` 通过 “delete + append” 模拟截断；该方案的竞态窗口已在 Javadoc 中显式注释。后续若 IDL 增加真正的 `WRITE` 模式，可一行改回。
2. **`delete` 的探测语义**：`BrokerSpiFileSystem.listPath(uri, recursive=false)` 对文件返回包含自身的单元素列表；对空目录返回空列表；对非空目录返回直接子项。判断 “是否非空目录” 用 path 不等于自身这一条件即可，避免对路径前缀做字符串拼接。
3. **连接池污染防护**：`closeWriter` 失败属于 Broker 侧状态未知场景，必须 `invalidate`，而非 `returnGood`，保持与既有 `TException` 分支一致。

## 单元测试

新增 / 调整测试均位于 `fe/fe-filesystem/fe-filesystem-broker/src/test/java/org/apache/doris/filesystem/broker/`：

- `BrokerSpiFileSystemTest`
  - `delete_nonRecursive_throwsWhenDirectoryNotEmpty`
  - `delete_nonRecursive_succeedsWhenDirectoryEmpty`
  - `delete_nonRecursive_succeedsWhenLocationIsFile`
  - `delete_recursive_alwaysCallsDeletePath`
- `BrokerOutputFileTest`（新增）
  - `create_throwsIfFileExists`
  - `create_writesWhenMissing`
  - `createOrOverwrite_usesWriteMode`（断言先调用 delete 再以 APPEND 打开）
- `BrokerOutputStreamTest`（新增）
  - `close_throwsOnNonOkStatus`（断言抛异常并 invalidate 客户端）
  - `close_isIdempotent`
  - `close_returnsClientToPoolOnSuccess`

执行结果：`mvn test -Dmaven.build.cache.enabled=false`，`Tests run: 37, Failures: 0, Errors: 0, Skipped: 0`，无 checkstyle 违规。

## 行为变更

- `BrokerSpiFileSystem.delete(loc, false)` 对非空目录现在抛 `IOException` 而不再静默递归删除；调用方若依赖旧的 “永远递归” 行为需显式传 `recursive=true`。
- `BrokerOutputFile.create()` 在文件已存在时抛 `IOException("File already exists: ...")`，原先会静默 APPEND。
- `BrokerOutputFile.createOrOverwrite()` 现在会先发起一次 `deletePath` RPC（路径不存在时无副作用），再以 APPEND 打开。
- `BrokerOutputStream.close()` 在 `closeWriter` 非 OK 时抛 `IOException`，原先只记 warn；同时把可疑 client invalidate 出连接池。

## 已知遗留

- 由于 Thrift IDL 限制，`createOrOverwrite` 的 “截断” 不是原子操作，存在与并发写者交错的可能；属于上游 Broker 协议层面的限制，待 IDL 引入真正的 `WRITE` 模式后即可消除。
- 余下 HIGH / MEDIUM / LOW / NIT 修复在后续提交中处理。
