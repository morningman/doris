# Broker FE 文件系统 MEDIUM 修复总结

对应审计文档 `plan-doc/broker.md` 中编号 **#9 / #10 / #11 / #12** 的四条 MEDIUM 级别问题。

## 修复矩阵

| 编号 | 文件 | 问题 | 修复方案 |
| ---- | ---- | ---- | -------- |
| #9 | `BrokerSpiFileSystem.java#mkdirs` | 静默 no-op；`exists(dir)` 在写入前依旧返回 false，破坏 “以 mkdirs 作 barrier” 的调用方语义。 | 直接抛 `UnsupportedOperationException("Broker filesystem does not support empty directory creation: ...")`；Javadoc 说明对象存储无空目录，broker 在 `openWriter` 时自动建父目录，不存在 mkdir RPC。 |
| #10 | `BrokerSpiFileSystem.java#rename` / 新 `renameDirectory` 重写 | `rename` 不区分 `FILE_NOT_FOUND` 与其他错误；默认 `renameDirectory` 走 `exists` + `rename` 形成 TOCTOU 竞态。 | `rename(...)` 在 opStatus 为 `FILE_NOT_FOUND` 时抛 `FileNotFoundException("Source path does not exist: ...")`；其他非 OK 仍抛 `IOException`。新增 `renameDirectory(src, dst, whenSrcNotExists)` 重写：直接 `rename`，捕获 `FileNotFoundException` 后跑回调，消除竞态。 |
| #11 | `BrokerClientFactory.java#create`（**未改动**） | 使用裸 `TSocket`，审计建议改为 `TFramedTransport`。 | **决议：维持现状**。fe-core 通过 `ClientPool.brokerPool`（`fe/fe-core/.../ClientPool.java:80-81`）使用 3 参 `GenericPool(...)`，在 `fe/fe-core/.../GenericPool.java:54-56` 默认 `isNonBlockingIO=false`，再于 `GenericPool.java:210-212` 直接 `new TSocket(...)`，没有 `TFramedTransport` 包装。当前模块若改为 framed，与生产 broker 服务端 `TThreadPoolServer + TBinaryProtocol` 协议会发生不兼容。本次保持与 fe-core 对齐。 |
| #12 | `BrokerClientFactory.java#validateObject` | 仅检查 `transport.isOpen()`；半关闭 socket 仍报 open，`testOnBorrow` 形同虚设。 | 采用方案 (a)：调用 IDL 中已存在的 `ping` RPC（`gensrc/thrift/PaloBrokerService.thrift:267-268`）。新增包级 `pingBroker(client, transport)` 辅助方法，临时把 `TSocket` 超时调到 5s 让半坏对端快失败、调用结束后还原为 `SOCKET_TIMEOUT_MS`；任意 `TException`/非 OK opStatus 返回 `false`。`validateObject` 先做 `transport.isOpen()` 短路，再做 ping。 |

## 关键设计点

1. **`mkdirs` 选 UOE 而非默默成功**：与审计共识一致 —— 由调用方在不支持的存储上看到失败，比让后续逻辑读到不存在的目录更安全。需要 mkdirs 作 barrier 的代码可改写为 “写入哨兵文件”。
2. **`renameDirectory` 重写消除竞态**：默认实现 `exists(src) → rename(src,dst)` 中间窗口允许 src 被并发删除；现在直接调 `rename`，异常分支负责 callback，并遵循契约 “src 缺失时只跑 callback 不抛”。
3. **F11 与生产兼容性**：审计文档预留了 “fe-core 用什么就用什么” 的口子；本次走查发现 fe-core 端走的是裸 `TSocket`，故不动。如果以后 fe-core 改 framed，本模块需同步改。
4. **Ping RPC 的超时控制**：`pingBroker` 在调用前/后切换 socket 超时（5s vs 默认 SOCKET_TIMEOUT_MS），保证 testOnBorrow 不会在异常路径长时间挂起。pool 内已有的 invalidate 链路会处理被判定不健康的连接。

## 单元测试

- `BrokerSpiFileSystemTest`
  - `mkdirs_throwsUnsupportedOperation`（替换原 `mkdirs_isNoOp`）
  - `rename_throwsFileNotFoundOnFileNotFoundStatus`
  - `renameDirectory_runsCallbackWhenSourceMissing`
  - `renameDirectory_propagatesOtherIOException`
  - `renameDirectory_callsRenameWhenSourceExists`
- `BrokerClientFactoryTest`（新增）
  - `pingBroker_returnsTrueOnHealthyClient`
  - `pingBroker_returnsFalseOnTException`
  - `pingBroker_returnsFalseOnNonOkStatus`

执行：`mvn test -Dmaven.build.cache.enabled=false` → `Tests run: 56, Failures: 0, Errors: 0, Skipped: 0`，0 checkstyle 违规。

## 行为变更

- `BrokerSpiFileSystem.mkdirs(loc)` 不再静默成功，改抛 `UnsupportedOperationException`。依赖该方法的调用方需改为写哨兵文件或调整流程。
- `BrokerSpiFileSystem.rename(src, dst)` 在 broker 返回 `FILE_NOT_FOUND` 时抛 `FileNotFoundException`（仍是 `IOException` 子类，对仅 catch `IOException` 的调用方透明）。
- `BrokerSpiFileSystem.renameDirectory(src, dst, whenSrcNotExists)` 由 SPI 默认实现切换为本地重写：源缺失时只跑 callback、源存在时直接 rename，避免 TOCTOU。
- `BrokerClientFactory.validateObject` 现在会发起一次 `ping` RPC；坏连接会更早被 commons-pool2 丢弃。

## 已知遗留 / 决议

- **F11 主动跳过**：当前与 fe-core 的 `GenericPool` 行为对齐（裸 `TSocket`）。若后续要全链路切换至 `TFramedTransport`，需要 fe-core + fe-filesystem 联动改动，本次审计扫尾时不动。
- 余下 LOW / NIT 修复在后续提交中处理。
