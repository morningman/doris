# Broker FE 文件系统 HIGH 修复总结

对应审计文档 `plan-doc/broker.md` 中编号 **#4 / #5 / #6 / #7 / #8** 的五条 HIGH 级别问题。基于 CRITICAL 修复（commit `092a973eaac`）继续推进。

## 修复矩阵

| 编号 | 文件 | 问题 | 修复方案 |
| ---- | ---- | ---- | -------- |
| #4 | `BrokerInputFile.java#newStream` | `catch (TException)` 未将 `returnToPool` 置 false，损坏的客户端经 `finally` 被 `returnGood` 回连接池，污染下次借用方。 | 在 `catch (TException)` 中先 `clientPool.invalidate(endpoint, client)`，再 `returnToPool = false`。非 OK opStatus 分支属于应用层错误（client 仍健康），保持 `returnGood` 并加注释说明。 |
| #5 | `BrokerOutputFile.java#openWriter` | 同 #4 的连接池污染。 | 同样在 `catch (TException)` 调 `invalidate` + 置 `returnToPool=false`，opStatus 分支加注释。 |
| #6 | `BrokerInputFile.java#length` | `knownLength<0` 时直接抛 `UnsupportedOperationException`（`RuntimeException`），打破 `DorisInputFile.length()` 抛 `IOException` 的契约；导致 `newInputFile(loc)` 无 length 重载几乎不可用。 | 构造器新增 `BrokerSpiFileSystem fs` 反向引用（沿用 CRITICAL 阶段在 `BrokerOutputFile` 引入的模式），`length()` 在 `knownLength<0` 时懒调 `fs.listPath(uri, false)`：空结果 → `FileNotFoundException`；`isDir=true` → `IOException("Not a file: ...")`；否则把 `getSize()` 缓存至 `knownLength`，后续直接复用。`BrokerSpiFileSystem.newInputFile` 两个重载都把 `this` 传入。 |
| #7 | `BrokerInputStream.java#close` | 仅靠 `clientInvalidated` 早返，正常路径下二次 `close()` 会再发一次 `closeReader` RPC，并对同一 client `returnGood` 第二次，触发 commons-pool2 的 `IllegalStateException`。 | 新增 `private boolean closed`，`close()` 首行：若 `closed` 即返回；随后立刻置 `closed=true`，再发 RPC。即便 RPC 抛出，`closed` 已为 true，二次 close 仍是 no-op，与 `Closeable` Javadoc 一致。 |
| #8 | `BrokerInputStream.java#read(byte[], int, int)` | 未做参数校验；违反 `InputStream.read(byte[],int,int)` 契约（`b==null` 应 NPE，越界应 IOOBE，`len==0` 应直接返回 0）。 | 用 `Objects.requireNonNull(bytes)` + `Objects.checkFromIndexSize(off, len, bytes.length)` 做校验，并在任何 RPC 之前对 `len==0` 快速返回 0，避免无谓 RPC。 |

## 关键设计点

1. **连接池处置三态**：与既有 `BrokerSpiFileSystem.exists/delete/rename/listPath` 的处理对齐 ——
   - opStatus != OK：应用层错误，`returnGood`；
   - `TException`：传输层错误，`invalidate` + `returnToPool=false`；
   - 正常完成：成功路径已显式翻转 `returnToPool=false` 时，由所有权方（如 `BrokerInputStream`）后续 returnGood。
2. **`length()` 缓存策略**：直接写回 `knownLength` 字段，使后续 `length()` 调用无需再发 `listPath` RPC；同时未对返回值做防御性 `>=0` 检查（broker 返回非法长度即视为协议错误，由后续读路径自然失败）。
3. **`close()` 幂等的关键顺序**：把 `closed=true` 放在 RPC 之前，保证哪怕 closeReader 抛出，状态仍切换完成；这与 `BrokerOutputStream.close()` 既有写法一致。
4. **`Objects.checkFromIndexSize` 而非自手写 `if`**：`Objects.checkFromIndexSize` 抛出标准 `IndexOutOfBoundsException`，并避免在性能敏感的 read 路径里加防御分支。

## 单元测试

- `BrokerInputFileTest`（新增）
  - `length_returnsCachedKnownLength`
  - `length_lazyFetchesViaListPath`（验证只发一次 RPC，第二次直接命中缓存）
  - `length_throwsFileNotFoundWhenMissing`
  - `length_throwsIOExceptionWhenDirectory`
  - `newStream_invalidatesClientOnTException`
- `BrokerInputStreamTest`（新增）
  - `close_isIdempotent`
  - `close_keepsClosedFlagAfterFailure`（首次 close 抛异常后二次 close 仍 no-op）
  - `read_zeroLengthReturnsZeroWithoutRpc`
  - `read_throwsIoobeWhenOffNegative`
  - `read_throwsIoobeWhenLenExceedsBuffer`
  - `read_throwsNpeWhenBufferNull`
- `BrokerOutputFileTest`（扩展）
  - `openWriter_invalidatesClientOnTException`（注意 mock 中 `fs.exists()` 走 returnGood，断言精确计数：1 次 invalidate + 1 次 returnGood）

执行：`mvn test -Dmaven.build.cache.enabled=false` → `Tests run: 49, Failures: 0, Errors: 0, Skipped: 0`，0 checkstyle 违规。

## 行为变更

- `BrokerInputFile.length()` 在缺省构造（无已知长度）时不再抛 `UnsupportedOperationException`，而是真正向 broker 拉取 file status；不存在 / 是目录会抛 `FileNotFoundException` / `IOException`。
- `BrokerInputStream.close()` 现在严格幂等。
- `BrokerInputStream.read(byte[], off, len)` 做参数校验：参数非法立即抛 NPE / IOOBE。
- `newStream` / `openWriter` 的 `TException` 路径不再污染连接池。

## 已知遗留

- `length()` 在失败路径（broker 返回非法 size）目前不做额外校验，由后续读路径暴露问题。
- 余下 MEDIUM / LOW / NIT 修复在后续提交中处理。
