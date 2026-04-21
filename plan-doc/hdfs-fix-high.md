# HDFS FE 文件系统 HIGH 问题修复总结

本批次针对 `plan-doc/hdfs.md` 中 **HIGH** 级别的 5 项审计发现进行修复。

## 修复矩阵

| # | 严重 | 位置 | 问题 | 修复策略 |
|---|------|------|------|---------|
| 2 | HIGH | `KerberosHadoopAuthenticator.doAs` | 构造时一次 keytab 登录后再无 relogin，长运行 FE 在票据过期后 `GSSException` | `doAs` 执行前调用 `ugi.checkTGTAndReloginFromKeytab()`；relogin IOException 直接抛出 |
| 3 | HIGH | `KerberosHadoopAuthenticator` ctor | `UserGroupInformation.setConfiguration` 污染 JVM 全局；多个 HDFS catalog 互相覆盖 | 引入进程级 `UGI_INIT_LOCK` 串行化；若 UGI 安全模式已启用且 AuthenticationMethod 一致则跳过 `setConfiguration`（first-writer-wins）；不一致时 `WARN` 记录，便于运维识别 |
| 4 | HIGH | `HdfsConfigBuilder.build` | 仅为 `hdfs` 禁用 FS 缓存，`viewfs/ofs/jfs/oss` 仍走全局缓存，`close()` 会关闭他人持有的 FileSystem | 以 `HdfsFileSystemProvider.SUPPORTED_SCHEMES` 为唯一真源，为每个 scheme 设置 `fs.<scheme>.impl.disable.cache=true` 与 `fs.AbstractFileSystem.<scheme>.impl.disable.cache=true` |
| 5 | HIGH | `DFSFileSystem.delete` | 丢弃 `fs.delete` 的 `boolean`；NN 拒绝（快照、viewfs 只读挂载）被静默吞掉 | 在同一 `doAs` 内检查返回；`false` 时再 `fs.exists`：仍存在则抛 `IOException`，不存在则按幂等语义静默成功 |
| 6 | HIGH | `DFSFileSystem.mkdirs` | 丢弃 `fs.mkdirs` 的 `boolean`；"路径已是文件"等失败被报告为成功 | 检查返回；`false` 时 `getFileStatus`：若为目录则幂等成功，否则抛 `IOException`；`FileNotFoundException` 时包装为 `IOException` |

## 关键设计点

- `HdfsFileSystemProvider.SUPPORTED_SCHEMES` 由 `private` 提升为 `public static final`，作为缓存禁用与 Provider 路由的单一真源，避免两处 scheme 列表漂移。
- Kerberos 修复遵循 Hadoop 惯用法：`checkTGTAndReloginFromKeytab` 在未过期时为 no-op，不需引入后台调度器；同时保留 first-writer-wins 策略避免运行时切换认证模式带来的不一致。
- `delete` / `mkdirs` 的错误处理均在原 `doAs` 闭包内完成，不增加额外 RPC 往返（除 `false` 分支下的单次 `exists` / `getFileStatus`），与 Doris "错误上报或崩溃"的编码规则一致。

## 单元测试

新增 5 个单测：

- `DFSFileSystemTest.delete_throwsWhenHadoopReturnsFalseAndPathStillExists`
- `DFSFileSystemTest.delete_returnsSilentlyWhenHadoopReturnsFalseAndPathAbsent`
- `DFSFileSystemTest.mkdirs_throwsWhenPathExistsAsFile`
- `DFSFileSystemTest.mkdirs_idempotentWhenPathAlreadyDirectory`
- `HdfsConfigBuilderTest.build_disablesCacheForAllSupportedSchemes`

未新增 Kerberos 单测：安全地 mock `UserGroupInformation` 的静态方法需要 `mockito-inline` 或 PowerMock，不在本仓依赖范围内；现有 `KerberosHadoopAuthenticatorEnvTest` 仍依赖带 KDC 的真实环境，保持不变。

运行结果：

```
Tests run: 41, Failures: 0, Errors: 0, Skipped: 0
Checkstyle violations: 0
BUILD SUCCESS
```

## 行为变更

- `delete` 与 `mkdirs` 现在能够将 Hadoop 静默失败转为显式 `IOException`，上层 catalog / load 任务不会再继续基于"假成功"推进。
- Kerberos catalog 在长运行 FE 下不再随票据过期而批量失败。
- 多 HDFS catalog 混合不同 scheme 时，单个 `DFSFileSystem.close()` 不再误关其他 catalog 的 FS 实例。

## 已知遗留

- `SimpleHadoopAuthenticator` 未调用 `setConfiguration` 的 NIT 级问题（#22）留待 NIT 批次处理。
- MEDIUM 级问题（#7 renameDirectory 非原子、#8 rename 错误上下文、#9 iterator close 无作用等）留待 MEDIUM 批次处理。
