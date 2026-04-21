# S3 FileSystem 修复总结 — CRITICAL 级别

本文档记录 `plan-doc/s3.md` 审计中 **CRITICAL** 级别（#1、#2）两个问题的修复内容。两者都属于"静默数据丢失"类风险，且均为一行级别的精确修复。

适用范围与 S3 实现复用一致：`s3` / `cos` / `obs` / `oss` 四个后端共享 `S3FileSystem` + `S3ObjStorage`，本次修复同时生效。

---

## 修复 #1 — `S3OutputFile.create()` 静默覆盖既有对象

### 问题

`S3FileSystem.S3OutputFile.create()` 直接调用 `createOrOverwrite()`，违反 `DorisOutputFile` 接口契约：
- `create()` 应在目标已存在时抛出 `FileAlreadyExistsException`；
- `createOrOverwrite()` 才是无条件覆盖。

任何依赖 fail-if-exists 语义的调用方（snapshot/manifest 发布、锁文件、幂等防重等）都会被静默覆盖，导致数据丢失或 split-brain。

### 修复

`fe/fe-filesystem/fe-filesystem-s3/src/main/java/org/apache/doris/filesystem/s3/S3FileSystem.java`：

```java
@Override
public OutputStream create() throws IOException {
    try {
        objStorage.headObject(location.uri());
    } catch (IOException e) {
        if (isNotFoundError(e)) {
            return createOrOverwrite();
        }
        throw e;
    }
    throw new FileAlreadyExistsException(location.uri());
}
```

- 复用现有 `objStorage.headObject(...)` 与 `isNotFoundError(...)` 用于 404 识别，保持错误处理风格一致；
- 仅在 HEAD 返回 404 时才放行 PUT；其它 IOException 直接外抛，不吞错。

### 已知遗留风险（不在本次范围）

- HEAD → PUT 之间存在 TOCTOU 窗口。如需真正的原子 exclusive-create，需要在 PUT 上携带 S3 在 2024 年新增的 `If-None-Match: *` 头部，留待后续。

---

## 修复 #2 — `S3ObjStorage.copyObject` 未对 `copySource` 做 URL 编码

### 问题

AWS S3 要求 `x-amz-copy-source` 头部按 RFC 3986 percent-encoded。原实现直接拼接：

```java
.copySource(srcUri.bucket() + "/" + srcUri.key())
```

后果（任一即触发）：
- key 含空格 → S3 返回 `400 InvalidArgument`，`rename` 失败；
- key 含 `+`、`%`、`?`、`#` 或非 ASCII 字符 → 拷到错误的源 key（数据错位 / 丢失）。

由于 `S3FileSystem.rename` 的实现是 `copyObject` + `deleteObject`，一旦命中以上字符，rename 行为完全不可预测。

### 修复

`fe/fe-filesystem/fe-filesystem-s3/src/main/java/org/apache/doris/filesystem/s3/S3ObjStorage.java`：

```java
.copySource(SdkHttpUtils.urlEncodeIgnoreSlashes(
        srcUri.bucket() + "/" + srcUri.key()))
```

- 选用 `software.amazon.awssdk.utils.http.SdkHttpUtils.urlEncodeIgnoreSlashes`，保留 `bucket/key` 之间的 `/` 分隔符不被编码，其余字符按 RFC 3986 percent-encode；
- 当前仓库 AWS SDK 版本（2.29.52）的 `CopyObjectRequest.Builder` 仅暴露 `copySource(String)`，没有 `.sourceBucket(...).sourceKey(...)` 拆分接口，因此采用手动编码的方式。

---

## 单元测试

新增测试均位于 `fe/fe-filesystem/fe-filesystem-s3/src/test/java/org/apache/doris/filesystem/s3/`：

- `S3FileSystemTest`
  - `create_throwsFileAlreadyExistsWhenObjectExists`
  - `create_succeedsWhenObjectDoesNotExist`
  - `create_propagatesNonNotFoundIOException`
  - `createOrOverwrite_doesNotProbeForExistence`
- `S3ObjStorageMockTest`
  - `copyObject_percentEncodesCopySourceWithSpecialChars`（断言 `my-bucket/path/has%20space%2Bplus.csv`）
  - `copyObject_percentEncodesUnicodeCopySource`（断言 `my-bucket/data/%C3%A9clair.csv`）

### 运行结果

```
cd fe/fe-filesystem/fe-filesystem-s3 && \
  mvn -q -Dtest=S3FileSystemTest,S3ObjStorageMockTest test
# Tests run: 44, Failures: 0, Errors: 0, Skipped: 0
# BUILD SUCCESS

# 模块全量回归
mvn -q test
# Tests run: 72, Failures: 0, Errors: 0, Skipped: 0
# BUILD SUCCESS
```

Checkstyle 0 violation。

---

## 影响面

- S3 / COS / OBS / OSS 四个对象存储后端立即同时获益；
- `rename`、`createOrOverwrite` 行为不变；
- 仅当对象已存在时 `create()` 行为发生变化（变为抛异常），符合接口契约，调用方若需要覆盖应显式使用 `createOrOverwrite()`。
