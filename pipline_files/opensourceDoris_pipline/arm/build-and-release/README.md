## 简介

此目录下的脚本用于Doris社区流水线发布二进制包，复用`../regression-p0/`中的脚本。
即发布arm平台二进制的流程为：
1. 指定分支、commit id、二进制包名（例如：doris-1.1.1-bin-x86）
2. 编译、部署、跑p0回归
3. 打包（doris-1.1.1-bin-x86.tar.gz）、上传到COS、给出下载链接
