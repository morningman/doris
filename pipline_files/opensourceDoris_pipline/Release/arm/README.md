## 简介

此目录下的脚本用于Doris社区流水线发布二进制包，复用`pipline_files/opensourceDoris_pipline/arm/regression-p0/`中的脚本。
即发布arm平台二进制的流程为：
1. 指定分支、commit id、二进制包名（例如：doris-1.1.1-bin-arm）
2. 编译、部署、跑p0回归
3. 打包（doris-1.1.1-bin-arm.tar.gz）、上传到COS、给出下载链接

### teamcity配置

此目录下的脚本对应teamcity流水线中的Build Steps, 顺序为:
1. checkout.sh
2. regression-p0/prepare.sh
3. regression-p0/compile.sh
4. regression-p0/deploy.sh
5. regression-p0/run.sh
6. regression-p0/clean.sh
7. pack.sh
