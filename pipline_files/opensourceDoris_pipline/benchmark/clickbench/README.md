## 简介

此目录下的脚本用于doris社区流水线clickbench。
流水线中会拷贝此目录下的所有脚本到teamcity的teamcity.build.checkoutDir中运行。

此目录下的脚本对应teamcity流水线中的Build Steps, 顺序为:
1. prepare.sh
2. compile.sh
3. deploy.sh
4. run.sh
5. record.sh
6. clean.sh

其余为辅助脚本及文件。

