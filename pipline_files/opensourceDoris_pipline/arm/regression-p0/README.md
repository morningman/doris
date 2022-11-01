## 简介

此目录下的脚本用于teamcity流水线，arm regression-p0 流水线。

此目录下的脚本对应teamcity流水线中的Build Steps, 顺序为:
1. prepare.sh
2. compile.sh
3. deploy.sh
4. run.sh
5. clean.sh

辅助脚本位于上级目录 `../common/` 中。

## 维护

更新此目录中的脚本后，需要手动拷贝到[teamcity server](http://43.132.222.7:8111/admin/editBuildRunners.html?id=buildType:Doris_ArmPipeline_P0Regression)以生效。

更新辅助脚本，无需手动操作，流水线会自动拉取最新脚本。
