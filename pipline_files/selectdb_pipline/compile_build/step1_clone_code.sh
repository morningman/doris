cd /home/zcp/selectdb
git checkout %test_branch%

#强制覆盖
git fetch --all
git reset --hard origin/%test_branch%
git pull

#修改第三方库配置
sed -i "s/export REPOSITORY_URL=https:\/\/doris-thirdparty-repo.bj.bcebos.com\/thirdparty/export REPOSITORY_URL=https:\/\/doris-thirdparty-hk-1308700295.cos.ap-hongkong.myqcloud.com\/thirdparty/g" thirdparty/vars.sh
