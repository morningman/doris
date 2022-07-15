if [ $# -eq 0 ]; then
	echo "$0 CLUSTER"
	exit 1
fi

CLUSTER=$1
CLUSTER_DIR=/mnt/ssd01/selectdb-1.0/$CLUSTER
EDIT_PORT=$(grep edit_log_port $CLUSTER/conf/fe.conf | awk -F '=' '{print $2}' |  sed 's/[ ]*//g')
FE_MASTER_HOST=$(cat $CLUSTER/fe_master_host | awk -F '@' '{print $2}')
echo " ============= start service=============="
EXPORT_ASAN="export ASAN_OPTIONS=symbolize=1:abort_on_error=1:disable_coredump=0:unmap_shadow_on_exit=1"
EXPORT_UBSAN="export UBSAN_OPTIONS=print_stacktrace=1"
EXPORT_ASAN_SYMBOLIZER_PATH="export ASAN_SYMBOLIZER_PATH=/var/local/ldb_toolchain/bin/llvm-symbolizer"
EXPORT_HEAP_PROFILE="export TCMALLOC_SAMPLE_PARAMETER=524289"
EXPORT_LDB_TOOLCHAIN_PATH="export PATH=/var/local/ldb_toolchain/bin/:\$PATH"
parallel-ssh -h $CLUSTER/be_hosts -i "sysctl -w vm.max_map_count=2100000000"
parallel-ssh -h $CLUSTER/be_hosts -i "sysctl -w vm.overcommit_memory=1"
echo "cd $CLUSTER_DIR/fe && ulimit -c unlimited -n 1000000 && $EXPORT_ASAN && $EXPORT_UBSAN && ./bin/start_fe.sh --daemon"
parallel-ssh -h $CLUSTER/fe_master_host -i "cd $CLUSTER_DIR/fe && ulimit -c unlimited -n 1000000 && $EXPORT_ASAN && $EXPORT_UBSAN && export JAVA_HOME=/home/ubuntu/tools/jdk1.8.0_131/ && ./bin/start_fe.sh --daemon"
parallel-ssh -h $CLUSTER/fe_follower_hosts -i "cd $CLUSTER_DIR/fe && ulimit -c unlimited -n 1000000 && $EXPORT_ASAN && $EXPORT_UBSAN && export JAVA_HOME=/home/ubuntu/tools/jdk1.8.0_131/ && ./bin/start_fe.sh  --helper $FE_MASTER_HOST:$EDIT_PORT --daemon"
parallel-ssh -h $CLUSTER/be_hosts -i "cd $CLUSTER_DIR/be && ulimit -c unlimited -n 1000000 && $EXPORT_ASAN && $EXPORT_UBSAN && $EXPORT_ASAN_SYMBOLIZER_PATH && $EXPORT_HEAP_PROFILE && $EXPORT_LDB_TOOLCHAIN_PATH  && ./bin/start_be.sh --daemon"
