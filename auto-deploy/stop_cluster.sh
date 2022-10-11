source $1

if [ $# -lt 2 ]; then
	echo "$0 CONF_FILE CLUSTER"
	exit 1
fi

CLUSTER=$2
CLUSTER_DIR=$cluster_dir_prefix

echo " ============= stop service=============="

parallel-ssh -h $CLUSTER/fe_hosts -i "cd $CLUSTER_DIR/fe && ./bin/stop_fe.sh"
parallel-ssh -h $CLUSTER/be_hosts -i "cd $CLUSTER_DIR/be && ./bin/stop_be.sh"
