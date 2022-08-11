source deploy.conf

if [ $# -eq 0 ]; then
	echo "$0 CLUSTER"
	exit 1
fi

CLUSTER=$1
CLUSTER_DIR=$cluster_dir_prefix/$CLUSTER

echo " ============= stop service=============="

parallel-ssh -h $CLUSTER/fe_hosts -i "cd $CLUSTER_DIR/fe && ./bin/stop_fe.sh"
parallel-ssh -h $CLUSTER/be_hosts -i "cd $CLUSTER_DIR/be && ./bin/stop_be.sh"
