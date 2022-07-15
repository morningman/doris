if [ $# -eq 0 ]; then
    echo "$0 CLUSTER"
    exit 1
fi

CLUSTER=$1
CLUSTER_DIR=/mnt/ssd01/selectdb-1.0/$CLUSTER

echo " ============= stop service=============="

parallel-ssh -h $CLUSTER/fe_hosts -i "cd $CLUSTER_DIR/fe && ./bin/stop_fe.sh"
parallel-ssh -h $CLUSTER/be_hosts -i "cd $CLUSTER_DIR/be && ./bin/stop_be.sh --grace"
