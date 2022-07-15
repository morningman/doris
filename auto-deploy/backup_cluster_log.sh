if [ $# -eq 0 ]; then
	echo "$0 CLUSTER"
	exit 1
fi

CLUSTER=$1
CLUSTER_DIR=/mnt/ssd01/selectdb-1.0/$CLUSTER

echo " ============= stop service=============="

option=$2

if [[ "$2" == "clean" ]];then
    parallel-ssh -h $CLUSTER/fe_hosts -i "rm -rf $CLUSTER_DIR/../${CLUSTER}_backup/fe"
    parallel-ssh -h $CLUSTER/be_hosts -i "rm -rf $CLUSTER_DIR/../${CLUSTER}_backup/be"
elif [[ "$2" == "backup" ]];then
    parallel-ssh -h $CLUSTER/fe_hosts -i "rm -rf $CLUSTER_DIR/../${CLUSTER}_backup/fe && mkdir -p $CLUSTER_DIR/../${CLUSTER}_backup/fe; cp -r $CLUSTER_DIR/fe/log $CLUSTER_DIR/../${CLUSTER}_backup/fe/"
    parallel-ssh -h $CLUSTER/be_hosts -i "rm -rf $CLUSTER_DIR/../${CLUSTER}_backup/be && mkdir -p $CLUSTER_DIR/../${CLUSTER}_backup/be; cp -r $CLUSTER_DIR/be/log $CLUSTER_DIR/../${CLUSTER}_backup/be/"
fi
