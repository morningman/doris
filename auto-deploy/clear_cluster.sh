if [ $# -eq 0 ]; then
        echo "$0 CLUSTER"
        exit 1
fi

CLUSTER=$1
CLUSTER_DIR=/mnt/ssd01/cold_on_s3/$CLUSTER
STEPS=2

# 1. stop service
echo " =============1/$STEPS stop services=============="
parallel-ssh -h $CLUSTER/fe_hosts -i "cd $CLUSTER_DIR/fe && ./bin/stop_fe.sh"
parallel-ssh -h $CLUSTER/be_hosts -i "cd $CLUSTER_DIR/be && ./bin/stop_be.sh"


# 2. remove directory
echo " =============2/$STEPS remove dirs=============="
parallel-ssh -h $CLUSTER/fe_hosts -i "rm -rf $CLUSTER_DIR"
parallel-ssh -h $CLUSTER/be_hosts -i "rm -rf $CLUSTER_DIR"
DATA_DIR=`grep ^storage_root_path $CLUSTER/conf/be.conf | awk -F '=' '{print $2}' | sed 's/;/ /g' | awk -F ',' '{print $1}' | tr '\n' ' '`
echo $DATA_DIR
parallel-ssh -h $CLUSTER/be_hosts -i "rm -rf $DATA_DIR"
