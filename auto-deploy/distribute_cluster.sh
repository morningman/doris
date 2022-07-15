
if [ $# -eq 0 ]; then
	echo "$0 CLUSTER"
	exit 1
fi

CLUSTER=$1
CLUSTER_DIR=/mnt/ssd01/selectdb-1.0/$CLUSTER
STEPS=9
# 1. create dirs
echo " =============1/$STEPS create dirs=============="
parallel-ssh -h $CLUSTER/fe_hosts -i "mkdir $CLUSTER_DIR/fe -p"
parallel-ssh -h $CLUSTER/be_hosts -i "mkdir $CLUSTER_DIR/be -p"
DATA_DIR=`grep ^storage_root_path $CLUSTER/conf/be.conf | awk -F '=' '{print $2}' | sed 's/;/ /g' | awk -F ',' '{print $1}' | tr '\n' ' '`
echo "mkdir $DATA_DIR"
parallel-ssh -h $CLUSTER/be_hosts -i "mkdir $DATA_DIR -p"

# 2. stop service
echo " =============2/$STEPS stop services=============="
parallel-ssh -h $CLUSTER/fe_hosts -i "cd $CLUSTER_DIR/fe && ./bin/stop_fe.sh"
parallel-ssh -h $CLUSTER/be_hosts -i "cd $CLUSTER_DIR/be && ./bin/stop_be.sh"

# 3. backup be binary
echo " =============3/$STEPS backup be binary=============="
parallel-ssh -h $CLUSTER/be_hosts -i "cd $CLUSTER_DIR/be && mv lib/palo_be lib/palo_be.bak"


# 4. distribute files
echo " =============4/$STEPS distribute files=============="
parallel-scp -h $CLUSTER/fe_hosts -r $CLUSTER/output/fe $CLUSTER_DIR
parallel-scp -h $CLUSTER/be_hosts -r $CLUSTER/output/be $CLUSTER_DIR

# 5. distribute conf
echo " =============5/$STEPS distribute conf=============="
parallel-scp -h $CLUSTER/fe_hosts -r $CLUSTER/conf/fe.conf $CLUSTER_DIR/fe/conf/
parallel-scp -h $CLUSTER/be_hosts -r $CLUSTER/conf/be.conf $CLUSTER_DIR/be/conf/
