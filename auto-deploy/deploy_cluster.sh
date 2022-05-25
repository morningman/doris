if [ $# -eq 0 ]; then
        echo "$0 CLUSTER"
        exit 1
fi

CLUSTER=$1
CLUSTER_DIR=/mnt/ssd01/cold_on_s3/$CLUSTER
STEPS=10
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

# 6. start service
echo " =============6/$STEPS start service=============="
./start_cluster.sh $1

# 7. wait fe master service
FE_PORT=`grep query_port $CLUSTER/conf/fe.conf | awk -F '=' '{print $2}' | sed 's/[ ]*//g'`
echo " =============7/$STEPS wait fe master service=============="
FE_HOST=""
for FE in `cat $CLUSTER/fe_master_host | awk -F '@' '{print $2}'`; do
    MYSQL_CMD="mysql -h$FE -P$FE_PORT"
    echo $MYSQL_CMD
    for _ in {1..60}; do
        if $MYSQL_CMD -e "select 1;" 2>&1 >/dev/null ; then
            FE_HOST=$FE
                break
        else
            sleep 1
        fi
    done
done

# 8. add fe follower
FE_MASTER_HOST=$(cat $CLUSTER/fe_master_host | awk -F '@' '{print $2}')
EDIT_PORT=`grep edit_log_port $CLUSTER/conf/fe.conf | awk -F '=' '{print $2}' |  sed 's/[ ]*//g'`
echo " =============8/$STEPS add fe follower=============="
for FE in `cat $CLUSTER/fe_follower_hosts | awk -F '@' '{print $2}'`; do
    MYSQL_CMD="mysql -h$FE_MASTER_HOST -P$FE_PORT"
        echo "add fe $FE:$EDIT_PORT"
        $MYSQL_CMD -e "alter system add follower '$FE:$EDIT_PORT';" 2>&1 >/dev/null
done

# 8. wait all fe  service started
echo " =============9/$STEPS wait all fe service started=============="

FE_HOST=""
for FE in `cat $CLUSTER/fe_hosts | awk -F '@' '{print $2}'`; do
    MYSQL_CMD="mysql -h$FE -P$FE_PORT"
    echo $MYSQL_CMD
    for _ in {1..60}; do
        if $MYSQL_CMD -e "select 1;" 2>&1 >/dev/null; then
            FE_HOST=$FE
                break
        else
            sleep 2
        fi
    done
done

# 9. add be
echo " =============10/$STEPS add be=============="
BE_PORT=`grep heartbeat_service_port $CLUSTER/conf/be.conf | awk -F '=' '{print $2}' |  sed 's/[ ]*//g'`
for BE in `cat $CLUSTER/be_hosts | awk -F '@' '{print $2}'`; do
    MYSQL_CMD="mysql -h$FE_MASTER_HOST -P$FE_PORT"
        echo "add be $BE:$BE_PORT"
        $MYSQL_CMD -e "alter system add backend '$BE:$BE_PORT';" 2>&1 >/dev/null
done

echo "==============DONE================"
