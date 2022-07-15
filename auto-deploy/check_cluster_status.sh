CLUSTER=$1
for FE in `cat $CLUSTER/fe_hosts | awk -F '@' '{print$2}'`; do
    PORT=`cat $CLUSTER/conf/fe.conf | grep query_port | awk -F '=' '{print$2}'`
    echo "$CLUSTER $FE:$PORT"
    mysql -h$FE -P$PORT -uroot -e 'show proc "/backends"\G' | grep -E "Alive"
done
