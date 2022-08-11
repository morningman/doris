source deploy.conf

for CLUSTER in $clusters; do
    for FE in `cat $CLUSTER/fe_hosts | awk -F '@' '{print$2}'`; do
        echo "$CLUSTER $FE:$PORT"
        PORT=`cat $CLUSTER/conf/fe.conf | grep query_port | awk -F '=' '{print$2}'`
        mysql -h$FE -P$PORT -uroot -e 'set global enable_vectorized_engine=true'
        mysql -h$FE -P$PORT -uroot -e 'show variables' | grep vec
        mysql -h$FE -P$PORT -uroot -e 'set global query_timeout=3600'
        mysql -h$FE -P$PORT -uroot -e 'show variables' | grep query_timeout
        mysql -h$FE -P$PORT -uroot -e 'set global exec_mem_limit=10737418240'
    done
done
