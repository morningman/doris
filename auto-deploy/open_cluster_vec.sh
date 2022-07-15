#!/bin/bash
CLUSTER=$1

clusters=("VEC_ASAN" "VEC_DEBUG" "VEC_RELEASE" "VEC_UBSAN")
if [ -z $CLUSTER ]; then
    echo "PLZ giving a regression cluster name"
    echo "U can choose one of cluster: ""${clusters[@]}"
else
    if [[ ! "${clusters[@]}" =~ "$CLUSTER" ]];then
        echo "PLZ check cluster name"
        echo "U can choose one of cluster: ""${clusters[@]}"
        exit -1
    fi

fi

for FE in `cat $CLUSTER/fe_hosts | awk -F '@' '{print$2}'`; do
    PORT=`cat $CLUSTER/conf/fe.conf | grep query_port | awk -F '=' '{print$2}'`
    echo "$CLUSTER $FE:$PORT"
    mysql -h$FE -P$PORT -uroot -e 'set global enable_vectorized_engine=true'
    mysql -h$FE -P$PORT -uroot -e 'show variables' | grep vec
    mysql -h$FE -P$PORT -uroot -e 'set global query_timeout=3600'
    mysql -h$FE -P$PORT -uroot -e 'show variables' | grep query_timeout
    mysql -h$FE -P$PORT -uroot -e 'set global exec_mem_limit=10737418240'
    mysql -h$FE -P$PORT -uroot -e 'show variables' | grep exec_mem_limit
done


#for CLUSTER in VEC_ASAN VEC_DEBUG VEC_RELEASE VEC_UBSAN; do
#    for FE in `cat $CLUSTER/fe_hosts | awk -F '@' '{print$2}'`; do
#        echo "$CLUSTER $FE:$PORT"
#        PORT=`cat $CLUSTER/conf/fe.conf | grep query_port | awk -F '=' '{print$2}'`
#        mysql -h$FE -P$PORT -uroot -e 'set global enable_vectorized_engine=true'
#        mysql -h$FE -P$PORT -uroot -e 'show variables' | grep vec
#        mysql -h$FE -P$PORT -uroot -e 'set global query_timeout=3600'
#        mysql -h$FE -P$PORT -uroot -e 'show variables' | grep query_timeout
#        mysql -h$FE -P$PORT -uroot -e 'set global exec_mem_limit=10737418240'
#    done
#done
