#!/bin/bash
set -ex

FE_HOST="172.21.0.4"
USER='root'
FE_QUERY_PORT='9030'

# wait_cluster_ready
wait_cluster_ready() {
    fe_try_times=10
    be_try_times=10
    while ((fe_try_times > 0)); do
        fe_ready=$(mysql -h$FE_HOST -u$USER -P$FE_QUERY_PORT -e "show frontends\G" | grep -c "Alive: true")
        # echo $fe_ready
        if ((fe_ready == 1)); then
            echo "fe started."
            break
        else
            echo "wait for fe start."
            fe_try_times=$((fe_try_times - 1))
            sleep 6
        fi
    done
    if ((fe_try_times == 0)); then echo "check fe started: failed." && exit 1; fi

    while ((be_try_times > 0)); do
        be_ready_count=$(mysql -h$FE_HOST -u$USER -P$FE_QUERY_PORT -e "show backends\G" | grep -c "Alive: true")
        if ((be_ready_count == 3)); then
            echo "3be started."
            break
        else
            echo "wait for be start."
            be_try_times=$((be_try_times - 1))
            sleep 6
        fi
    done
    if ((be_try_times == 0)); then echo "check be started: failed." && exit 1; fi

    echo "#### frontends ####"
    mysql -h$FE_HOST -u$USER -P$FE_QUERY_PORT -e 'show frontends\G'
    echo "#### backends ####"
    mysql -h$FE_HOST -u$USER -P$FE_QUERY_PORT -e 'show backends\G'
}

wait_cluster_ready
