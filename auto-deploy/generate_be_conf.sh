echo $STORAGE_ROOT
for CLUSTER in NON_VEC_ASAN NON_VEC_DEBUG NON_VEC_RELEASE NON_VEC_UBSAN VEC_ASAN VEC_DEBUG VEC_RELEASE VEC_UBSAN; do
#for CLUSTER in NON_VEC_RELEASE; do
    STORAGE_ROOT="storage_root_path="
    for IDX in {1..6}; do
        STORAGE_ROOT="$STORAGE_ROOT/mnt/hdd0$IDX/doris.SSD/$CLUSTER;/mnt/hdd0$IDX/doris.HDD/$CLUSTER;"
    done

    sed -i '/.*Doris.SSD*/d' $CLUSTER/conf/be.conf
    sed -i '/.*Doris.HDD*/d' $CLUSTER/conf/be.conf
    sed -i '/.*doris.HDD*/d' $CLUSTER/conf/be.conf
    sed -i '/.*doris.SSD*/d' $CLUSTER/conf/be.conf
    sed -i '/.*storage_root_path/d' $CLUSTER/conf/be.conf
    echo $STORAGE_ROOT >>$CLUSTER/conf/be.conf
done
