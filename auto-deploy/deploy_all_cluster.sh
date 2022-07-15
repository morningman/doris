for CLUSTER in VEC_ASAN VEC_RELEASE VEC_DEBUG VEC_UBSAN; do
    echo "deploying $CLUSTER"
    ./deploy_cluster.sh $CLUSTER
done
