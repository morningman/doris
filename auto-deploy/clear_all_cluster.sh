source deploy.conf

for CLUSTER in $clusters; do
    echo "clearing $CLUSTER"
    ./clear_cluster.sh $CLUSTER
done
