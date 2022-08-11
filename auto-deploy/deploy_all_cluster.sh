source deploy.conf

for CLUSTER in $clusters; do
    echo "deploying $CLUSTER"
    ./deploy_cluster.sh $CLUSTER
done
