FE_HOST=127.0.0.1
FE_QUERY_PORT=9030
FE_HTTP_PORT=8030
USER=root

query_id=$(mysql -h$FE_HOST -u$USER -P$FE_QUERY_PORT -e"show query profile '/'" | cut -f 1 | sed -n '3p') && \
query_profile=$(curl -H 'Authorization:Basic cm9vdDo=' "http://${FE_HOST}:${FE_HTTP_PORT}/api/profile?query_id=${query_id}") && \
echo -e "$query_profile"
