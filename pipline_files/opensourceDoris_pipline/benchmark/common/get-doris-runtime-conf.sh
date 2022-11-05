#!/bin/bash

set -e

echo -e "\n\n\n\n
#############################
fe configs
mysql -h127.1 -P9030 -uroot -e'admin show frontend config;'
#############################
"
mysql -h127.1 -P9030 -uroot -e'admin show frontend config;'

echo -e "\n\n\n\n
#############################
be configs
curl '127.0.0.1:8040/varz'
#############################
"
curl '127.0.0.1:8040/varz'

echo -e "\n\n\n\n
#############################
session variables
mysql -h127.1 -P9030 -uroot -e'show variables'
#############################
"
mysql -h127.1 -P9030 -uroot -e'show variables'

echo -e "\n\n\n\n
#############################
tail -n 10 "$(cat doris_home)"/fe/bin/start_fe.sh
#############################
"

tail -n 10 "$(cat doris_home)"/fe/bin/start_fe.sh