#!/bin/bash

set -e
set -o pipefail

echo -e "\n\n\n\n
#############################
all rowsets, brief
for i in \$(mysql -h127.1 -P9030 -uroot -Dhits -e'show tablets from hits;' | sed -n '2,\$p' | awk '{print \$NF}');do curl \$i 2>/dev/null | grep '\\\"\['; done
#############################
"
for i in $(mysql -h127.1 -P9030 -uroot -Dhits -e'show tablets from hits;' | sed -n '2,$p' | awk '{print $NF}'); do
    curl "$i" 2>/dev/null | grep '\"\['
    # curl "$i" 2>/dev/null
done

echo -e "\n\n\n\n
#############################
all segments
ls -alh $(cat doris_home)/be/storage/data/*/*/*
#############################
"
ls -alh "$(cat doris_home)"/be/storage/data/*/*/*

echo -e "\n\n\n\n
#############################
all tablets
mysql -h127.1 -P9030 -uroot -Dhits -e'show tablets from hits;'
#############################
"
mysql -h127.1 -P9030 -uroot -Dhits -e'show tablets from hits;'

echo -e "\n\n\n\n
#############################
all CompactionStatus
for i in \$(mysql -h127.1 -P9030 -uroot -Dhits -e'show tablets from hits;' | sed -n '2,\$p' | awk '{print \$NF}');do curl \$i 2>/dev/null; done
#############################
"
for i in $(mysql -h127.1 -P9030 -uroot -Dhits -e'show tablets from hits;' | sed -n '2,$p' | awk '{print $NF}'); do
    # curl "$i" 2>/dev/null | grep '\"\['
    curl "$i" 2>/dev/null
done
