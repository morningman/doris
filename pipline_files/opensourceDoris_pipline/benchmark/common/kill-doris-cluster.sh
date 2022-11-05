#!/bin/bash

# set -e

echo -e "\n\n\n\n
#############################
kill doris fe
pgrep -f 'doris.PaloFe' | xargs kill -9
#############################
"
# kill -9 "$(ps -ef | grep -v 'grep' | grep 'doris.PaloFe' | awk '{print $2}')"
# kill -9 "$(pgrep -f 'doris.PaloFe')"
pgrep -f 'doris.PaloFe' | xargs sudo kill -9
pgrep -f 'selectdb.PaloFe' | xargs sudo kill -9

echo -e "\n\n\n\n
#############################
kill doris be
pgrep -f 'doris_be' | xargs kill -9
#############################
"
# kill -9 "$(ps -ef | grep -v 'grep' | grep 'doris_be' | awk '{print $2}')"
# kill -9 "$(pgrep -f 'doris_be')"
pgrep -f 'doris_be' | xargs sudo kill -9
pgrep -f 'selectdb_be' | xargs sudo kill -9

sleep 5

if pgrep 'doris.PaloFe'; then echo "stop doris FE failed !!!"; fi
if pgrep 'doris_be'; then echo "stop doris BE failed !!!"; fi
