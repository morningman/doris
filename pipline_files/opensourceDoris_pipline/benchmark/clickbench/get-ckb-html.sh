#!/bin/bash

doris_result=$(bash get-result-json.sh)
# replace [ to \[, replace ] to \]
# note:
#     on GNU bash, version 4.2.46(2)-release (x86_64-koji-linux-gnu): echo $(echo '[0.4]') -> 0
#     on GNU bash, version 5.0.17(1)-release (x86_64-pc-linux-gnu): echo $(echo '[0.4]') -> [0.4]
# I don't know why, at current, get around it...
doris_result_for_sed=$(echo "$doris_result" | sed 's/\[/\\\[/g' | sed 's/\]/\\\]/g')
doris_result_for_sed=$(echo "$doris_result_for_sed" | tr '\n' ' ')

rm 'index.html'
wget --continue 'https://benchmark.clickhouse.com/'
# add doris result into html
sed "s/const data = \[/const data = \[\n${doris_result_for_sed},/" index.html
