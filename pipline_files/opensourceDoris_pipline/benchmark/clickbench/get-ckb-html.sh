#!/bin/bash

doris_result=$(bash get-result-json.sh)
# replace [ to \[, replace ] to \]
doris_result_for_sed=$(echo $doris_result | sed 's/\[/\\\[/g' | sed 's/\]/\\\]/g')

rm 'index.html'
wget --continue 'https://benchmark.clickhouse.com/'
# add doris result into html
sed "s/const data = \[/const data = \[\n${doris_result_for_sed},/" index.html
