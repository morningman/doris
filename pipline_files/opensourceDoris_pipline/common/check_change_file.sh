#!/bin/bash

res=(`git diff --name-only HEAD~ HEAD`)

need_pipline=false
file_nums=${#res[@]}
doc_num=0
for file in ${res[@]}
do
    #check change file is on docs/fs_brokers or not
    file_dir=$(echo $file|cut -d '/' -f 1)
    if [[ $file_dir == "docs" || $file_dir == "fs_brokers" ]];then
        let doc_num+=1
        continue
    fi

    #check change file is md/txt/doc file
    #file_type=$(echo $file|cut -d '.' -f 2)
    #if [[ $file_type == "md" || $file_type == "txt" || $file_type == "doc" ]];then
    #    let doc_num+=1
    #fi
done

if [[ $doc_num -eq $file_nums ]];then
    echo "JUST MODIFY DOCUMENT, NO COED CHSNGED, PASSED!"
    exit 0
else
    echo "CODES IS CHANGED, TRIGGER PIPLINE!"
    exit 2
fi
