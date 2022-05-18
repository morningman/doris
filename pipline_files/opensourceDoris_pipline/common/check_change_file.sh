#!/bin/bash

res=(`git diff --name-only HEAD~ HEAD`)
file_nums=${#res[@]}


usage() {
  echo "
Usage: $0 <options>
  Optional options:

    $0                                  check file changed api
    $0 --is_modify_only_invoved_be           if changed code only invoved be, doc, fs_brocker, return 0; else return 2
    $0 --is_modify_only_invoved_fe           if changed code only invoved fe doc, fs_brocker, return 0; else return 2
    $0 --is_modify_only_invoved_doc          if changed code only invoved doc, fs_brocker, return 0; else return 2
  "
  exit 1
}

function check_all_change_files_is_under_doc() {

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
}

function check_all_change_files_is_under_be() {
    doc_num=0
    echo "START CHECK CODE IS ONLY RELATED BE OR NOT"
    for file in ${res[@]}
    do
        #check change file is on be or not
        file_dir=$(echo $file|cut -d '/' -f 1)
        if [[ $file_dir == "be" || $file_dir == "docs" || $file_dir == "fs_brokers" ]];then
            let doc_num+=1
            continue
        fi
    done
    if [[ $doc_num -eq $file_nums ]];then
        echo "JUST MODIFY BE CODE, NO NEED RUN FE UT, PASSED!"
        exit 0
    else
        echo "NOT ONLY BE CODE CHANGED, TRIGGER PIPLINE!!"
        exit 2
    fi
}

function check_all_change_files_is_under_fe() {
    doc_num=0
    echo "START CHECK CODE IS ONLY RELATED FE OR NOT"
    for file in ${res[@]}
    do
        #check change file is on be or not
        file_dir=$(echo $file|cut -d '/' -f 1)
        if [[ $file_dir == "fe" || $file_dir == "docs" || $file_dir == "fs_brokers" ]];then
            let doc_num+=1
            continue
        fi
    done
    if [[ $doc_num -eq $file_nums ]];then
        echo "JUST MODIFY FE CODE, NO NEED RUN BE UT, PASSED!"
        exit 0
    else
        echo "NOT ONLY FE CODE CHANGED, TRIGGER PIPLINE!"
        exit 2
    fi
}

main() {

if [ $# > 0 ]; then
    case "$1" in
        --is_modify_only_invoved_be) check_all_change_files_is_under_be; shift ;;
        --is_modify_only_invoved_fe) check_all_change_files_is_under_fe; shift ;;
        --is_modify_only_doc) check_all_change_files_is_under_doc; shift ;;
        *) echo "ERROR"; usage; exit 1 ;;
    esac

fi
}

main $@
