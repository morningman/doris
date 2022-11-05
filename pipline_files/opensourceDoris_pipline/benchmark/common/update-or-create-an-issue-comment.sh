#!/bin/bash
set -ex

# check required parameters
if [[ -z "$1" ]] || [[ -z "$2" ]]; then
    echo 'Usage: bash update-or-create-an-issue-comment.sh <ISSUE_NUMBER> <COMMENT_BODY>' && exit 1
fi

GITHUB_TOKEN='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'
OWNER='apache'
REPO='doris'
ISSUE_NUMBER="$1"
COMMENT_BODY="$2"
COMMENT_USER='"hello-stephen"'
# ISSUE_NUMBER="13259"
# COMMENT_BODY="TeamCity pipeline, clickbench performance test result:\nhttps://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com/tmp/20221025232912_clickbench_pr_33979.html"

echo -e "
Use github api to create an commnet on the pull request web page like https://github.com/apache/doris/pull/13259
What tricky is the api named create-an-issue-comment, not create-a-review-comment-for-a-pull-request :(
Refer to: https://docs.github.com/en/rest/issues/comments#create-an-issue-comment
"

file_name='comments_file'
if ! curl \
    -H "Accept: application/vnd.github+json" \
    -H "Authorization: Bearer ${GITHUB_TOKEN}" \
    https://api.github.com/repos/"${OWNER}"/"${REPO}"/issues/"${ISSUE_NUMBER}"/comments \
    >"${file_name}"; then
    echo -e "\033[31m List issue(${ISSUE_NUMBER}) comments FAIL... \033[0m" && exit 1
fi

comments_count=$(jq '.[] | length' "${file_name}" | wc -l)
for ((i = 1; i <= comments_count; ++i)); do
    comment_body=$(jq ".[-$i].body" "${file_name}")
    comment_user=$(jq ".[-$i].user.login" "${file_name}")
    if [[ "${comment_user}" == "${COMMENT_USER}" ]] &&
        [[ "${comment_body}" == *"${COMMENT_BODY:0:18}"* ]]; then
        echo "Similar comment already exists, will update it..."
        comment_id=$(jq ".[-$i].id" "${file_name}")
        if curl \
            -X PATCH \
            -H "Accept: application/vnd.github+json" \
            -H "Authorization: Bearer ${GITHUB_TOKEN}" \
            https://api.github.com/repos/"${OWNER}"/"${REPO}"/issues/comments/"${comment_id}" \
            -d "{\"body\":\"${COMMENT_BODY}\"}"; then
            echo -e "\033[32m Update issue(${ISSUE_NUMBER}) comment SUCCESS... \033[0m" && exit 0
        else
            echo -e "\033[31m Update issue(${ISSUE_NUMBER}) comment FAIL... \033[0m" && exit 1
        fi
    fi
done

echo "No similar comment exists, will create a new one..."
if curl \
    -X POST \
    -H "Accept: application/vnd.github+json" \
    -H "Authorization: Bearer ${GITHUB_TOKEN}" \
    https://api.github.com/repos/"${OWNER}"/"${REPO}"/issues/"${ISSUE_NUMBER}"/comments \
    -d "{\"body\": \"${COMMENT_BODY}\"}"; then
    echo -e "\033[32m Create issue(${ISSUE_NUMBER}) comment SUCCESS... \033[0m" && exit 0
else
    echo -e "\033[31m Create issue(${ISSUE_NUMBER}) comment FAIL... \033[0m" && exit 1
fi
