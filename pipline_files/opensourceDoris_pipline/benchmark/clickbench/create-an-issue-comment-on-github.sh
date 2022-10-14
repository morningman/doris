#!/bin/bash
set -ex

# check required parameters
if [[ -z "$1" ]] || [[ -z "$2" ]]; then
    echo 'Usage: bash create-an-issue-comment-on-github.sh <ISSUE_NUMBER> <COMMENT_BODY>'
    exit 1
fi

GITHUB_TOKEN='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'
OWNER='apache'
REPO='doris'
ISSUE_NUMBER="$1"
COMMENT_BODY="$2"
# ISSUE_NUMBER="13259"
# COMMENT_BODY="https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com/tmp/clickbench_pr_29133.html"

echo -e "
Use github api to create an commnet on the pull request web page like https://github.com/apache/doris/pull/13259
What tricky is the api named create-an-issue-comment, not create-a-review-comment-for-a-pull-request :(
Refer to: https://docs.github.com/en/rest/issues/comments#create-an-issue-comment
"
curl \
    -X POST \
    -H "Accept: application/vnd.github+json" \
    -H "Authorization: Bearer $GITHUB_TOKEN" \
    https://api.github.com/repos/"$OWNER"/"$REPO"/issues/"$ISSUE_NUMBER"/comments \
    -d "{\"body\": \"$COMMENT_BODY\"}"
