#!/bin/bash --login
set -ex
if [[ $1 == "" ]]; then
    echo "No thrift file passed in as first argument"
    exit 1
fi
if [[ ! -f $1 ]]; then
    echo "Passed in an argument that wasn't a file"
    exit 1
fi
git_root=$(git rev-parse --show-toplevel)
work_dir=$(mktemp -d)
cd $work_dir
thrift -gen py:json,utf8strings $1
rc=$?
if [[ $rc != 0 ]];then
    echo "Generating thrift did not succeed. exit code: $rc"
    exit 1
fi
cp -r $work_dir/gen-py/concord/* $git_root/concord
