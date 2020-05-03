#!/usr/bin/env bash
branch=`git symbolic-ref --short HEAD`
echo ${branch}
docker build . -t nasr_download:${branch}