#!/bin/bash

path="`pwd`/data"
echo ${path}
docker run -it --restart=on-failure:10 -v ${path}:/app/data nasr_download:0.1.3 python run1.py