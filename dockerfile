FROM python:3.7.6-slim-stretch

RUN mkdir /app
COPY . /app



# RUN cd /code && sed -i s/deb.debian.org/mirrors.aliyun.com/g /etc/apt/sources.list \
#     && apt-get clean && apt-get update \
#     && apt-get install -y --no-install-recommends gcc  \
#     && rm -rf /var/lib/apt/lists/* \
#     && pip install -r requirements.txt -i http://pypi.douban.com/simple --trusted-host pypi.douban.com \
#     && apt-get purge -y --auto-remove gcc
RUN pip install -r requirements.txt -i http://pypi.douban.com/simple --trusted-host pypi.douban.com && cd /app
WORKDIR /app

# ENTRYPOINT python main.py $0 $@

# docker run -it -p 5000:5000  pipeservice:0.0.4 --address=0.0.0.0 --port=5000