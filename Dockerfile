FROM python:alpine

ADD requirements /tmp

RUN python3 -m pip install -r /tmp/requirements && \
    mkdir /data

ADD src/* /opt/
