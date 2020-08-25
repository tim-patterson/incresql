# Dockerfile
FROM ubuntu

RUN apt-get update -y
RUN apt-get install -y git make gcc

RUN git clone https://github.com/electrum/tpch-dbgen

RUN cd tpch-dbgen; make