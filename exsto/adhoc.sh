#!/bin/bash -x

export SPARK_HOME=~/opt/spark

rm -rf reply_edge.parquet reply_node.parquet
rm -rf graf_edge.parquet graf_node.parquet

SPARK_LOCAL_IP=127.0.0.1 \
$SPARK_HOME/bin/spark-submit ./adhoc.py