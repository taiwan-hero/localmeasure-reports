#!/usr/bin/bash

export HADOOP_HOME=`pwd`/hadoop-binaries/hadoop-2.4.1
export HADOOP_PREFIX=''
export PATH=`pwd`/hadoop-binaries/pig-0.13.0/bin:$PATH
export HADOOP_OPTS=-Xmx8192m
export PIG_HEAPSIZE=8192