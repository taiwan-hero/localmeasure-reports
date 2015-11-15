#!/usr/bin/bash

export HADOOP_HOME=`pwd`/hadoop-binaries/hadoop-2.4.1
export HADOOP_PREFIX=''
export PATH=`pwd`/hadoop-binaries/pig-0.13.0/bin:$PATH
export HADOOP_OPTS=-Xmx16384m
export PIG_HEAPSIZE=16384
#uncomment this if you running on Mac
#export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.7.0_79.jdk/Contents/Home
export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64/