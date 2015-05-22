.PHONY: test

run-interactions:
	pig -x local -f pigscripts/interactions_users.pig -param JARFILES=`pwd`/jar -param LM_UDF=`pwd`/udf -param DB=localhost -param DB_PORT=27017

run-keywords:
	pig -x local -f pigscripts/content_keywords.pig -param JARFILES=`pwd`/jar -param LM_UDF=`pwd`/udf -param DB=localhost -param DB_PORT=27017

develop:
	mkdir -p hadoop-binaries
	wget -P hadoop-binaries/ http://archive.apache.org/dist/hadoop/common/hadoop-2.4.1/hadoop-2.4.1.tar.gz
	wget -P hadoop-binaries/ https://archive.apache.org/dist/pig/pig-0.13.0/pig-0.13.0.tar.gz
	tar xzf hadoop-binaries/hadoop-2.4.1.tar.gz -C hadoop-binaries/ 
	tar xzf hadoop-binaries/pig-0.13.0.tar.gz -C hadoop-binaries/
	source exports.sh
