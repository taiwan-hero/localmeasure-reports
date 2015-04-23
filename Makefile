.PHONY: test

run-interactions:
	pig -x local -f pigscripts/interactions_users.pig -param JARFILES=`pwd`/jar -param LM_UDF=`pwd`/udf -param DB=localhost -param DB_PORT=27017

run-keywords:
	pig -x local -f pigscripts/content_keywords.pig -param JARFILES=`pwd`/jar -param LM_UDF=`pwd`/udf -param DB=localhost -param DB_PORT=27017

develop:
	mkdir -p hadoop-binaries
	cd hadoop-binaries
	wget http://archive.apache.org/dist/hadoop/common/hadoop-2.4.1/hadoop-2.4.1.tar.gz
	wget https://archive.apache.org/dist/pig/pig-0.13.0/pig-0.13.0.tar.gz
	tar xzf hadoop-2.4.1.tar.gz
	tar xzf pig-0.13.0.tar.gz
	cd ..
	source exports.sh
