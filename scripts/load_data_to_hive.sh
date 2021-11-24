#!/bin/bash

# description: a script to export parent organizations from csv to hive
# author     : dl
# usage      : if in shared directory `source load_data_to_hive.sh`

file_input=$1
table_name=$2

if ! [ -f file_input ] ; then
	echo "file_input does not exist"

namenode_path = hdfs://namenode:8020/user/hive/warehouse/testdb.db/$table_name

hadoop fs -put $file_input $namenode_path
