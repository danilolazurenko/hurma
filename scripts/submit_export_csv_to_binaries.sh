#!/bin/bash

# description: a script to submit python export job to spark
# author     : dl
# usage      : if in root directory source scripts/submit_export_csv_to_binaries.sh 

#set -axeE

echo "Start submit"

SPARK_MASTER_MOD="local[1]"

csv_file=$1
input_type=$2
output_file=$3
output_format=$4
session_name=$5
rows_limit=$6

if test -f $csv_file; then
	$SPARK_HOME/bin/spark-submit \
	--master ${SPARK_MASTER_MOD} \
	~/proj/p1/hurma/spark_export_jobs/write_csv_to_binary.py \
	$SPARK_HOME $csv_file $input_type $output_file $output_format \
	$session_name $rows_limit
	echo "Export"
else
	echo "$csv_file does not exist"
fi



echo "Finish submit"
