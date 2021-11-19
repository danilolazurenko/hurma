#!/bin/bash

# description: a script to submit python parent and children joined data
# author     : dl
# usage      : if in root directory source scripts/submit_crunchbase_parent_children_org_joiner.sh 

#set -axeE

echo "Start submit"

SPARK_MASTER_MOD="local[1]"

parent_org_input=$1
org_input=$2
output_file=$3
output_format=$4
session_name=$5

if [ -f $parent_org_input ]  && [ -f $org_input ]; then

	$SPARK_HOME/bin/spark-submit \
	--master ${SPARK_MASTER_MOD} \
	~/proj/p1/hurma/spark_export_jobs/crunchbase_parent_children_org_joiner.py \
	$SPARK_HOME $parent_org_input $org_input $output_file $output_format \
	$session_name $rows_limit

elif ! [ -f $parent_org_input ] ; then
	echo "$parent_org_input does not exist"

elif ! [ -f $org_input ] ; then
	echo "$org_input does not exist"

fi

echo "Finish submit"
