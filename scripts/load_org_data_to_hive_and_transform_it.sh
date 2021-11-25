#!/bin/bash

# description: a script to export parent organizations from csv to hive
# author     : dl
# usage      : if in shared directory `/bin/bash load_org_data_to_hive_and_transform_it.sh`
# check that parquet files with names `parent_organizations`, `organizations`, `parent_children_orgs`
# with valid schema are present in shared folder

namenode_root=${1:-hdfs://namenode:8020/user/hive/warehouse/crunchbase.db}


declare -a arr=(parent_organizations organizations parent_children_orgs)

for filename in "${arr[@]}"
do

  namenode_path="$namenode_root/$filename"

  echo "Exporting $filename to $namenode_path"

  hadoop fs -put $filename $namenode_path
done

hive -f insert_and_deduplicate_parent_children_org_data.hql;
