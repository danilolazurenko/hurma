# description: a script to load crunchbase datasets in parquet format to hdfs
# author     : dl
# usage      : if in shared directory: `/bin/bash load_parquet_to_hdfs.sh`
# check that parquet files with names `parent_organizations`, `organizations`, `parent_children_orgs`
# with valid schema are present in shared folder
#
# do after hdfs dfs -mkdir -p hdfs://namenode:8020/user/datasets/crunchbase

dataset_name=$1
file_root=${2:-hdfs://namenode:8020/user/datasets/crunchbase}

hdfs dfs -put $dataset_name "$file_root/$dataset_name"
