create database if not exists testdb;
use testdb;
create external table if not exists joined_parent_children_orgs (
  parent_uuid string,
  names string,
  uuids string,
  count int
)
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile location 'hdfs://namenode:8020/user/hive/warehouse/testdb.db/joined_parent_children_orgs';
