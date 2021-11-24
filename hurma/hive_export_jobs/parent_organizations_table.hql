create database if not exists testdb;
use testdb;
create external table if not exists parent_organizations (
  uuid string,
  name string,
  type string,
  permalink string,
  cb_url string,
  rank string,
  created_at timestamp,
  updated_at timestamp,
  parent_uuid string,
  parent_name string
)
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile location 'hdfs://namenode:8020/user/hive/warehouse/testdb.db/parent_organizations';
