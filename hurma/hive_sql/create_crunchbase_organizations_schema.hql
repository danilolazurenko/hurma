create database if not exists crunchbase;
use crunchbase;

create external table if not exists parent_organizations (
  uuid        string,
  parent_uuid string
)
stored as parquet
location 'hdfs://namenode:8020/user/datasets/crunchbase/parent_organizations';


create external table if not exists organizations (
  uuid      string,
  name      string,
  permalink string
)
stored as parquet
location 'hdfs://namenode:8020/user/datasets/crunchbase/organizations';


create external table if not exists parent_children_orgs (
  parent_uuid string,
  names string,
  uuids string,
  count int
)
stored as parquet
location 'hdfs://namenode:8020/user/datasets/crunchbase/parent_children_orgs';
