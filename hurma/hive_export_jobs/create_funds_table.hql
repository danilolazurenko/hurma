create database if not exists testdb;
use testdb;
create external table if not exists funds (
  uuid string,
  name string,
  type string,
  permalink string,
  cb_url string,
  rank string,
  created_at timestamp,
  updated_at timestamp,
  entity_uuid string,
  entity_name string,
  entity_type organization,
  announced_on date,
  raised_amount_usd int,
  raised_amount int,
  raised_amount_currency_code string
)
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile location 'hdfs://namenode:8020/user/hive/warehouse/testdb.db/funds';
