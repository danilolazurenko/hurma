create database if not exists crunchbase;
use crunchbase;

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
stored as parquet
location 'hdfs://namenode:8020/user/hive/warehouse/crunchbase.db/parent_organizations';


create external table if not exists organizations (
  uuid string,
  name string,
  type string,
  permalink string,
  cb_url string,
  rank string,
  created_at timestamp,
  updated_at timestamp,
  legal_name string,
  homepage_url string,
  country_code char(3),
  state_code char(2),
  region string,
  city string,
  address string,
  roles string,
  domain string,
  postal_code string,
  status string,
  short_description string,
  category_list string,
  category_groups_list string,
  num_funding_rounds int,
  total_funding_usd int,
  total_funding int,
  total_funding_currency_code char(3),
  founded_on date,
  last_funding_on date,
  closed_on date,
  employee_count string,
  email string,
  phone string,
  facebook_url string,
  linkedin_url string,
  twitter_url string,
  logo_url string,
  alias1 string,
  alias2 string,
  alias3 string,
  primary_role string,
  num_exits int
)
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as parquet
location 'hdfs://namenode:8020/user/hive/warehouse/crunchbase.db/organizations';


create external table if not exists parent_children_orgs (
  parent_uuid string,
  names string,
  uuids string,
  count int
)
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as parquet
location 'hdfs://namenode:8020/user/hive/warehouse/crunchbase.db/parent_children_orgs';
