#!/bin/bash

# description: a script to load additional data in parent_children_orgs table
# author     : dl
# usage      : if in shared directory: `/bin/bash load_additional_data_in_parent_children_orgs.sh`


hive -f insert_parent_children_org_data.hql;
