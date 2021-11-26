#!/bin/bash

# description: a script to deduplicate data in parent_children_orgs table
# author     : dl
# usage      : if in shared directory: `/bin/bash deduplicate_parent_children_org_data.sh`


hive -f deduplicate_parent_children_orgs.hql;
