$SPARK_HOME/bin/spark-submit \
--master local[1] \
~/proj/p1/hurma/spark_export_jobs/crunchbase_parent_children_org_joiner.py \
$SPARK_HOME $2 $3 $4 $5 $6
