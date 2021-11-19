
$SPARK_HOME/bin/spark-submit \
--master local[1] \
~/proj/p1/hurma/spark_export_jobs/write_csv_to_binary.py \
$SPARK_HOME $2 $3 $4 $5 $6 $7
