# SCRIPT UTILS FOR SPARK JOB
Usage:

for exporting from csv
```
python3 write_csv_to_binary.py 'spark_root_directory' 'path_to_csv_file' 'input_type' 'name_of_output_file' 'format_of_output_file' 'session_name'
```
for combining 2 files in one with specific transformations
```
python3 crunchbase_parent_children_org_joiner.py 'spark_root_directory' 'parent_org_input' 'org_input' 'output_file' 'output_format' 'session_name'
```

example:

```
python3 write_csv_to_binary.py '/home/ubuntu/bin/spark-3.0.3-bin-hadoop2.7' '/home/ubuntu/Documents/funds_a1.csv' 'funds' 'reports' 'parquet' 'Test'
```

```
python3 crunchbase_parent_children_org_joiner.py '/home/ubuntu/bin/spark-3.0.3-bin-hadoop2.7' '/home/ubuntu/Documents/p_org_report' '/home/ubuntu/Documents/org_report' '/home/ubuntu/Documents/p_c_org_report' 'csv' 'Test'
```

# TO SETUP

If you have spark, java, scala, py4j, jupyter installed, then do
```
export SPARK_HOME="/home/ubuntu/bin/spark-3.0.3-bin-hadoop2.7"  (place of your spark installation)
export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
export PATH=$SPARK_HOME:$PATH
export PYSPARK_DRIVER_PYTHON="jupyter"
export PYSPARK_DRIVER_PYTHON_OPTS="notebook"
export PYSPARK_PYTHON=python3
export PATH=$PATH:~/.local/bin
export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"
export SCALA_HOME="/usr/bin"
export PYSPARK_SUBMIT_ARGS="--master local[3] pyspark-shell"
export PATH=$PATH:$JAVA_HOME/jre/bin
```
