# TESTING SCRIPT FOR SPARK JOB
Usage:

```
python3 job.py 'spark_root_directory' 'path_to_csv_file' 'name_of_output_file' 'type_of_output_file'
```

example:

```
python3 job.py '/home/ubuntu/bin/spark-3.0.3-bin-hadoop2.7' '/home/ubuntu/Documents/funds_a1.csv' 'reports' 'parquet'
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
