# TESTING SCRIPT FOR SPARK JOB
Usage:

```
python3 sparkMain.py 'spark_root_directory' 'path_to_csv_file' 'input_type' 'name_of_output_file' 'format_of_output_file' 'session_name'
```

example:

```
python3 sparkMain.py '/home/ubuntu/bin/spark-3.0.3-bin-hadoop2.7' '/home/ubuntu/Documents/funds_a1.csv' 'funds' 'reports' 'parquet' 'Test'
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
