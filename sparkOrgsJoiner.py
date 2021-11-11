import sys
from pyspark.sql.functions import collect_list
from sparkMain import check_output_format
from sparkMain import build_spark_session
from sparkMain import write_to_file


def get_job_args():
    try:
        spark_root, input_file_1, input_file_2, output_file, output_format, session_name = sys.argv[1:]
    except IndexError:
        print("""
            Positional parameters of job 
            spark_root     '/home/ubuntu/bin/spark-3.0.3-bin-hadoop2.7'
            input_file_1   '/home/ubuntu/Documents/parent_organizations'
            input_file_2   '/home/ubuntu/Documents/organizations'
            output_file    like 'organizations_with_parents'
            output_format  csv for example
            session_name   String of session name
            """)
    check_output_format(output_format)
    return spark_root, input_file_1, input_file_2, output_file, output_format, session_name


if __name__ == '__main__':
    """
    Positional parameters of job 
    spark_root     '/home/ubuntu/bin/spark-3.0.3-bin-hadoop2.7'
    input_file_1   '/home/ubuntu/Documents/parent_organizations'
    input_file_2   '/home/ubuntu/Documents/organizations'
    output_file    like 'organizations_with_parents'
    output_format  csv for example
    session_name   String of session name
    """

    spark_root, input_file_1, input_file_2, output_file, output_format, session_name = get_job_args()

    spark = build_spark_session(spark_root, session_name)
    dataframe_1 = spark.read.parquet(input_file_1)
    dataframe_2 = spark.read.parquet(input_file_2)
    output_data_frame = (dataframe_1
        .select(['parent_uuid', 'uuid', 'name', 'count'])
        .join(dataframe_2, dataframe_1.uuid == dataframe_2.uuid)
        .groupBy('parent_uuid').count()
        .agg(collect_list(uuid).alias(uuid))
        .agg(collect_list(name).alias(name))
        )

    write_to_file(output_data_frame, output_file, output_format)
