import sys
from pyspark.sql.functions import collect_list
from sparkMain import check_output_format
from sparkMain import build_spark_session
from sparkMain import write_to_file


def get_job_args():
    try:
        spark_root, parent_org_input, org_input, output_file, output_format, session_name = sys.argv[1:]
    except IndexError:
        print("""
            Positional parameters of job 
            spark_root     '/home/ubuntu/bin/spark-3.0.3-bin-hadoop2.7'
            parent_org_input   '/home/ubuntu/Documents/parent_organizations'
            org_input   '/home/ubuntu/Documents/organizations'
            output_file    like 'organizations_with_parents'
            output_format  csv for example
            session_name   String of session name
            """)
    check_output_format(output_format)
    return spark_root, parent_org_input, org_input, output_file, output_format, session_name


if __name__ == '__main__':
    """
    Positional parameters of job 
    spark_root     '/home/ubuntu/bin/spark-3.0.3-bin-hadoop2.7'
    parent_org_input   '/home/ubuntu/Documents/parent_organizations'
    org_input   '/home/ubuntu/Documents/organizations'
    output_file    like 'organizations_with_parents'
    output_format  csv for example
    session_name   String of session name
    """

    spark_root, parent_org_input, org_input, output_file, output_format, session_name = get_job_args()

    spark = build_spark_session(spark_root, session_name)
    parent_org_df = spark.read.parquet(parent_org_input)
    org_df = spark.read.parquet(org_input)
    joined_data_frame = (parent_org_df
                         .join(org_df, parent_org_df.uuid == org_df.uuid)
                         .groupBy('parent_uuid'))
    joined_data_frame.describe()
    joined_data_frame.showSchema()
    # .agg(collect_list(uuid).alias(uuid))
    # .agg(collect_list(name).alias(name))

    # write_to_file(joined_data_frame, output_file, output_format)
