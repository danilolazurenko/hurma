import sys
from pyspark.sql.functions import col
from pyspark.sql.functions import collect_list
from pyspark.sql.functions import concat_ws
from pyspark.sql.functions import count
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


def do_transform(parent_org_df, org_df):
    return (parent_org_df
            .join(org_df, parent_org_df.uuid == org_df.uuid)
            .groupBy('parent_uuid')
            .agg(collect_list(org_df.name),
                 collect_list(org_df.uuid),
                 count(org_df.uuid))
            .withColumn('collect_list(uuid)', concat_ws(',',col('collect_list(uuid)')))
            .withColumn('collect_list(name)', concat_ws(',',col('collect_list(name)')))
            .withColumnRenamed('collect_list(name)', 'names')
            .withColumnRenamed('collect_list(uuid)', 'uuids'))


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
    joined_data_frame = do_transform(parent_org_df, org_df)

    write_to_file(joined_data_frame, output_file, output_format, sep='|')
