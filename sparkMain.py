import sys

import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce
from pyspark.sql.functions import col
from pyspark.sql.functions import to_date
from pyspark.sql.types import DateType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import StructType



funds_csv_schema = StructType([
    StructField('uuid', StringType()),
    StructField('name', StringType()),
    StructField('type', StringType()),
    StructField('permalink', StringType()),
    StructField('cb_url', StringType()),
    StructField('rank', StringType()),
    StructField('created_at', DateType()),
    StructField('updated_at', DateType()),
    StructField('entity_uuid', StringType()),
    StructField('entity_name', StringType()),
    StructField('entity_type', StringType()),
    StructField('announced_on', StringType()),
    StructField('raised_amount_usd', IntegerType()),
    StructField('raised_amount', IntegerType()),
    StructField('raised_amount_currency_code', StringType()),
])


category_groups_csv_schema = StructType([
    StructField('uuid', StringType()),
    StructField('name', StringType()),
    StructField('type', StringType()),
    StructField('permalink', StringType()),
    StructField('cb_url', StringType()),
    StructField('rank', StringType()),
    StructField('created_at', DateType()),
    StructField('updated_at', DateType()),
    StructField('category_groups_list', StringType()),
])

OUTPUT_FORMATS = ('csv', 'parquet', 'avro')

config = {
    'funds': {
        'sep': '\\t',
        'schema': funds_csv_schema,
        'fields': [
            'uuid',
            'name',
            'type',
            'permalink',
            'cb_url',
            'rank',
            'created_at',
            'updated_at',
            'entity_uuid',
            'entity_name',
            'entity_type',
            'announced_on',
            'raised_amount_usd',
            'raised_amount',
            'raised_amount_currency_code'
        ]
    },
    'category_groups': {
        'sep': ',',
        'schema': category_groups_csv_schema,
        'fields': [
            'uuid',
            'name',
            'type',
            'permalink',
            'cb_url',
            'rank',
            'created_at',
            'updated_at',
            'category_groups_list'
        ]
    },
    'organizations': {
        'sep': ',',
        'fields': [
            'uuid',
            'name',
            'permalink',
        ]
    },
    'parent_organizations': {
        'sep': ',',
        'fields': [
            'uuid',
            'parent_uuid',
        ]
    },
}


def build_spark_session(spark_root, session_name):
    findspark.init(spark_root)
    return SparkSession.builder.appName(session_name).getOrCreate()


def get_dataframe(spark, csv_file, sep, schema=None, infer_schema=False, rows_limit=None):
    read_params = {
        'path': csv_file,
        'sep': sep,
        'header': True
    }

    if infer_schema:
        read_params['inferSchema'] = infer_schema
    else:
        read_params['schema'] = schema
    if rows_limit is not None:
        return spark.read.csv(**read_params).limit(rows_limit)
    data_frame = spark.read.csv(**read_params)
    return data_frame


def get_dataframe_from_csv(spark, csv_file, input_type, rows_limit=None):
    read_params = {
        'spark': spark,
        'csv_file': csv_file,
        'sep': config[input_type]['sep'],
        'rows_limit': rows_limit,
    }
    if input_type in ('organizations', 'parent_organizations'):
        read_params['infer_schema'] = True
    else:
        read_params['infer_schema'] = False
        read_params['schema'] = config[input_type]['schema']

    data_frame = get_dataframe(**read_params)
    return data_frame


def transform_dataframe(data_frame, input_type):
    if input_type == 'funds':
        data_frame = (data_frame
                      .withColumn("d1", to_date(col("announced_on"),'yyyy-mm-dd'))
                      .withColumn("d2", to_date(col("announced_on"),'MMM d, yyyy'))
                      .withColumn("announced_on", coalesce('d1', 'd2'))
                      .sort(col('announced_on').desc_nulls_first()))
    return data_frame.select(config[input_type]['fields'])


def write_to_file(data_frame, output_file, output_format):
    data_frame.write.format(output_format).save(output_file)


def check_output_format(output_format):
    if output_format not in OUTPUT_FORMATS:
        raise Exception('Provide valid output format')


def check_input_type(input_type):
    if input_type not in config.keys():
        raise Exception('Provide valid input type')


def get_job_args():
    try:
        if len(sys.argv) == 7:
            spark_root, csv_file, input_type, output_file, output_format, session_name = sys.argv[1:]
            rows_limit = None
        elif len(sys.argv) == 8:
            spark_root, csv_file, input_type, output_file, output_format, session_name, rows_limit = sys.argv[1:]
            rows_limit = int(rows_limit)
    except (IndexError, ValueError):
        print("""
            Positional parameters of job 
            spark_root     '/home/ubuntu/bin/spark-3.0.3-bin-hadoop2.7'
            csv_file       '/home/ubuntu/Documents/funds_a1.csv'
            input_type     either funds or category_groups   
            output_file    like 'report'
            output_format  either parquet, avro or csv
            session_name   String of session name
            rows_limit     Optional: how much first rows to read. Example: 1000
            """)

    check_output_format(output_format)
    check_input_type(input_type)

    return spark_root, csv_file, input_type, output_file, output_format, session_name, rows_limit


if __name__ == '__main__':
    """
    Positional parameters of job 
    spark_root     '/home/ubuntu/bin/spark-3.0.3-bin-hadoop2.7'
    csv_file       '/home/ubuntu/Documents/funds_a1.csv'
    input_type     either funds or category_groups   
    output_file    like '/home/ubuntu/Documents/funds_report'
    output_format  either parquet or avro
    session_name   String of session name
    rows_limit     Optional: How much first rows to read
    """

    spark_root, csv_file, input_type, output_file, output_format, session_name, rows_limit = get_job_args()

    spark = build_spark_session(spark_root, session_name)
    initial_data_frame = get_dataframe_from_csv(spark, csv_file, input_type, rows_limit)
    output_data_frame = transform_dataframe(initial_data_frame, input_type)
    write_to_file(output_data_frame, output_file, output_format)
