import sys
import findspark
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import IntegerType
from pyspark.sql.types import DatetimeConverter
from pyspark.sql.types import DateType
from pyspark.sql.types import DateConverter
from pyspark.sql.types import StringType
from pyspark.sql.functions import date_format, col, udf, coalesce, to_date


parquet_schema = StructType([
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


avro_schema = StructType([
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


reading_config = {
    'parquet': {
        'sep': '\\t',
        'schema': parquet_schema,
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
    'avro': {
        'sep': ',',
        'schema': avro_schema,
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
    }
}


def build_spark_session(spark_root):
    findspark.init(spark_root)
    return SparkSession.builder.appName('Test').getOrCreate()


def get_dataframe(spark, csv_file, schema, sep):
    data_frame = spark.read.csv(
            path=csv_file,
            schema=schema,
            sep=sep,
            header=True)
    return data_frame


def transform_dataframe(data_frame):
    return (data_frame
            .withColumn("d1", to_date(col("announced_on"),'yyyy-mm-dd'))
            .withColumn("d2", to_date(col("announced_on"),'MMM d, yyyy'))
            .withColumn("announced_on", coalesce('d1', 'd2')))


def get_dataframe_from_csv(spark, csv_file, output_type):
    data_frame = get_dataframe(
                    spark,
                    csv_file,
                    schema=reading_config[output_type]['schema'],
                    sep=reading_config[output_type]['sep'])
    if output_type == 'parquet':
        data_frame = transform_dataframe(data_frame)
    return data_frame


def write_to_file(data_frame, output_file, output_type):
    fields = reading_config[output_type]['fields']
    if output_type == 'parquet':
        (data_frame
            .select(fields)
            .sort(col('announced_on').desc_nulls_first())
            .write.parquet(output_file))
    else:
        data_frame.select(fields).write.format('avro').save(output_file)


def check_output_type(output_type):
    if output_type not in ('parquet', 'avro'):
        print('Provide valid output type')
        return


if __name__ == '__main__':
    """
    Positional parameters of job 
    spark_root     '/home/ubuntu/bin/spark-3.0.3-bin-hadoop2.7'
    csv_file       '/home/ubuntu/Documents/funds_a1.csv'
    output_file    like 'report'
    output_type    either parquet or avro
    """

    spark_root = sys.argv[1]
    csv_file = sys.argv[2]
    output_file = sys.argv[3]
    output_type = sys.argv[4]
    
    check_output_type(output_type)
    print(spark_root)
    spark = build_spark_session(spark_root)
    data_frame = get_dataframe_from_csv(spark, csv_file, output_type)
    write_to_file(data_frame, output_file, output_type)
