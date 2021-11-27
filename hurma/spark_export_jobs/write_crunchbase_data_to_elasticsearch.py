import os
import sys
import json

from pyspark.sql.functions import col
from pyspark.sql.functions import collect_list
from pyspark.sql.functions import create_map
from pyspark.sql.functions import lit
from write_csv_to_binary import build_spark_session
from write_csv_to_binary import InvalidJobParametersException


es_write_conf = {
        "es.nodes": 'localhost',  # specify the node that we are sending data to (this should be the master)
        "es.port": '9200',  # specify the port in case it is not the default port
        "es.resource": 'org_index/org_doc',  # specify a resource in the form 'index/doc-type'
        "es.input.json": "yes",  # is the input JSON?
        "es.mapping.id": "uuid"  # is there a field in the mapping that should be used to specify the ES document ID
    }

os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--jars /home/miko/bin/elasticsearch-hadoop-7.15.2/dist/elasticsearch-spark-20_2.11-7.15.2.jar pyspark-shell'

SYS_ARGS_LEN_PROPER = 5


def get_job_args():
    try:
        sys_argv_len = len(sys.argv)
        print(sys_argv_len)
        print(sys.argv)
        if sys_argv_len < SYS_ARGS_LEN_PROPER:
            raise InvalidJobParametersException('Number of arguments is too small.')
        elif sys_argv_len > SYS_ARGS_LEN_PROPER:
            raise InvalidJobParametersException('Number of arguments is too big.')

        spark_root, organizations_input, funding_rounds_input, session_name = sys.argv[1:]
    except (IndexError, InvalidJobParametersException) as e:
        print(main.__doc__)
        raise e

    return spark_root, organizations_input, funding_rounds_input, session_name


def format_data(data):
    return data['uuid'], json.dumps(data.asDict())


def main():
    """
    Positional parameters of job

    spark_root     '/home/ubuntu/bin/spark-3.0.3-bin-hadoop2.7'
    organizations_input   '/home/ubuntu/Documents/parent_organizations'
    funding_rounds_input   '/home/ubuntu/Documents/organizations'
    session_name   String of session name
    """
    spark_root, organizations_input, funding_rounds_input, session_name = get_job_args()

    spark = build_spark_session(spark_root, session_name)

    org_df = spark.read.csv(organizations_input, header=True).limit(100)
    funding_rounds_df = spark.read.csv(funding_rounds_input, header=True).limit(100)

    create_map_for_funding_rounds = funding_rounds_df.withColumn('funding_rounds_data', create_map(
        lit('lead_investor_uuids'), col('lead_investor_uuids'),
        lit('investment_type'), col('investment_type'),
        lit('investor_count'), col('investor_count')
    )).drop('lead_investor_uuids', 'investment_type', 'investor_count')

    collected_funding_rounds = (create_map_for_funding_rounds
                                .groupBy('org_uuid')
                                .agg(collect_list('funding_rounds_data').alias('funding_rounds')))

    org_with_fund_rounds_df = org_df.join(
        collected_funding_rounds, org_df.uuid == collected_funding_rounds.org_uuid, 'left')

    rdd = org_with_fund_rounds_df.rdd.map(lambda x: format_data(x))

    rdd.saveAsNewAPIHadoopFile(
        path='-',
        outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
        keyClass="org.apache.hadoop.io.NullWritable",
        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
        conf=es_write_conf)


if __name__ == '__main__':
    main()
