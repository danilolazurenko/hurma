hive -e "
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.stats.autogather=true;
set hive.compute.query.using.stats=true;
set hive.stats.fetch.column.stats=true;
set hive.stats.fetch.partition.stats=true;
set hive.tez.auto.reducer.parallelism=true;

set hive.vectorized.execution.reduce.enabled=true;
set hive.vectorized.execution.reduce.groupby.enabled=true;
set hive.vectorized.execution.enabled=true;

set hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set tez.session.client.timeout.secs=900;
set hive.exec.reducers.bytes.per.reducer=67108864;
set hive.tez.min.partition.factor=0.25;
set hive.tez.max.partition.factor=2.0;

set hive.merge.smallfiles.avgsize=400000000;
set hive.merge.mapredfiles=false;
set hive.merge.tezfiles=true;
set hive.merge.mapfiles=false;

set hive.exec.stagingdir=/tmp/hive/;
set hive.exec.scratchdir=/tmp/hive/;

set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=1342177000;

set hive.auto.convert.sortmerge.join=false;
set hive.tez.container.size=6144;
set hive.tez.java.opts=-Xmx4096m;
set mapreduce.map.memory.mb=6144;
set mapreduce.map.java.opts=-Xmx4096m;
set mapreduce.reduce.memory.mb=6144;
set mapreduce.reduce.java.opts=-Xmx4096m;
"
