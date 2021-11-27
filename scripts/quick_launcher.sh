spark_root='/home/miko/bin/spark'
csv_file='/home/miko/Data/crunchbase/funding_rounds.csv'
input_type='generic_file'
output_file='/home/miko/Data/fr_1k'
output_format='csv'
session_name='test'
rows_limit='1000'

python hurma/spark_export_jobs/write_csv_to_binary.py $spark_root $csv_file $input_type $output_file $output_format $session_name $rows_limit
