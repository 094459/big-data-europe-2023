from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.mysql.transfers.s3_to_mysql import S3ToMySqlOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator

# To use this demo ensure that in your default aws connection
# you add in the extras section
#
# {"region_name": "us-east-1"}
# 
# You will need to create your MySQL database, S3 bucket and Athena database beforehand

from datetime import datetime, timedelta
import requests
import json
from datetime import datetime
import csv
import boto3

# Define some variables - you would typically do this in the Airflow UI

s3_bucket="094459-jokes"
athena_database="my_joke_archive"
athena_table_name="best_jokes_table"

# Create a dynamic filename based on the date the DAG is running

time = datetime.now().strftime("%m/%d/%Y").replace('/', '-')
csv_filename = f"jokes-{time}.csv"
s3_csv_file= f"{time}/{csv_filename}"

def pull_jokes():

    # pull jokes with the api
    url = r"https://official-joke-api.appspot.com/random_ten"
    response = requests.get(url)
    text = json.loads(response.text)
   

    # export to csv

    csv_file = open(csv_filename, 'w')
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(['Type', 'Setup', 'Punchline'])
    for i in text:
        csv_writer.writerow([i['type'], i['setup'], i['punchline']])
        print(i)
    csv_file.close()

    # strip quotes

    with open(csv_filename, "r+", encoding="utf-8") as csv_file:
        content = csv_file.read()
    with open(csv_filename, "w+", encoding="utf-8") as csv_file:
        csv_file.write(content.replace('"', ''))

    # upload data_file to s3 bucket

    s3_client = boto3.client('s3')
    s3_client.upload_file(csv_filename, s3_bucket, s3_csv_file)
    print(f"File {csv_filename} uploaded to s3 bucket {s3_bucket}")
    
    return None

dag = DAG(
    'my_funny_joke_archive',
    description='The essential collection of bad jokes to keep me amused',
    start_date=datetime(2023, 6, 1),
    catchup=False,
    schedule_interval=timedelta(days=1),
)

csv_generate_task = PythonOperator(
        task_id='grab_jokes',
        python_callable=pull_jokes,
        dag=dag
    )

athena_query = """

 CREATE EXTERNAL TABLE IF NOT EXISTS {athena_database}.{athena_table_name} (
  category string,
  joke string,
  punchline string 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION 's3://{s3_bucket}/'

TBLPROPERTIES (
  'has_encrypted_data'='false',
  'skip.header.line.count'='1'
) 
;

""".format(s3_bucket=s3_bucket, time=time, athena_table_name=athena_table_name, athena_database=athena_database)


create_joke_table = """
CREATE TABLE IF NOT EXISTS bad_jokes (
  category TEXT NOT NULL,
  joke TEXT NOT NULL,
  punchline TEXT NOT NULL
);

"""

import_csv_to_athena = AthenaOperator(
    task_id='import_csv_to_athena',
    query=athena_query,
    database=athena_database,
    output_location=f's3://{s3_bucket}/import-processing/',
    aws_conn_id='aws_default',
    dag=dag,
)

# These uses the MySQL database defined in the mysql_conn_id
# make sure you set the following additional info in extras
# {"local_infile": "true"}
# otherwise the import will fail.

create_mysql_table = MySqlOperator(
    task_id="create_mysql_table",
    sql=create_joke_table,
    dag=dag
    )

export_csv_to_mysql = S3ToMySqlOperator(
    s3_source_key=f"s3://{s3_bucket}/{s3_csv_file}",
    mysql_table='bad_jokes',
    mysql_duplicate_key_handling='IGNORE',
    mysql_extra_options="""
            FIELDS TERMINATED BY ','
            IGNORE 1 LINES
            """,
    task_id= 'export_csv_to_mysql',
    aws_conn_id='aws_default',
    mysql_conn_id='mysql_default',
    dag=dag
)

create_mysql_table >> csv_generate_task
csv_generate_task >> import_csv_to_athena
csv_generate_task >> export_csv_to_mysql

