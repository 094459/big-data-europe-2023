from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
with DAG(dag_id="aws-iam-mwaa-env", schedule_interval=None, catchup=False, start_date=days_ago(1)) as dag:
    env_aws_identity = BashOperator(
        task_id="sts_getcaller",
        bash_command="aws sts get-caller-identity"
    ),
    s3_list = list_s3_buckets = BashOperator(
        task_id="list_s3_buckets",
        bash_command="aws s3 ls")