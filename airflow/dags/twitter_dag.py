from datetime import datetime
from os.path import join
from airflow.models import DAG
from airflow.operators.alura import TwitterOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.utils.dates import days_ago
from pathlib import Path

ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(3)
}

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"
BASE_FOLDER = join(
    str(Path("~/").expanduser()),
    "datapipeline/datalake/{stage}/twitter_sorocaba/{partition}"
)
PARTITION_FOLDER = "extract_date={{ ds }}"

with DAG(
    dag_id="twitter_dag",
    #start_date=datetime.now(),
    default_args=ARGS,
    schedule_interval = "45 19 * * *", #minutos horas dias_do_mes meses semanas
    max_active_runs=1
) as dag:
    twitter_operator = TwitterOperator(
        task_id = "twitter_sorocaba",
        query = "sorocaba",
        file_path = join(
            #"/home/nicholas/datapipeline/datalake",
            #"twitter_sorocaba",
            #"extract_date={{ds}}",
            BASE_FOLDER.format(stage="bronze", partition=PARTITION_FOLDER),
            "QuerySorocaba_{{ ds_nodash }}.json"
        ),
        start_time = (
            "{{"
            f"execution_date.strftime('{ TIMESTAMP_FORMAT }')"
            "}}"
        ),
        end_time = (
            "{{"
            f"next_execution_date.strftime('{ TIMESTAMP_FORMAT }')"
            "}}"
        )
    )

    twitter_transform = SparkSubmitOperator(
        task_id = "transform_twitter_sorocaba",
        application= join(
            #"/home/nicholas/datapipeline/spark/transformation.py",
            str(Path(__file__).parents[2]),
            "spark/transformation.py"
        ),
        name="twitter_transformation",
        application_args=[
            "--src",
            #"/home/nicholas/datapipeline/datalake/"
            #"bronze/twitter_sorocaba/extract_date=2022-01-16",
            BASE_FOLDER.format(stage="bronze", partition=PARTITION_FOLDER),
            "--dest",
            #"/home/nicholas/datapipeline/datalake/silver/twitter_sorocaba",
            BASE_FOLDER.format(stage="silver", partition=""),
            "--process-date",
            "{{ ds }}"
        ]
    )

    twitter_operator >> twitter_transform