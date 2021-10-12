import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from soccer_matches_results import get_match_results, persist_soccer_data


default_args = {
    "owner": os.getenv("ADMIN_USER_EMAIL"),
    "depends_on_past": False,
    "start_date": days_ago(0, 0, 0, 0),
    "email": [os.getenv("ADMIN_USER_EMAIL")],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}


dag = DAG(
    "soccer-dag",
    description="Automated data pipeline to keep track of Brazilian Soccer League results.",
    default_args=default_args,
    schedule_interval=timedelta(days=1)
)


extract_and_transform = PythonOperator(
    task_id="soccer_scrapping",
    python_callable=get_match_results,
    dag=dag
)


load_data = PythonOperator(
    task_id="soccer_persist",
    python_callable=persist_soccer_data,
    dag=dag
)


extract_and_transform >> load_data