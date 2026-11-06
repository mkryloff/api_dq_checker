import os
import sys
from datetime import datetime, timedelta

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(CURRENT_DIR))

import pandas as pd

from scripts.data_fetcher import fetch_and_corrupt_data, RAW_FILE_PATH
from scripts.dq_core import NullCheck, DuplicatesDQCheck, AnomalyDQCheck
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'krylov_matvey',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=20),
}


def validate_and_log_data(**context):
    run_id = context.get('run_id')

    df = pd.read_json(RAW_FILE_PATH)

    checks = [
        NullCheck(),
        DuplicatesDQCheck(),
        AnomalyDQCheck(threshold=0.30)
    ]

    pg_hook = PostgresHook(postgres_conn_id='my_postgres_dwh')

    for check in checks:
        result = check.run(df=df, column='rate', run_id=run_id)

        insert_query = """
            INSERT INTO dq_logs (check_name, run_id, status, records_scanned, bad_records_count, comment)
            VALUES (%s, %s, %s, %s, %s, %s);
        """

        pg_hook.run(insert_query, parameters=(
            result['check_name'],
            result['run_id'],
            result['status'],
            result['records_scanned'],
            result['bad_records_count'],
            result['comment']
        ))


def load_clean_data_to_dwh(**context):
    df = pd.read_json(RAW_FILE_PATH)

    df_clean = df.drop_duplicates(subset=['report_date', 'currency_code'])

    df_clean = df_clean.dropna(subset=['rate'])

    df_clean = df_clean.sort_values(by='report_date')
    df_clean['pct_change'] = df_clean.groupby('currency_code')['rate'].pct_change().abs()

    df_clean = df_clean[(df_clean['pct_change'].isna()) | (df_clean['pct_change'] <= 0.30)]

    df_clean = df_clean.drop(columns=['pct_change'])

    pg_hook = PostgresHook(postgres_conn_id='my_postgres_dwh')

    rows_to_insert = list(df_clean[['report_date', 'currency_code', 'rate']].itertuples(index=False, name=None))

    insert_query = """
        INSERT INTO currency_rates (report_date, currency_code, rate)
        VALUES (%s, %s, %s)
        ON CONFLICT (report_date, currency_code) DO NOTHING;
    """

    for row in rows_to_insert:
        pg_hook.run(insert_query, parameters=row)



with DAG(
        dag_id='api_data_quality_platform_v1',
        default_args=default_args,
        description='Автоматическая очистка и валидация валютных котировок',
        schedule_interval='@daily',
        start_date=datetime(2026, 1, 1),
        catchup=False
) as dag:
    task_fetch_data = PythonOperator(
        task_id='fetch_raw_data',
        python_callable=fetch_and_corrupt_data
    )

    task_validate_data = PythonOperator(
        task_id='validate_and_log_dq',
        python_callable=validate_and_log_data,
        provide_context=True
    )

    task_load_data = PythonOperator(
        task_id='load_clean_data_to_dwh',
        python_callable=load_clean_data_to_dwh,
        provide_context=True
    )

    task_fetch_data >> task_validate_data >> task_load_data

