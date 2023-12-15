from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator, BigQueryCheckOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date': days_ago(1),
}

dag = DAG(
    'bigquery_table_processing_with_checks',
    default_args=default_args,
    description='Process tables with checks in BigQuery',
    schedule_interval='@daily',
)

DEPENDENT_DAG_ID = 'load_data_from_gcs_to_bigquery'
DEPENDENT_TASK_ID = 'load_employement_data'

#will check if previous dag has completed its execution or not 
wait_for_dependent_dag = ExternalTaskSensor(
    task_id='wait_for_dependent_dag',
    external_dag_id=DEPENDENT_DAG_ID,
    external_task_id=DEPENDENT_TASK_ID,
    timeout=7200,
    dag=dag,
)

PROJECT_ID = 'uconnect-assesment'
DATASET_ID = 'norm_data'
STAGING_DATASET_ID = 'staging_data'

#copying Students table from staging_data to norm_data
transform_students = BigQueryExecuteQueryOperator(
    task_id='transform_students',
    sql=  f"""CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.students` 
            PARTITION BY graduation_date CLUSTER BY major_id 
            AS 
            SELECT * FROM `{PROJECT_ID}.{STAGING_DATASET_ID}.students`;""",
    use_legacy_sql=False,
    dag=dag,
)

#checking if table is empty or not
check_empty_students = BigQueryCheckOperator(
    task_id='check_empty_students',
    sql=  f"SELECT COUNT(1) FROM `{PROJECT_ID}.{DATASET_ID}.students`",
    use_legacy_sql=False,
    dag=dag,
)

#checking if table is proper grain
check_grain_students = BigQueryCheckOperator(
    task_id='check_grain_students',
    sql=  f"""
        SELECT CASE WHEN COUNT(*) > 0 THEN 0 ELSE 1 END 
        FROM (
            SELECT COUNT(*) 
            FROM `{PROJECT_ID}.{DATASET_ID}.students` 
            GROUP BY student_id 
            HAVING COUNT(*) > 1
        )""",
    use_legacy_sql=False,
    dag=dag,
)

#copying Majors table from staging_data to norm_data
transform_majors = BigQueryExecuteQueryOperator(
    task_id='transform_majors',
    sql=  f"""CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.majors` 
            CLUSTER BY department, major_name 
            AS 
            SELECT * FROM `{PROJECT_ID}.{STAGING_DATASET_ID}.majors`;""",
    use_legacy_sql=False,
    dag=dag,
)

#checking if table is empty or not
check_empty_majors = BigQueryCheckOperator(
    task_id='check_empty_majors',
    sql=  f"SELECT COUNT(1) FROM `{PROJECT_ID}.{DATASET_ID}.majors`",
    use_legacy_sql=False,
    dag=dag,
)

#checking if table is proper grain
check_grain_majors = BigQueryCheckOperator(
    task_id='check_grain_majors',
    sql=  f"""
        SELECT CASE WHEN COUNT(*) > 0 THEN 0 ELSE 1 END 
        FROM (
            SELECT COUNT(*) 
            FROM `{PROJECT_ID}.{DATASET_ID}.majors` 
            GROUP BY major_id 
            HAVING COUNT(*) > 1
        )""",
    use_legacy_sql=False,
    dag=dag,
)
#copying Employment table from staging_data to norm_data
transform_employment = BigQueryExecuteQueryOperator(
    task_id='transform_employment',
    sql=  f"""CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.employment` 
            PARTITION BY start_date
            CLUSTER BY company_name, job_title 
            AS 
            SELECT 
            employment_id
            , student_id
            , company_name
            , job_title
            , salary
            , DATE(TIMESTAMP_SECONDS(CAST(start_date AS INT64))) AS start_date 
            FROM `{PROJECT_ID}.{STAGING_DATASET_ID}.employment`;""",
    use_legacy_sql=False,
    dag=dag,
)

#checking if table is empty or not
check_empty_employment = BigQueryCheckOperator(
    task_id='check_empty_employment',
    sql=f"SELECT COUNT(1) FROM `{PROJECT_ID}.{DATASET_ID}.employment`",
    use_legacy_sql=False,
    dag=dag,
)

#checking if table is proper grain
check_grain_employment = BigQueryCheckOperator(
    task_id='check_grain_employment',
    sql=  f"""
        SELECT CASE WHEN COUNT(*) > 0 THEN 0 ELSE 1 END 
        FROM (
            SELECT COUNT(*) 
            FROM `{PROJECT_ID}.{DATASET_ID}.employment` 
            GROUP BY employment_id, student_id 
            HAVING COUNT(*) > 1
        )""",
    use_legacy_sql=False,
    dag=dag,
)

wait_for_dependent_dag >> transform_students >> check_empty_students >> check_grain_students 
wait_for_dependent_dag >> transform_majors >> check_empty_majors >> check_grain_majors 
wait_for_dependent_dag >> transform_employment >> check_empty_employment >> check_grain_employment 