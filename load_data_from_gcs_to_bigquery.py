from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'load_data_from_gcs_to_bigquery',
    default_args=default_args,
    description='DAG to load data from GCS to BigQuery',
    schedule_interval='@daily',
    start_date=days_ago(1),
)

dataset_id = 'staging_data'  
project_id = 'uconnect-assesment'  

#static schema for each table
students_schema = [
    {'name': 'student_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
    {'name': 'first_name', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'last_name', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'major_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'graduation_date', 'type': 'DATE', 'mode': 'NULLABLE'}  
]

majors_schema = [
    {'name': 'major_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
    {'name': 'major_name', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'department', 'type': 'STRING', 'mode': 'NULLABLE'}
]

employment_schema = [
    {'name': 'employment_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
    {'name': 'student_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'company_name', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'job_title', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'salary', 'type': 'FLOAT', 'mode': 'NULLABLE'},  
    {'name': 'start_date', 'type': 'FLOAT', 'mode': 'NULLABLE'}  
]

#loading csv files to BigQuery
load_students_data = GCSToBigQueryOperator(
    task_id='load_students_data',
    bucket='uconnect-staging-data',  
    source_objects=['students_data.csv'], 
    destination_project_dataset_table=f'{project_id}.{dataset_id}.students', 
    skip_leading_rows=1,
    write_disposition='WRITE_TRUNCATE',
    schema_fields=students_schema,
    dag=dag,
)

load_majors_data = GCSToBigQueryOperator(
    task_id='load_majors_data',
    bucket='uconnect-staging-data', 
    source_objects=['majors_data.csv'],  
    destination_project_dataset_table=f'{project_id}.{dataset_id}.majors', 
    skip_leading_rows=1,
    write_disposition='WRITE_TRUNCATE',
    schema_fields=majors_schema,
    dag=dag,
)

load_employement_data = GCSToBigQueryOperator(
    task_id='load_employement_data',
    bucket='uconnect-staging-data',  
    source_objects=['Employment.csv'],  
    destination_project_dataset_table=f'{project_id}.{dataset_id}.employment',  
    skip_leading_rows=1,
    write_disposition='WRITE_TRUNCATE',
    schema_fields=employment_schema,
    dag=dag,
)
# DAG dependencies
load_students_data >> load_majors_data >> load_employement_data
