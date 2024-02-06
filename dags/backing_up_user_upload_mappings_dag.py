from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from datetime import datetime
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 16),
}

def extract_latest_updated_user_upload_mappings(**kwargs):
    S3_CONN_ID = 's3_aws_credentials'
    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
    s3_bucket_name = 'newton-airflow-dags-temp'
    s3_key = 'user_upload/total_count.txt'

    # Read data from S3 file
    s3_object = s3_hook.get_key(key=s3_key, bucket_name=s3_bucket_name)
    
    if s3_object:
        s3_data = s3_object.get()['Body'].read().decode('utf-8')
        # Perform further processing on the S3 data
        return s3_data
    else:
        print(f"S3 object {s3_key} not found in bucket {s3_bucket_name}")

    return 0

def upload_user_upload_to_s3(**kwargs):
    S3_CONN_ID = 's3_aws_credentials'
    POSTGRES_CONN_ID = 'postgres_read_replica'
    
    ti = kwargs['ti']
    latest_updated_id = ti.xcom_pull(task_ids='extract_latest_updated')
    
    # Connect to PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
    s3_bucket_name = 'newton-airflow-dags-temp'

    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)

    current_offset = 0

    latest_id = 0

    while True:
        postgres_query = f"""select 
uploads_useruploadmapping.id as id,
uploads_useruploadmapping.hash as hash,
uploads_useruploadmapping.content_type_id as content_type_id,
uploads_useruploadmapping.object_id as object_id,
uploads_useruploadmapping.device_type as device_type,
uploads_userupload.upload as upload_url,
uploads_userupload.user_id as user_id,
uploads_userupload.name as name
from uploads_useruploadmapping
join uploads_userupload ON uploads_useruploadmapping.user_upload_id = uploads_userupload.id
left join assignments_assignmentcourseusermapping on assignments_assignmentcourseusermapping.id = cast(uploads_useruploadmapping.object_id as int) and uploads_useruploadmapping.content_type_id = 61 and assignments_assignmentcourseusermapping.cheated = false
left join assessments_courseuserassessmentmapping on assessments_courseuserassessmentmapping.id = cast(uploads_useruploadmapping.object_id as int) and uploads_useruploadmapping.content_type_id = 38 and assessments_courseuserassessmentmapping.cheated = false
where uploads_useruploadmapping.created_at < CURRENT_DATE - INTERVAL '2 months' and content_type_id in (61,38) and id > {latest_updated_id}
order by uploads_useruploadmapping.id limit {100000} offset {current_offset}
"""
        cursor.execute(postgres_query)
        results = cursor.fetchall()

        current_offset += 100000

        df = pd.DataFrame(results, columns=[column[0] for column in cursor.description])

        if len(df) == 0:
            break

        latest_id = df.iloc[-1]['id']

        df.to_csv(f'data_upload_{current_offset}.csv')

        s3_hook.load_file(
            filename=f'data_upload_{current_offset}.csv',
            key=f'user_upload/data/data_upload_{current_offset}.csv',
            bucket_name=s3_bucket_name,
            replace=True,
        )

    s3_hook.load_string(
        f"{latest_id}",
        key='user_upload/total_count.txt',
        bucket_name=s3_bucket_name,
        replace=True,
    )


dag = DAG(
    'backing_up_user_upload_mappings_dag',
    default_args=default_args,
    description='Backing up user uploads',
    schedule_interval='30 20 * * *',
    catchup=False
)

extract_latest_updated = PythonOperator(
    task_id='extract_latest_updated',
    python_callable=extract_latest_updated_user_upload_mappings,
    provide_context=True,
    dag=dag
)

upload_user_upload = PythonOperator(
    task_id='upload_user_upload',
    python_callable=upload_user_upload_to_s3,
    provide_context=True,
    dag=dag
)

extract_latest_updated >> upload_user_upload