from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
import json
import math

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 16),
}

total_number_of_sub_dags = Variable.get("total_number_of_sub_dags_job_posting", 20)

dag = DAG(
    'Job_Posting_limit_offset_DAG',
    default_args=default_args,
    description='A DAG for Job Posting data from Job Posting Analytics DB with limit offset',
    schedule_interval='30 4 * * *',
    catchup=False
)


def extract_data_to_nested(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres_result_db')
    pg_conn = pg_hook.get_conn()
    pg_cursor = pg_conn.cursor()
    ti = kwargs['ti']
    transform_data_output = ti.xcom_pull(task_ids='transform_data')
    for transform_row in transform_data_output:
        pg_cursor.execute(
            'INSERT INTO job_postings_v2 (other_skills,company,max_ctc,min_ctc,job_role,job_type,job_title,'
            'department,job_source,is_duplicate,job_location,preferred_skills,max_experience,min_experience,'
            'relevancy_score,job_description_url,job_description_raw_text,job_description_url_without_job_id,'
            '_airbyte_ab_id,_airbyte_emitted_at,_airbyte_normalized_at,_airbyte_job_openings_hashid,'
            '_airbyte_unique_key,number_of_openings)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (job_description_url_without_job_id) do update set other_skills=EXCLUDED.other_skills,company=EXCLUDED.company,'
            'max_ctc=EXCLUDED.max_ctc,min_ctc=EXCLUDED.min_ctc,job_role=EXCLUDED.job_role,'
            'job_type=EXCLUDED.job_type,job_title=EXCLUDED.job_title,department=EXCLUDED.department,'
            'job_source=EXCLUDED.job_source,is_duplicate=EXCLUDED.is_duplicate,job_location=EXCLUDED.job_location,'
            'preferred_skills=EXCLUDED.preferred_skills,max_experience=EXCLUDED.max_experience,'
            'min_experience=EXCLUDED.min_experience,relevancy_score=EXCLUDED.relevancy_score,'
            'job_description_url=EXCLUDED.job_description_url,job_description_raw_text=EXCLUDED.job_description_raw_text,'
            '_airbyte_ab_id=EXCLUDED._airbyte_ab_id,_airbyte_emitted_at=EXCLUDED._airbyte_emitted_at,'
            '_airbyte_normalized_at=EXCLUDED._airbyte_normalized_at,'
            '_airbyte_job_openings_hashid=EXCLUDED._airbyte_job_openings_hashid,'
            '_airbyte_unique_key=EXCLUDED._airbyte_unique_key,number_of_openings=EXCLUDED.number_of_openings ;',
            (
                transform_row[0],
                transform_row[1],
                transform_row[2],
                transform_row[3],
                transform_row[4],
                transform_row[5],
                transform_row[6],
                transform_row[7],
                transform_row[8],
                transform_row[9],
                transform_row[10],
                transform_row[11],
                transform_row[12],
                transform_row[13],
                transform_row[14],
                transform_row[15],
                transform_row[16],
                transform_row[17],
                transform_row[18],
                transform_row[19],
                transform_row[20],
                transform_row[21],
                transform_row[22],
                transform_row[23],
            )
        )
    pg_conn.commit()


create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS job_postings_v2 (
            id serial,
            other_skills text[],
            company varchar(1000),
            max_ctc varchar(200),
            min_ctc varchar(200),
            job_role varchar(1000),
            job_type varchar(1000),
            job_title varchar(1000), 
            department varchar(1000),
            job_source varchar(50),
            is_duplicate boolean,
            job_location  varchar(1000),
            preferred_skills text[],
            max_experience varchar(100),
            min_experience varchar(100),
            relevancy_score real,
            job_description_url  varchar(1000),
            job_description_raw_text varchar(35000),
            job_description_url_without_job_id varchar(1000) not null PRIMARY KEY,
            _airbyte_ab_id varchar(1000),
            _airbyte_emitted_at TIMESTAMP,
            _airbyte_normalized_at TIMESTAMP,
            _airbyte_job_openings_hashid varchar(1000),
            _airbyte_unique_key varchar(1000),
            number_of_openings real
        );
    ''',
    dag=dag
)

def get_postgres_job_posting_operator(task_iterator):
    return PostgresOperator(
        task_id=f'transform_data_{task_iterator}',
        postgres_conn_id='postgres_job_posting',
        sql=f'''select
                distinct
                skills -> 'otherSkills' as other_skills,
                job_openings.company,
                cast(job_openings.max_ctc as varchar) as max_ctc,
                job_openings.min_ctc,
                job_openings.job_role,
                job_openings.job_type,
                job_openings.job_title,
                job_openings.department,
                job_openings.job_source,
                job_openings.is_duplicate,
                job_openings.job_location,
                skills -> 'preferredSkills' as preferred_skills,
                job_openings.max_experience,
                job_openings.min_experience,
                job_openings.relevancy_score,
                job_openings.job_description_url,
                job_openings.job_description_raw_text,
                job_openings.job_description_url_without_job_id,
                job_openings._airbyte_ab_id,
                job_openings._airbyte_emitted_at::timestamp + INTERVAL '5 hours 30 minutes' as _airbyte_emitted_at,
                job_openings._airbyte_normalized_at::timestamp + INTERVAL '5 hours 30 minutes' as _airbyte_normalized_at,
                job_openings._airbyte_job_openings_hashid,
                job_openings._airbyte_unique_key,
                raw_response -> 'vacancy' as number_of_openings
                from job_openings limit {{{{ ti.xcom_pull(key="query_limit_job_posting_{task_iterator}") }}}} offset {{{{ ti.xcom_pull(key="query_offset_job_posting_{task_iterator}") }}}};
            ''',
        dag=dag
    )


# extract_python_data = PythonOperator(
#     task_id='extract_python_data',
#     python_callable=extract_data_to_nested,
#     provide_context=True,
#     dag=dag
# )

extract_total_job_posting = PostgresOperator(
    task_id='extract_total_job_posting',
    postgres_conn_id='postgres_job_posting',
    sql='''select count(job_description_url_without_job_id) from job_openings;''',
    dag=dag
)

for job_postings_sub_dag_id in range(int(total_number_of_sub_dags)):
    with TaskGroup(group_id=f"job_posting_sub_dag_{job_postings_sub_dag_id}", dag=dag) as job_posting_sub_dag_task_group:
        def transform_data(**kwargs):
            ti = kwargs['ti']
            extract_total_job_posting = ti.xcom_pull("extract_total_job_posting")[0][0]
            individual_total_job_posting = extract_total_job_posting / total_number_of_sub_dags
            query_limit = math.floor(individual_total_job_posting)
            query_offset = individual_total_job_posting * kwargs["current_iterator"]

            ti.xcom_push(key=f"query_limit_job_posting_{job_postings_sub_dag_id}", value=query_limit)
            ti.xcom_push(key=f"query_offset_job_posting_{job_postings_sub_dag_id}", value=query_offset)


        transform_limit_offset = PythonOperator(
            task_id="transform_limit_offset",
            python_callable=transform_data,
            provide_context=True,
            op_kwargs={
                'current_iterator': job_postings_sub_dag_id,
            },
            dag=dag,
        )

        transform_data = get_postgres_job_posting_operator(job_postings_sub_dag_id)

        extract_python_data = PythonOperator(
            task_id=f'extract_python_data_{job_postings_sub_dag_id}',
            python_callable=extract_data_to_nested,
            provide_context=True,
            dag=dag
        )

        transform_limit_offset >> transform_data >> extract_python_data

    create_table >> extract_total_job_posting >> job_posting_sub_dag_task_group
