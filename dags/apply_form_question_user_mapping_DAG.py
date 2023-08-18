from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 16),
}
def extract_data_to_nested(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres_result_db')
    pg_conn = pg_hook.get_conn()
    pg_cursor = pg_conn.cursor()
    ti = kwargs['ti']
    transform_data_output = ti.xcom_pull(task_ids='transform_data')
    for transform_row in transform_data_output:
        pg_cursor.execute(
                'INSERT INTO apply_form_course_user_question_mapping (id,user_id,course_id,'
                'apply_form_question_mapping_id,course_user_mapping_id,course_user_apply_form_mapping_id,'
                'apply_form_question_id,response)'
                'VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'
                'on conflict (id) do update set course_user_mapping_id = EXCLUDED.course_user_mapping_id,'
                'response = EXCLUDED.response;',
                (
                    transform_row[0],
                    transform_row[1],
                    transform_row[2],
                    transform_row[3],
                    transform_row[4],
                    transform_row[5],
                    transform_row[6],
                    transform_row[7],
                )
        )
    pg_conn.commit()


dag = DAG(
    'apply_form_course_user_question_mapping_DAG',
    default_args=default_args,
    description='DAG for apply form question x user mapping',
    schedule_interval='30 20 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS apply_form_course_user_question_mapping (
            id int not null PRIMARY KEY,
            user_id bigint,
            course_id int,
            apply_form_question_mapping_id int,
            course_user_mapping_id bigint,
            course_user_apply_form_mapping_id bigint,
            apply_form_question_id int,
            response text
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''select
        id,
        user_id,
        course_id,
        apply_form_question_mapping_id,
        course_user_mapping_id,
        course_user_apply_form_mapping_id,
        apply_form_question_id,
        response
        from apply_forms_courseuserapplyformquestionmapping;
        ''',
    dag=dag
)

extract_python_data = PythonOperator(
    task_id='extract_python_data',
    python_callable=extract_data_to_nested,
    provide_context=True,
    dag=dag
)
create_table >> transform_data >> extract_python_data