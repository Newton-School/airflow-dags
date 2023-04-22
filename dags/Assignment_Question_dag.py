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
    def clean_input(data_type, data_value):
        if data_type == 'string':
            return 'null' if not data_value else f'\"{data_value}\"'
        elif data_type == 'datetime':
            return 'null' if not data_value else f'CAST(\'{data_value}\' As TIMESTAMP)'
        else:
            return data_value

    pg_hook = PostgresHook(postgres_conn_id='postgres_result_db')
    pg_conn = pg_hook.get_conn()
    pg_cursor = pg_conn.cursor()
    ti = kwargs['ti']
    transform_data_output = ti.xcom_pull(task_ids='transform_data')
    for transform_row in transform_data_output:
        pg_cursor.execute(
            'INSERT INTO course_user_mapping (course_user_mapping_id,user_id,course_id,course_name,unit_type,admin_course_user_mapping_id,admin_unit_name,admin_course_id,created_at,status,label_id,utm_campaign,utm_source,utm_medium,hash) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (course_user_mapping_id) do update set status = EXCLUDED.status,label_id = EXCLUDED.label_id,admin_course_user_mapping_id = EXCLUDED.admin_course_user_mapping_id;',
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
                transform_row[20]
            )
        )
    pg_conn.commit()


dag = DAG(
    'courses_cum_dag',
    default_args=default_args,
    description='Course user mapping detailed version',
    schedule_interval='0 20 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS assignment_question (
            assignment_question_id bigint not null PRIMARY KEY,
            created_at timestamp,
            created_by_id bigint,
            hash varchar(128),
            is_deleted boolean,
            max_points int,
            max_marks int,
            peer_reviewed boolean,
            peer_reviewed_by_id bigint,
            question_for_assignment_type array,
            question_title varchar(256),
            question_type int,
            test_case_count int,
            verified boolean,
            feedback_evaluable boolean,
            rating bigint,
            difficulty_type int,
            mandatory boolean,
            topic_id bigint,
            question_utility_type int,
            relevance int
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''select 
    assignments_assignmentquestion.id as assignment_question_id,
    assignments_assignmentquestion.created_at,
    assignments_assignmentquestion.created_by_id,
    assignments_assignmentquestion.hash,
    assignments_assignmentquestion.is_deleted,
    assignments_assignmentquestion.max_points,
    assignments_assignmentquestion.max_marks,
    assignments_assignmentquestion.peer_reviewed, --
    assignments_assignmentquestion.peer_reviewed_by_id, --
    assignments_assignmentquestion.question_for_assignment_type, --
    assignments_assignmentquestion.question_title, --
    assignments_assignmentquestion.question_type, 
    assignments_assignmentquestion.test_cases_count, --
    assignments_assignmentquestion.verified, -- 
    assignments_assignmentquestion.feedback_evaluable,
    assignments_assignmentquestion.rating, -- 
    assignments_assignmentquestion.difficulty_type, -- 
    assignments_assignmentquestiontopicmapping.mandatory, -- 
    assignments_assignmentquestiontopicmapping.topic_id, --
    assignments_assignmentquestiontopicmapping.question_utility_type, --
    assignments_assignmentquestiontopicmapping.relevance -- 
from
    assignments_assignmentquestion
left join assignments_assignmentquestiontopicmapping
    on assignments_assignmentquestiontopicmapping.assignment_question_id = assignments_assignmentquestion.id and main_topic = true;
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