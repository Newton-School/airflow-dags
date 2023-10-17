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
            'INSERT INTO user_ratings (table_unique_key,'
            'student_id,'
            'date,'
            'rating,'
            'missing_features,'
            'successfully_completed,'
            'created_at,'
            'topic_pool_id,'
            'template_name,'
            'assignment_rating,'
            'contest_rating,'
            'milestone_rating,'
            'mock_rating,'
            'proctored_contest_rating,'
            'quiz_rating,'
            'plagiarised_assignment_rating,'
            'plagiarised_contest_rating,'
            'plagiarised_proctored_contest_rating,'
            'plagiarised_rating)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (table_unique_key) do update set rating = EXCLUDED.rating,'
            'missing_features = EXCLUDED.missing_features,'
            'successfully_completed = EXCLUDED.successfully_completed,'
            'topic_pool_id = EXCLUDED.topic_pool_id,'
            'template_name = EXCLUDED.template_name,'
            'assignment_rating = EXCLUDED.assignment_rating,'
            'contest_rating = EXCLUDED.contest_rating,'
            'milestone_rating = EXCLUDED.milestone_rating,'
            'mock_rating = EXCLUDED.mock_rating,'
            'proctored_contest_rating = EXCLUDED.proctored_contest_rating,'
            'quiz_rating = EXCLUDED.quiz_rating,'
            'plagiarised_assignment_rating = EXCLUDED.plagiarised_assignment_rating,'
            'plagiarised_contest_rating = EXCLUDED.plagiarised_contest_rating,'
            'plagiarised_proctored_contest_rating = EXCLUDED.plagiarised_proctored_contest_rating,'
            'plagiarised_rating = EXCLUDED.plagiarised_rating ;',
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
            )
        )
    pg_conn.commit()


dag = DAG(
    'user_rating_dag',
    default_args=default_args,
    description='An Analytics Data Layer DAG for User Rating from the Data Science Schema',
    schedule_interval='40 19 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS user_ratings (
            id serial,
            table_unique_key bigint not null PRIMARY KEY,
            student_id bigint,
            date TIMESTAMP,
            rating int,
            missing_features text[],
            successfully_completed boolean,
            created_at TIMESTAMP,
            topic_pool_id int,
            template_name text,
            assignment_rating int,
            contest_rating int,
            milestone_rating int,
            mock_rating int,
            proctored_contest_rating int,
            quiz_rating int,
            plagiarised_assignment_rating int,
            plagiarised_contest_rating int,
            plagiarised_proctored_contest_rating int,
            plagiarised_rating int
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_newton_ds',
    sql='''select
            distinct user_persona_studentperdayrating.id as table_unique_key,
            user_persona_studentperdayrating.student_id,
            user_persona_studentperdayrating.date,
            user_persona_studentperdayrating.rating,
            user_persona_studentperdayrating.missing_features,
            user_persona_studentperdayrating.successfully_completed,
            user_persona_studentperdayrating.created_at,
            user_persona_studentperdayrating.topic_pool_id,
            user_persona_template.name as template_name,
            user_persona_studentperdayrating.assignment_rating,
            user_persona_studentperdayrating.contest_rating,
            user_persona_studentperdayrating.milestone_rating,
            user_persona_studentperdayrating.mock_rating,
            user_persona_studentperdayrating.proctored_contest_rating,
            user_persona_studentperdayrating.quiz_rating,
            user_persona_studentperdayrating.plagiarised_assignment_rating,
            user_persona_studentperdayrating.plagiarised_contest_rating,
            user_persona_studentperdayrating.plagiarised_proctored_contest_rating,
            user_persona_studentperdayrating.plagiarised_rating
            from 
                user_persona_studentperdayrating
            left join user_persona_template 
                on user_persona_template.id = user_persona_studentperdayrating.topic_pool_id;
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