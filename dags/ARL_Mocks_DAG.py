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
            'INSERT INTO arl_mocks (table_unique_key,course_id,'
            'one_to_one_date,one_to_one_type,topic_pool_id,topic_pool_title,difficulty_level,'
            'scheduled,pending_confirmation,interviewer_declined,confirmation,'
            'student_cancellation,interviewer_cancellation,conducted,cleared,'
            'final_call_no,final_call_maybe,student_no_show,interviewer_no_show)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (table_unique_key) do update set difficulty_level=EXCLUDED.difficulty_level,'
            ' scheduled=EXCLUDED.scheduled,'
            'pending_confirmation=EXCLUDED.pending_confirmation,interviewer_declined=EXCLUDED.interviewer_declined,'
            'confirmation=EXCLUDED.confirmation,student_cancellation=EXCLUDED.student_cancellation,'
            'interviewer_cancellation=EXCLUDED.interviewer_cancellation,conducted=EXCLUDED.conducted,'
            'cleared=EXCLUDED.cleared,final_call_no=EXCLUDED.final_call_no,final_call_maybe=EXCLUDED.final_call_maybe,'
            'student_no_show=EXCLUDED.student_no_show,interviewer_no_show=EXCLUDED.interviewer_no_show ;',
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
    'ARL_Mocks',
    default_args=default_args,
    description='An Analytics Reporting Layer DAG for Mocks DoD level cut',
    schedule_interval='30 0 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS arl_mocks (
            id serial,
            table_unique_key double precision not null PRIMARY KEY,
            course_id int,
            one_to_one_date Date,
            one_to_one_type varchar(64),
            topic_pool_id int,
            topic_pool_title varchar(64),
            difficulty_level varchar(16),
            scheduled int,
            pending_confirmation int,
            interviewer_declined int,
            confirmation int,
            student_cancellation int,
            interviewer_cancellation int,
            conducted int,
            cleared int,
            final_call_no int,
            final_call_maybe int,
            student_no_show int,
            interviewer_no_show int
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_result_db',
    sql='''select
            distinct concat(one_to_one.course_id,EXTRACT(month FROM date(one_to_one.one_to_one_start_timestamp)),one_to_one.difficulty_level,EXTRACT(year FROM date(one_to_one.one_to_one_start_timestamp)),one_to_one.one_to_one_type,one_to_one_topic_mapping.topic_pool_id,EXTRACT(day FROM date(one_to_one.one_to_one_start_timestamp))) as table_unique_key, 
            one_to_one.course_id,
            date(one_to_one.one_to_one_start_timestamp) as one_to_one_date,
            case
            when one_to_one.one_to_one_type = 1 then 'Mock Technical Interview'
            when one_to_one.one_to_one_type = 2 then 'Mock HR Interview'
            when one_to_one.one_to_one_type = 3 then 'Mock Project'
            when one_to_one.one_to_one_type = 4 then 'Mock DSA'
            when one_to_one.one_to_one_type = 5 then 'Mock Full Stack'
            when one_to_one.one_to_one_type = 6 then 'Entrance Interview'
            when one_to_one.one_to_one_type = 7 then 'Mentor Catch Up'
            when one_to_one.one_to_one_type = 8 then 'Interview Coaching'
            when one_to_one.one_to_one_type = 9 then 'Mock Data Science'
            when one_to_one.one_to_one_type = 10 then 'General Interview' end as one_to_one_type,
            one_to_one_topic_mapping.topic_pool_id,
            topic_pool_mapping.topic_pool_title,
            case 
            when one_to_one.difficulty_level = 1 then 'Beginner'
            when one_to_one.difficulty_level = 2 then 'Easy'
            when one_to_one.difficulty_level = 3 then 'Medium'
            when one_to_one.difficulty_level = 4 then 'Hard'
            when one_to_one.difficulty_level = 5 then 'Challenge' else null end as difficulty_level,
            count(distinct one_to_one.one_to_one_id) as scheduled,
            count(distinct one_to_one.one_to_one_id) filter (where one_to_one.one_to_one_status = 1) as pending_confirmation,
            count(distinct one_to_one.one_to_one_id) filter (where one_to_one.one_to_one_status = 3) as interviewer_declined,
            count(distinct one_to_one.one_to_one_id) filter (where one_to_one.one_to_one_status = 2) as confirmation,
            count(distinct one_to_one.one_to_one_id) filter (where one_to_one.one_to_one_status = 5) as student_cancellation,
            count(distinct one_to_one.one_to_one_id) filter (where one_to_one.one_to_one_status = 4) as interviewer_cancellation,
            count(distinct one_to_one.one_to_one_id) filter (where one_to_one.one_to_one_status = 2 and one_to_one.final_call in (1,2,3)) as conducted,
            count(distinct one_to_one.one_to_one_id) filter (where one_to_one.one_to_one_status = 2 and one_to_one.final_call = 1) as cleared,
            count(distinct one_to_one.one_to_one_id) filter (where one_to_one.one_to_one_status = 2 and one_to_one.final_call = 2) as final_call_no,
            count(distinct one_to_one.one_to_one_id) filter (where one_to_one.one_to_one_status = 2 and one_to_one.final_call = 3) as final_call_maybe,
            count(distinct one_to_one.one_to_one_id) filter (where one_to_one.one_to_one_status = 10 and (one_to_one.cancel_reason like 'Insufficient time spent by booked by user%' or one_to_one.cancel_reason like 'Insufficient overlap time')) as student_no_show,
            count(distinct one_to_one.one_to_one_id) filter (where one_to_one.one_to_one_status = 10 and one_to_one.cancel_reason like 'Insufficient time spent by booked with user%') as interviewer_no_show
            from one_to_one
            left join one_to_one_topic_mapping on one_to_one_topic_mapping.one_to_one_id = one_to_one.one_to_one_id
            left join topic_pool_mapping on topic_pool_mapping.topic_pool_id = one_to_one_topic_mapping.topic_pool_id
            group by 1,2,3,4,5,6,7;
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