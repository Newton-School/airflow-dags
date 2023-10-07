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
                'INSERT INTO ai_chat_messages (table_id,'
                'user_id,'
                'created_at,'
                'sender_id,'
                'sender_type,'
                'course_id,'
                'content_type,'
                'assignment_question_id,'
                'lecture_id,'
                'message_type,'
                'senders_response,'
                'failed_response,'
                'is_system_generated_nudge,'
                'selected_response,'
                'correct_option) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                'on conflict (table_id) do update set senders_response = EXCLUDED.senders_response,'
                'failed_response = EXCLUDED.failed_response,'
                'is_system_generated_nudge = EXCLUDED.is_system_generated_nudge,'
                'selected_response = EXCLUDED.selected_response,'
                'correct_option = EXCLUDED.correct_option;',
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
                )
        )
    pg_conn.commit()


dag = DAG(
    'ai_chat_messages_dag',
    default_args=default_args,
    description='cleaner version of ai chat messages table',
    schedule_interval='0 21 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS ai_chat_messages (
            id serial,
            table_id int not null PRIMARY KEY,
            user_id bigint,
            created_at timestamp,
            sender_id bigint,
            sender_type text,
            course_id int,
            content_type text,
            assignment_question_id bigint,
            lecture_id bigint,
            message_type text,
            senders_response text,
            failed_response boolean,
            is_system_generated_nudge boolean,
            selected_response text,
            correct_option text
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''
    select
        id as table_id,
        user_id,
        created_at,
        sender_id,
        case
            when ai_chats_aichatmessage.sender_id = 196030 then 'Newton AI'
            when ai_chats_aichatmessage.sender_id <> 196030 then 'User'
        end as sender_type,
        course_id,
        case
            when content_type_id = 62 then 'Assignments'
            when content_type_id = 46 then 'Lecture'
        end as content_type,
        case when content_type_id = 62 then object_id end as assignment_question_id,
        case when content_type_id = 46 then object_id end as lecture_id,
        ai_chats_aichatmessage.message ->> 'type' as message_type,
        ai_chats_aichatmessage.message ->> 'text' as senders_response,
        failed_response,
        ai_chats_aichatmessage.message ->> 'is_system_generated_nudge' as is_system_generated_nudge,
        message ->> 'selected_response' as selected_response,
        message ->> 'correct_option' as correct_option
    from
        ai_chats_aichatmessage;
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