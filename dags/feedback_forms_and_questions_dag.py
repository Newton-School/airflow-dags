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
            'INSERT INTO feedback_forms_and_questions (table_unique_key,'
            'feedback_form_id,form_title,feedback_form_hash,feedback_form_for,'
            'default_feedback_form,'
            'feedback_form_for_roles,'
            'feedback_question_id,'
            'feedback_question_for_role,'
            'question_text,'
            'feedback_question_type,'
            'feedback_question_hash,'
            'question_mandatory)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (table_unique_key) do update set feedback_form_id = EXCLUDED.feedback_form_id,'
            'form_title = EXCLUDED.form_title,'
            'feedback_form_hash = EXCLUDED.feedback_form_hash,'
            'feedback_form_for = EXCLUDED.feedback_form_for,'
            'default_feedback_form = EXCLUDED.default_feedback_form,'
            'feedback_form_for_roles = EXCLUDED.feedback_form_for_roles,'
            'feedback_question_id = EXCLUDED.feedback_question_id,'
            'feedback_question_for_role = EXCLUDED.feedback_question_for_role,'
            'question_text = EXCLUDED.question_text,'
            'feedback_question_type = EXCLUDED.feedback_question_type,'
            'feedback_question_hash = EXCLUDED.feedback_question_hash,'
            'question_mandatory = EXCLUDED.question_mandatory;',
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
                transform_row[12]
            )
        )
    pg_conn.commit()


dag = DAG(
    'feedback_forms_and_questions_dag',
    default_args=default_args,
    description='feedback form cross feedback questions table',
    schedule_interval='30 20 * * *',
    catchup=False,
    max_active_runs=1
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS feedback_forms_and_questions (
            id serial,
            table_unique_key bigint not null PRIMARY KEY,
            feedback_form_id bigint,
            form_title varchar(256),
            feedback_form_hash varchar(32),
            feedback_form_for int,
            default_feedback_form boolean,
            feedback_form_for_roles integer[],
            feedback_question_id int,
            feedback_question_for_role int,
            question_text varchar(1024),
            feedback_question_type int,
            feedback_question_hash varchar(32),
            question_mandatory boolean
    );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''select
    cast(concat(feedback_feedbackform.id, feedback_feedbackquestion.id, feedback_feedbackform.id, feedback_feedbackquestion.feedback_question_type) as bigint) as table_unique_key,
    feedback_feedbackform.id as feedback_form_id,
    feedback_feedbackform.title as form_title,
    feedback_feedbackform.hash as feedback_form_hash,
    feedback_feedbackform.feedback_form_for,
    feedback_feedbackform.default_feedback_form,
    feedback_feedbackform.feedback_form_for_roles,
    feedback_feedbackquestion.id as feedback_question_id,
    feedback_feedbackquestion.feedback_question_for_role,
    feedback_feedbackquestion.text as question_text,
    feedback_feedbackquestion.feedback_question_type,
    feedback_feedbackquestion.hash as feedback_question_hash,
    feedback_feedbackformquestionmapping.mandatory as question_mandatory
from
    feedback_feedbackform
join feedback_feedbackformquestionmapping
    on feedback_feedbackformquestionmapping.feedback_form_id = feedback_feedbackform.id
join feedback_feedbackquestion
    on feedback_feedbackquestion.id = feedback_feedbackformquestionmapping.feedback_question_id;
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