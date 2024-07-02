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
            'INSERT INTO apply_forms_and_questions (table_unique_key, apply_form_id, auto_apply,'
            'apply_form_mandatory, apply_form_created_at, form_created_by_id, course_id,'
            'apply_form_question_id, question_text, apply_form_question_type,'
            'lead_squared_lead_field_schema_name, analytics_tool_profile_property_name,'
            'apply_form_question_mandatory)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (table_unique_key) do update set apply_form_id = EXCLUDED.apply_form_id,'
            'auto_apply = EXCLUDED.auto_apply,'
            'apply_form_mandatory = EXCLUDED.apply_form_mandatory,'
            'apply_form_created_at = EXCLUDED.apply_form_created_at,'
            'form_created_by_id = EXCLUDED.form_created_by_id,'
            'course_id = EXCLUDED.course_id,'
            'apply_form_question_id =EXCLUDED.apply_form_question_id,'
            'question_text = EXCLUDED.question_text,'
            'apply_form_question_type = EXCLUDED.apply_form_question_type,'
            'lead_squared_lead_field_schema_name = EXCLUDED.lead_squared_lead_field_schema_name,'
            'analytics_tool_profile_property_name = EXCLUDED.analytics_tool_profile_property_name,'
            'apply_form_question_mandatory = EXCLUDED.apply_form_question_mandatory;',
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
            )
        )
    pg_conn.commit()


dag = DAG(
    'apply_forms_and_questions_dag',
    default_args=default_args,
    description='apply forms cross apply forms questions table',
    schedule_interval='0 20 * * *',
    catchup=False,
    max_active_runs=1
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS apply_forms_and_questions (
            id serial,
            table_unique_key bigint not null PRIMARY KEY,
            apply_form_id bigint,
            auto_apply boolean,
            apply_form_mandatory boolean,
            apply_form_created_at timestamp,
            form_created_by_id bigint,
            course_id int,
            apply_form_question_id int,
            question_text text,
            apply_form_question_type int,
            lead_squared_lead_field_schema_name text,
            analytics_tool_profile_property_name text,
            apply_form_question_mandatory boolean
    );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''select 
    concat(apply_forms_applyform.id, apply_forms_applyform.course_id, apply_forms_applyformquestion.id) as table_unique_key,
    apply_forms_applyform.id as apply_form_id,
    apply_forms_applyform.auto_apply,
    apply_forms_applyform.mandatory as apply_form_mandatory,
    apply_forms_applyform.created_at as apply_form_created_at,
    apply_forms_applyform.created_by_id as form_created_by_id,
    apply_forms_applyform.course_id,
    apply_forms_applyformquestion.id as apply_form_question_id,
    apply_forms_applyformquestion.text as question_text,
    apply_forms_applyformquestion.type as apply_form_question_type,
    apply_forms_applyformquestion.lead_squared_lead_field_schema_name,
    apply_forms_applyformquestion.analytics_tool_profile_property_name,
    apply_forms_applyformquestion.mandatory as apply_form_question_mandatory
from
    apply_forms_applyform
left join apply_forms_applyformquestionmapping
    on apply_forms_applyformquestionmapping.apply_form_id = apply_forms_applyform.id
left join apply_forms_applyformquestion
    on apply_forms_applyformquestion.id = apply_forms_applyformquestionmapping.apply_form_question_id;
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