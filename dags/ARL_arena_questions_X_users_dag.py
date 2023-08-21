from airflow import DAG
# from airflow.decorators import dag
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
            'INSERT INTO arl_arena_questions_x_users (table_unique_key, user_id, '
            'assignment_question_id, module_name, started_at, completed_at, max_test_case_passed, completed, '
            'all_test_case_passed, playground_type, max_plag_score, lead_type, course_id, '
            'course_name, course_structure_class, student_name,'
            'user_placement_status, activity_status_7_days, activity_status_14_days, activity_status_30_days)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (table_unique_key) do update set module_name = EXCLUDED.module_name,'
            'started_at = EXCLUDED.started_at,'
            'completed_at = EXCLUDED.completed_at,'
            'max_test_case_passed = EXCLUDED.max_test_case_passed,'
            'completed = EXCLUDED.completed,'
            'all_test_case_passed = EXCLUDED.all_test_case_passed,'
            'playground_type = EXCLUDED.playground_type,'
            'max_plag_score = EXCLUDED.max_plag_score,'
            'lead_type = EXCLUDED.lead_type,'
            'course_name = EXCLUDED.course_name,'
            'course_structure_class = EXCLUDED.course_structure_class,'
            'student_name = EXCLUDED.student_name,'
            'user_placement_status = EXCLUDED.user_placement_status,'
            'activity_status_7_days = EXCLUDED.activity_status_7_days,'
            'activity_status_14_days = EXCLUDED.activity_status_14_days,'
            'activity_status_30_days = EXCLUDED.activity_status_30_days;',
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
            )
        )
    pg_conn.commit()


dag = DAG(
    'arl_arena_questions_x_users_dag',
    default_args=default_args,
    description='An Analytics Reporting Layer DAG for arena questions at user level',
    schedule_interval='10 1 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS arl_arena_questions_x_users (
            id serial,
            table_unique_key text not null PRIMARY KEY,
            user_id bigint,
            assignment_question_id bigint,
            module_name text,
            started_at timestamp,
            completed_at timestamp,
            max_test_case_passed int,
            completed boolean,
            all_test_case_passed boolean,
            playground_type text,
            max_plag_score real,
            lead_type text,
            course_id int,
            course_name text,
            course_structure_class text,
            student_name text ,
            user_placement_status text,
            activity_status_7_days text,
            activity_status_14_days text,
            activity_status_30_days text
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_result_db',
    sql='''
        select 
            concat(aqum.user_id, c.course_id, aqum.assignment_question_id, cum.course_user_mapping_id) as table_unique_key,
            aqum.user_id,
            aqum.assignment_question_id,
            aqum.module_name,
            aqum.started_at,
            aqum.completed_at,
            aqum.max_test_case_passed,
            aqum.completed,
            aqum.all_test_case_passed,
            aqum.playground_type,
            aqum.max_plag_score,
            ui.lead_type,
            c.course_id,
            c.course_name,
            c.course_structure_class,
            concat(ui.first_name, ' ', ui.last_name) as student_name,
            cum.user_placement_status,
            uasm.activity_status_7_days,
            uasm.activity_status_14_days,
            uasm.activity_status_30_days
        from
            arena_questions_user_mapping aqum
        left join course_user_mapping cum
            on cum.user_id = aqum.user_id
        left join courses c
            on c.course_id = cum.course_id
        left join users_info ui
            on ui.user_id = aqum.user_id
        left join user_activity_status_mapping uasm
            on uasm.user_id = aqum.user_id
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20;
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