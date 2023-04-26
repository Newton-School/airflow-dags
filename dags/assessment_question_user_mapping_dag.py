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

            'INSERT INTO assessment_question_user_mapping (table_unique_key,course_user_assessment_mapping_id,assessment_id,'
            'course_user_mapping_id,assessment_completed,assessment_completed_at,user_assessment_level_hash,'
            'assessment_late_completed,marks_obtained,assessment_started_at,cheated,'
            'cheated_marked_at,mcq_id,option_marked_at,marked_choice,correct_choice,user_question_level_hash)'

            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'

            'on conflict (table_unique_key) do update set course_user_assessment_mapping_id = EXCLUDED.course_user_assessment_mapping_id,'
            'assessment_id = EXCLUDED.assessment_id,'
            'course_user_mapping_id = EXCLUDED.course_user_mapping_id,'
            'assessment_completed = EXCLUDED.assessment_completed,'
            'assessment_completed_at = EXCLUDED.assessment_completed_at,'
            'user_assessment_level_hash = EXCLUDED.user_assessment_level_hash,'
            'assessment_late_completed = EXCLUDED.assessment_late_completed,'
            'marks_obtained = EXCLUDED.marks_obtained,'
            'assessment_started_at = EXCLUDED.assessment_started_at,'
            'cheated = EXCLUDED.cheated,'
            'cheated_marked_at = EXCLUDED.cheated_marked_at,'
            'mcq_id = EXCLUDED.mcq_id,'
            'option_marked_at = EXCLUDED.option_marked_at,'
            'marked_choice = EXCLUDED.marked_choice,'
            'correct_choice = EXCLUDED.correct_choice,'
            'user_question_level_hash = EXCLUDED.user_question_level_hash;',
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
                transform_row[16]
            )
        )
    pg_conn.commit()


dag = DAG(
    'assessment_question_user_mapping_dag',
    default_args=default_args,
    description='Assessment questions (MCQ) and user level data all attempted questions data',
    schedule_interval='20 4 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS assessment_question_user_mapping (
            id serial not null,
            table_unique_key double precision not null PRIMARY KEY,
            course_user_assessment_mapping_id bigint,
            assessment_id bigint,
            course_user_mapping_id bigint,
            assessment_completed boolean,
            assessment_completed_at timestamp,
            user_assessment_level_hash varchar(32),
            assessment_late_completed boolean,
            marks_obtained int,
            assessment_started_at timestamp,
            cheated boolean,
            cheated_marked_at timestamp,
            mcq_id bigint,
            option_marked_at timestamp,
            marked_choice int,
            correct_choice int,
            user_question_level_hash varchar(32)
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''select
    cast(concat(assessments_courseuserassessmentmapping.id, row_number() over (order by assessments_courseuserassessmentmapping.id)) as double precision) as table_unique_key,
    assessments_courseuserassessmentmapping.id as course_user_assessment_mapping_id,
    assessments_courseuserassessmentmapping.assessment_id,
    assessments_courseuserassessmentmapping.course_user_mapping_id,
    assessments_courseuserassessmentmapping.completed as assessment_completed,
    assessments_courseuserassessmentmapping.completed_at as assessment_completed_at,
    assessments_courseuserassessmentmapping.hash as user_assessment_level_hash,
    assessments_courseuserassessmentmapping.late_completed as assessment_late_completed,
    assessments_courseuserassessmentmapping.marks as marks_obtained,
    assessments_courseuserassessmentmapping.started_at as assessment_started_at,
    assessments_courseuserassessmentmapping.cheated,
    assessments_courseuserassessmentmapping.cheated_marked_at,
    assessments_multiplechoicequestioncourseusermapping.multiple_choice_question_id as mcq_id,
    assessments_multiplechoicequestioncourseusermapping.marked_at as option_marked_at,
    assessments_multiplechoicequestioncourseusermapping.marked_choice,
    assessments_multiplechoicequestion.correct_choice,
    assessments_multiplechoicequestioncourseusermapping.hash as user_question_level_hash
from
    assessments_courseuserassessmentmapping
left join assessments_multiplechoicequestioncourseusermapping
    on assessments_multiplechoicequestioncourseusermapping.course_user_assessment_mapping_id = assessments_courseuserassessmentmapping.id
left join assessments_multiplechoicequestion
    on assessments_multiplechoicequestion.id = assessments_multiplechoicequestioncourseusermapping.multiple_choice_question_id;
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