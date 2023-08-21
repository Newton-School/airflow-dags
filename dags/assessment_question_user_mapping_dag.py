from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from datetime import datetime


default_args = {
    'owner': 'airflow',
    'max_active_tasks': 6,
    'max_active_runs': 6,
    'concurrency': 6,
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 16),
}

assessment_per_dags = Variable.get("assessment_per_dag", 1000)

total_number_of_sub_dags = Variable.get("total_number_of_sub_dags", 10)

total_number_of_extraction_cps_dags = Variable.get("total_number_of_extraction_cps_dags", 10)

dag = DAG(
    'assessment_question_user_mapping_dag',
    description='Assessment questions (MCQ) and user level data all attempted questions data',
    default_args=default_args,
    concurrency=6,
    max_active_tasks=6,
    max_active_runs=6,
    schedule_interval='30 18 * * *',
    catchup=False
)

# Root Level Create Table
create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS assessment_question_user_mapping (
        id serial not null,
        table_unique_key text not null PRIMARY KEY,
        course_user_assessment_mapping_id bigint,
        assessment_attempt_number int,
        assessment_id bigint,
        user_id bigint,
        course_user_mapping_id bigint,
        assessment_completed boolean,
        assessment_completed_at timestamp,
        user_assessment_level_hash varchar(256),
        assessment_late_completed boolean,
        marks_obtained int,
        assessment_started_at timestamp,
        cheated boolean,
        cheated_marked_at timestamp,
        mcq_id bigint,
        option_marked_at timestamp,
        marked_choice int,
        correct_choice int,
        user_question_level_hash varchar(256)
        );
    ''',
    dag=dag
)


# Leaf Level Abstraction
def extract_data_to_nested(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres_result_db')
    pg_conn = pg_hook.get_conn()
    ti = kwargs['ti']
    current_assessment_sub_dag_id = kwargs['current_assessment_sub_dag_id']
    current_cps_sub_dag_id = kwargs['current_cps_sub_dag_id']
    transform_data_output = ti.xcom_pull(
        task_ids=f'transforming_data_{current_assessment_sub_dag_id}.extract_and_transform_individual_assignment_sub_dag_{current_assessment_sub_dag_id}_cps_sub_dag_{current_cps_sub_dag_id}.transform_data')
    for transform_row in transform_data_output:
        pg_cursor = pg_conn.cursor()
        pg_cursor.execute(
        'INSERT INTO assessment_question_user_mapping (table_unique_key, course_user_assessment_mapping_id,'
        'assessment_attempt_number, assessment_id,'
        'user_id, course_user_mapping_id, assessment_completed, assessment_completed_at,'
        'user_assessment_level_hash, assessment_late_completed, marks_obtained, '
        'assessment_started_at, cheated, cheated_marked_at, mcq_id, '
        'option_marked_at, marked_choice, correct_choice, user_question_level_hash)'
        'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        'on conflict (table_unique_key) do update set course_user_assessment_mapping_id = EXCLUDED.course_user_assessment_mapping_id,'
        'assessment_attempt_number = EXCLUDED.assessment_attempt_number,'
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
        'correct_choice = EXCLUDED.correct_choice,user_id = EXCLUDED.user_id,'
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
                transform_row[16],
                transform_row[17],
                transform_row[18],
            )
        )
        pg_conn.commit()
        pg_cursor.close()
    pg_conn.close()


def number_of_rows_per_assignment_sub_dag_func(start_assessment_id, end_assessment_id):
    return PostgresOperator(
        task_id='number_of_rows_per_assignment_sub_dag',
        postgres_conn_id='postgres_read_replica',
        dag=dag,
        sql=''' select count(table_unique_key) from
        (select
            concat(assessments_assessment.id, courses_courseusermapping.user_id, courses_courseusermapping.id, assessments_courseuserassessmentmapping.attempt, assessments_multiplechoicequestioncourseusermapping.multiple_choice_question_id) as table_unique_key,
            assessments_courseuserassessmentmapping.id as course_user_assessment_mapping_id,
            assessments_courseuserassessmentmapping.attempt as assessment_attempt_number,
            assessments_courseuserassessmentmapping.assessment_id,
            courses_courseusermapping.user_id,
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
            assessments_assessment
        join courses_course
            on courses_course.id = assessments_assessment.course_id
        join courses_courseusermapping
            on courses_courseusermapping.course_id = courses_course.id
        join assessments_courseuserassessmentmapping
            on assessments_assessment.id = assessments_courseuserassessmentmapping.assessment_id
                and courses_courseusermapping.id = assessments_courseuserassessmentmapping.course_user_mapping_id and (assessments_assessment.id between %d and %d)
        left join assessments_multiplechoicequestioncourseusermapping
            on assessments_multiplechoicequestioncourseusermapping.course_user_assessment_mapping_id = assessments_courseuserassessmentmapping.id
        left join assessments_multiplechoicequestion
            on assessments_multiplechoicequestion.id = assessments_multiplechoicequestioncourseusermapping.multiple_choice_question_id
        ) query_rows;
            ''' % (start_assessment_id, end_assessment_id),
    )

# Python Limit Offset generator
def limit_offset_generator_func(**kwargs):
    ti = kwargs['ti']
    current_assessment_sub_dag_id = kwargs['current_assessment_sub_dag_id']
    current_cps_sub_dag_id = kwargs['current_cps_sub_dag_id']
    count_cps_rows = ti.xcom_pull(
        task_ids=f'transforming_data_{current_assessment_sub_dag_id}.number_of_rows_per_assignment_sub_dag')
    print(count_cps_rows)
    total_count_rows = count_cps_rows[0][0]
    return {
        "limit": total_count_rows // total_number_of_extraction_cps_dags,
        "offset": current_cps_sub_dag_id * (total_count_rows // total_number_of_extraction_cps_dags) + 1,
    }


# TODO: Add Count Logic
def transform_data_per_query(start_assessment_id, end_assessment_id, cps_sub_dag_id, current_assessment_sub_dag_id):
    return PostgresOperator(
        task_id='transform_data',
        postgres_conn_id='postgres_read_replica',
        dag=dag,
        params={
            'current_cps_sub_dag_id': cps_sub_dag_id,
            'current_assessment_sub_dag_id': current_assessment_sub_dag_id,
            'task_key': f'transforming_data_{current_assessment_sub_dag_id}.extract_and_transform_individual_assignment_sub_dag_{current_assessment_sub_dag_id}_cps_sub_dag_{cps_sub_dag_id}.limit_offset_generator'
        },
        sql=''' select * from
        (select
            concat(assessments_assessment.id, courses_courseusermapping.user_id, courses_courseusermapping.id, assessments_courseuserassessmentmapping.attempt, assessments_multiplechoicequestioncourseusermapping.multiple_choice_question_id) as table_unique_key,
            assessments_courseuserassessmentmapping.id as course_user_assessment_mapping_id,
            assessments_courseuserassessmentmapping.attempt as assessment_attempt_number,
            assessments_courseuserassessmentmapping.assessment_id,
            courses_courseusermapping.user_id,
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
            assessments_assessment
        join courses_course
            on courses_course.id = assessments_assessment.course_id
        join courses_courseusermapping
            on courses_courseusermapping.course_id = courses_course.id
        join assessments_courseuserassessmentmapping
            on assessments_assessment.id = assessments_courseuserassessmentmapping.assessment_id
                and courses_courseusermapping.id = assessments_courseuserassessmentmapping.course_user_mapping_id and (assessments_assessment.id between %d and %d)
        left join assessments_multiplechoicequestioncourseusermapping
            on assessments_multiplechoicequestioncourseusermapping.course_user_assessment_mapping_id = assessments_courseuserassessmentmapping.id
        left join assessments_multiplechoicequestion
            on assessments_multiplechoicequestion.id = assessments_multiplechoicequestioncourseusermapping.multiple_choice_question_id
        ) final_query
        limit {{ ti.xcom_pull(task_ids=params.task_key, key='return_value').limit }} 
        offset {{ ti.xcom_pull(task_ids=params.task_key, key='return_value').offset }}
        ;
            ''' % (start_assessment_id, end_assessment_id),
    )


for assessment_sub_dag_id in range(int(total_number_of_sub_dags)):
    with TaskGroup(group_id=f"transforming_data_{assessment_sub_dag_id}", dag=dag) as assessment_sub_dag_task_group:
        assessment_start_id = assessment_sub_dag_id * int(assessment_per_dags) + 1
        assessment_end_id = (assessment_sub_dag_id + 1) * int(assessment_per_dags)
        number_of_rows_per_assignment_sub_dag = number_of_rows_per_assignment_sub_dag_func(assessment_start_id,assessment_end_id)

        for cps_sub_dag_id in range(int(total_number_of_extraction_cps_dags)):
            with TaskGroup(
                    group_id=f"extract_and_transform_individual_assignment_sub_dag_{assessment_sub_dag_id}_cps_sub_dag_{cps_sub_dag_id}",
                    dag=dag) as cps_sub_dag:
                limit_offset_generator = PythonOperator(
                    task_id='limit_offset_generator',
                    python_callable=limit_offset_generator_func,
                    provide_context=True,
                    op_kwargs={
                        'current_assessment_sub_dag_id': assessment_sub_dag_id,
                        'current_cps_sub_dag_id': cps_sub_dag_id,
                    },
                    dag=dag,
                )

                transform_data = transform_data_per_query(assessment_start_id, assessment_end_id, cps_sub_dag_id,assessment_sub_dag_id)

                extract_python_data = PythonOperator(
                    task_id='extract_python_data',
                    python_callable=extract_data_to_nested,
                    provide_context=True,
                    op_kwargs={
                        'current_assessment_sub_dag_id': assessment_sub_dag_id,
                        'current_cps_sub_dag_id': cps_sub_dag_id
                    },
                    dag=dag,
                )

                limit_offset_generator >> transform_data >> extract_python_data

            number_of_rows_per_assignment_sub_dag >> cps_sub_dag

    create_table >> assessment_sub_dag_task_group
