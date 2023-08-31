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
                'INSERT INTO assignments (assignment_id, parent_assignment_id, assignment_sub_type, '
                'assignment_type, course_id, created_at,'
                'created_by_id, duration, start_timestamp,'
                'end_timestamp, hash, hidden, is_group, title, was_competitive,'
                'random_assignment_questions, is_proctored_exam,'
                'whole_course_access, lecture_slot_id, lecture_id,'
                'original_assignment_type,'
                'plagiarism_check_analysis,parent_module_assignment_id, question_count)'
                'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                'on conflict (assignment_id) do update set parent_assignment_id = EXCLUDED.parent_assignment_id,'
                'assignment_sub_type = EXCLUDED.assignment_sub_type,'
                'assignment_type = EXCLUDED.assignment_type,'
                'course_id = EXCLUDED.course_id,'
                'created_at = EXCLUDED.created_at,'
                'created_by_id = EXCLUDED.created_by_id,'
                'duration = EXCLUDED.duration,'
                'start_timestamp = EXCLUDED.start_timestamp,'
                'end_timestamp = EXCLUDED.end_timestamp,'
                'hash = EXCLUDED.hash,'
                'hidden = EXCLUDED.hidden,'
                'is_group = EXCLUDED.is_group,'
                'title = EXCLUDED.title,'
                'was_competitive = EXCLUDED.was_competitive,'
                'random_assignment_questions = EXCLUDED.random_assignment_questions,'
                'is_proctored_exam = EXCLUDED.is_proctored_exam,'
                'whole_course_access = EXCLUDED.whole_course_access,'
                'lecture_slot_id = EXCLUDED.lecture_slot_id,'
                'lecture_id = EXCLUDED.lecture_id,'
                'original_assignment_type = EXCLUDED.original_assignment_type,'
                'plagiarism_check_analysis = EXCLUDED.plagiarism_check_analysis,'
                'parent_module_assignment_id = EXCLUDED.parent_module_assignment_id,'
                'question_count = EXCLUDED.question_count;',
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
                    transform_row[20],
                    transform_row[21],
                    transform_row[22],
                    transform_row[23],

                )
        )
    pg_conn.commit()


dag = DAG(
    'assignments_dag',
    default_args=default_args,
    description='Assignments Details, a version of assignments_assignment',
    schedule_interval='30 20 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS assignments (
            assignment_id bigint not null PRIMARY KEY,
            parent_assignment_id bigint,
            assignment_sub_type int,
            assignment_type int,
            course_id bigint,
            created_at timestamp,
            created_by_id bigint,
            duration interval,
            start_timestamp timestamp,
            end_timestamp timestamp,
            hash varchar(100),
            hidden boolean,
            is_group boolean,
            title varchar(256),
            was_competitive boolean,
            random_assignment_questions boolean,
            is_proctored_exam boolean,
            whole_course_access boolean,
            lecture_slot_id bigint,
            lecture_id bigint,
            original_assignment_type int,
            plagiarism_check_analysis boolean,
            parent_module_assignment_id bigint,
            question_count int 
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''select
    distinct assignments_assignment.id as assignment_id,
    assignments_assignment.parent_assignment_id,
    assignments_assignment.assignment_sub_type,
    assignments_assignment.assignment_type,
    assignments_assignment.course_id,
    assignments_assignment.created_at as created_at,
    assignments_assignment.created_by_id,
    assignments_assignment.duration,
    assignments_assignment.start_timestamp as start_timestamp,
    assignments_assignment.end_timestamp as end_timestamp,
    assignments_assignment.hash,
    assignments_assignment.hidden,
    assignments_assignment.is_group,
    assignments_assignment.title,
    assignments_assignment.was_competitive,
    assignments_assignment.random_assignment_questions,
    assignments_assignment.is_proctored_exam,
    assignments_assignment.whole_course_access,
    assignments_assignment.lecture_slot_id,
    video_sessions_lecture.id as lecture_id,
    assignments_assignment.original_assignment_type,
    assignments_assignment.plagiarism_check_analysis,
    assignments_assignment.parent_module_assignment_id,
    question_count.question_count
    from
        assignments_assignment
    left join video_sessions_lectureslot
        on video_sessions_lectureslot.id = assignments_assignment.lecture_slot_id
    left join video_sessions_lecture
        on video_sessions_lecture.id = video_sessions_lectureslot.lecture_id
    left join  
        (with question_mapping as
    (select 
        assignments_assignment.id as assignment_id,
        count(distinct assignments_assignmentquestionmapping.assignment_question_id) as question_count
    from
        assignments_assignment
    join assignments_assignmentquestionmapping 
        on assignments_assignmentquestionmapping.assignment_id = assignments_assignment.id and assignments_assignment.hidden = false
    group by 1),

topic_mapping as 
    (select 
        assignments_assignment.id as assignment_id,
        sum(aatdnm.number) as question_count
    from
        assignments_assignment
    join assignments_assignmenttopicmapping
        on assignments_assignmenttopicmapping.assignment_id = assignments_assignment.id and assignments_assignment.hidden = false
    join assignments_assignmenttopicdifficultynumbermapping aatdnm
        on aatdnm.assignment_topic_mapping_id = assignments_assignmenttopicmapping.id
    group by 1
    order by 2 desc)

select * from question_mapping

union 

select * from topic_mapping) question_count on question_count.assignment_id = assignments_assignment.id;
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