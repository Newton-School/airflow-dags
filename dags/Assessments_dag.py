from airflow import DAG
# from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from datetime import datetime

from sqlalchemy_utils.types.enriched_datetime.pendulum_date import pendulum

default_args = {
    'owner': 'airflow',
    'max_active_tasks': 6,
    'max_active_runs': 6,
    'concurrency': 4,
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

            'INSERT INTO assessments (assessment_id,created_at,hash,'
            'start_timestamp,end_timestamp,title,course_id,hidden,is_proctored_exam,'
            'max_marks,max_attempts,assessment_type,generation_and_creation_type,lecture_slot_id,lecture_id,'
            'was_competitive,random_multiple_choice_questions,sub_type,preserve_question_sequence,'
            'assessment_mapping_type,question_count)'
            
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            
            'on conflict (assessment_id) do update set start_timestamp = EXCLUDED.start_timestamp,'
            'end_timestamp = EXCLUDED.end_timestamp,'
            'title = EXCLUDED.title,'
            'hidden = EXCLUDED.hidden,'
            'is_proctored_exam = EXCLUDED.is_proctored_exam,'
            'max_marks = EXCLUDED.max_marks,'
            'max_attempts = EXCLUDED.max_attempts,'
            'assessment_type = EXCLUDED.assessment_type,'
            'generation_and_creation_type = EXCLUDED.generation_and_creation_type,'
            'lecture_slot_id = EXCLUDED.lecture_slot_id,'
            'lecture_id = EXCLUDED.lecture_id,'
            'was_competitive = EXCLUDED.was_competitive,'
            'random_multiple_choice_questions = EXCLUDED.random_multiple_choice_questions,'
            'sub_type = EXCLUDED.sub_type,'
            'preserve_question_sequence = EXCLUDED.preserve_question_sequence,'
            'assessment_mapping_type = EXCLUDED.assessment_mapping_type,'
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
                transform_row[20]
            )
        )
    pg_conn.commit()


dag = DAG(
    'assessment_table_dag',
    default_args=default_args,
    concurrency=4,
    max_active_tasks=6,
    max_active_runs=6,
    description='Assessments all details and question release count per assessment',
    schedule_interval='30 20 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS assessments (
            id serial,
            assessment_id bigint not null PRIMARY KEY,
            created_at timestamp,
            hash varchar(64),
            start_timestamp timestamp,
            end_timestamp timestamp,
            title varchar(256),
            course_id int,
            hidden boolean,
            is_proctored_exam boolean,
            max_marks int,
            max_attempts int,
            assessment_type int,
            generation_and_creation_type int,
            lecture_slot_id bigint,
            lecture_id bigint,
            was_competitive boolean,
            random_multiple_choice_questions boolean,
            sub_type int,
            preserve_question_sequence boolean,
            assessment_mapping_type varchar(64),
            question_count int
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''with assessment_details as
    (select
        assessments_assessment.id as assessment_id,
        assessments_assessment.created_at,
        assessments_assessment.hash,
        assessments_assessment.start_timestamp,
        assessments_assessment.end_timestamp,
        assessments_assessment.title,
        assessments_assessment.course_id,
        assessments_assessment.hidden,
        assessments_assessment.is_proctored_exam,
        assessments_assessment.max_marks,
        assessments_assessment.max_attempts,
        assessments_assessment.assessment_type,
        assessments_assessment.generation_and_creation_type, 
        assessments_assessment.lecture_slot_id,
        video_sessions_lectureslot.lecture_id,
        assessments_assessment.was_competitive,
        assessments_assessment.random_multiple_choice_questions,
        assessments_assessment.sub_type,
        assessments_assessment.preserve_question_sequence
        
    from
        assessments_assessment
    left join video_sessions_lectureslot
        on video_sessions_lectureslot.id = assessments_assessment.lecture_slot_id
    order by 1 desc),

question_count as 

    (select * from

            ((select distinct
                    assessments_assessment.id as assessment_id,
                    'Question Mapping' as assessment_mapping_type,
                    count(distinct assessments_assessmentmultiplechoicequestionmapping.multiple_choice_question_id) as question_count
            from
                assessments_assessment
            join assessments_assessmentmultiplechoicequestionmapping
                on assessments_assessmentmultiplechoicequestionmapping.assessment_id = assessments_assessment.id
            group by 1,2)
            
            union 
            
            (select distinct
                assessments_assessment.id as assessment_id,
                'Label Mapping' as assessment_mapping_type,
                sum(assessments_assessmentlabellevelnumbermapping.number) as question_count
            from
                assessments_assessment
            join assessments_assessmentlabelmapping
                on assessments_assessmentlabelmapping.assessment_id = assessments_assessment.id
            join assessments_assessmentlabellevelnumbermapping
                on assessments_assessmentlabellevelnumbermapping.assessment_label_mapping_id = assessments_assessmentlabelmapping.id
            group by 1,2)
            
            union 
            
            (select distinct
                assessments_assessment.id as assessment_id,
                'Topic Mapping' as assessment_mapping_type,
                sum(assessments_assessmenttopiclevelnumbermapping.number) as question_count
            from
                assessments_assessment
                join assessments_assessmenttopicmapping
                    on assessments_assessmenttopicmapping.assessment_id = assessments_assessment.id
                join assessments_assessmenttopiclevelnumbermapping
                    on assessments_assessmenttopiclevelnumbermapping.assessment_topic_mapping_id = assessments_assessmenttopicmapping.id
            group by 1,2)) all_assessments_data),

/*this query is required to ensure uniqueness at assessment_id level; while fetching data at assessment_id with question_count CTE; same assessment_id(s) have mapping(s) with label_mapped assessments and topic_mapped assessments
and query resolves that, probably!*/
   
drop_query as
    (select
        assessment_id, 
        count(assessment_mapping_type) as check_value
    from
        question_count 
    group by 1 
    order by 2 desc, 1 desc)

select
    assessment_details.*,
    question_count.assessment_mapping_type,
    question_count.question_count
from
    assessment_details
left join question_count
    on assessment_details.assessment_id = question_count.assessment_id
where assessment_details.assessment_id not in (select distinct assessment_id from drop_query where check_value > 1);
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