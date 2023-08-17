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
            'INSERT INTO arl_assessments_x_user (table_unique_key, user_id,'
            'student_name, lead_type, label_mapping_status, '
            'course_id, course_name, student_category, course_structure_class, '
            'assessment_id, assessment_title, assessment_type, assessment_sub_type, generation_and_creation_type,'
            'assessment_class, assessment_release_date,'
            'assessment_open_date, assessment_submission_date, assessment_attempt_status,'
            'assessment_submission_status, question_count, questions_marked, questions_correct,'
            'max_marks, marks_obtained, cheated, activity_status_7_days,'
            'activity_status_14_days,'
            'activity_status_30_days,'
            'user_placement_status)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (table_unique_key) do update set student_name = EXCLUDED.student_name,'
            'lead_type = EXCLUDED.lead_type,'
            'label_mapping_status = EXCLUDED.label_mapping_status,'
            'course_name = EXCLUDED.course_name,'
            'student_category = EXCLUDED.student_category,'
            'course_structure_class = EXCLUDED.course_structure_class,'
            'assessment_title = EXCLUDED.assessment_title,'
            'assessment_type = EXCLUDED.assessment_type,'
            'assessment_sub_type = EXCLUDED.assessment_sub_type,'
            'generation_and_creation_type = EXCLUDED.generation_and_creation_type,'
            'assessment_class = EXCLUDED.assessment_class,'
            'assessment_release_date = EXCLUDED.assessment_release_date,'
            'assessment_open_date = EXCLUDED.assessment_open_date,'
            'assessment_submission_date = EXCLUDED.assessment_submission_date,'
            'assessment_attempt_status = EXCLUDED.assessment_attempt_status,'
            'assessment_submission_status = EXCLUDED.assessment_submission_status,'
            'question_count = EXCLUDED.question_count,'
            'questions_marked = EXCLUDED.questions_marked,'
            'questions_correct = EXCLUDED.questions_correct,'
            'max_marks = EXCLUDED.max_marks,'
            'marks_obtained = EXCLUDED.marks_obtained,'
            'cheated = EXCLUDED.cheated,'
            'activity_status_7_days = EXCLUDED.activity_status_7_days,'
            'activity_status_14_days = EXCLUDED.activity_status_14_days,'
            'activity_status_30_days = EXCLUDED.activity_status_30_days,'
            'user_placement_status = EXCLUDED.user_placement_status;',
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
                transform_row[24],
                transform_row[25],
                transform_row[26],
                transform_row[27],
                transform_row[28],
                transform_row[29],
            )
        )
    pg_conn.commit()


dag = DAG(
    'ARL_Assessments_x_user',
    default_args=default_args,
    concurrency=4,
    max_active_tasks=6,
    max_active_runs=6,
    description='An Analytics Reporting Layer DAG for Assessments x user level',
    schedule_interval='35 0 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS arl_assessments_x_user (
            id serial,
            table_unique_key text not null PRIMARY KEY,
            user_id bigint,
            student_name text,
            lead_type text,
            label_mapping_status text,
            course_id int,
            course_name text,
            student_category text,
            course_structure_class text,
            assessment_id bigint,
            assessment_title varchar(256),
            assessment_type varchar(32),
            assessment_sub_type varchar(16),
            generation_and_creation_type varchar(256),
            assessment_class varchar(256),
            assessment_release_date DATE,
            assessment_open_date DATE,
            assessment_submission_date DATE,
            assessment_attempt_status varchar(32),
            assessment_submission_status varchar(32),
            question_count int,
            questions_marked int,
            questions_correct int,
            max_marks int,
            marks_obtained int,
            cheated boolean,
            activity_status_7_days text,
            activity_status_14_days text,
            activity_status_30_days text,
            user_placement_status text
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_result_db',
    sql='''select
            concat(assessments.course_id, assessments.assessment_id, course_user_mapping.user_id) as table_unique_key,
            course_user_mapping.user_id,
            concat(ui.first_name,' ', ui.last_name) as student_name,
            ui.lead_type,
            case 
                when course_user_mapping.label_id is null and course_user_mapping.status in (8,9) then 'Enrolled Student'
                when course_user_mapping.label_id is not null and course_user_mapping.status in (8,9) then 'Label Marked Student'
                when courses.course_structure_id in (1,18) and course_user_mapping.status in (11,12) then 'ISA Cancelled Student'
                when courses.course_structure_id not in (1,18) and course_user_mapping.status in (30) then 'Deferred Student'
                when courses.course_structure_id not in (1,18) and course_user_mapping.status in (11) then 'Foreclosed Student'
                when courses.course_structure_id not in (1,18) and course_user_mapping.status in (12) then 'Reject by NS-Ops'
                else 'Mapping Error'
            end as label_mapping_status,
            courses.course_id,
            courses.course_name,
            cucm.student_category,
            courses.course_structure_class,
            assessments.assessment_id,
            assessments.title as assessment_title,
            case
                when assessments.assessment_type = 1 then 'Normal Assessment'
                when assessments.assessment_type = 2 then 'Filtering Assessment'
                when assessments.assessment_type = 3 then 'Competitive Assessment'
                when assessments.assessment_type = 4 then 'Duration Assessment'
                when assessments.assessment_type = 5 then 'Poll Assessment'
            end as assessment_type,
            case 
                when assessments.sub_type = 1 then 'General'
                when assessments.sub_type = 2 then 'In-Class'
                when assessments.sub_type = 3 then 'Post-Class'
                when assessments.sub_type = 4 then 'Classroom-Quiz'
            end as assessment_sub_type,
            case 
            	when assessments.generation_and_creation_type = 1 then 'Fully Automated'
            	when assessments.generation_and_creation_type = 2 then 'Manually Released'
            	when assessments.generation_and_creation_type = 3 then 'Manually Created'
            	when assessments.generation_and_creation_type = 4 then 'Fully Manual'
            end as generation_and_creation_type,
            case 
            	when (assessments.assessment_type = 1 and assessments.sub_type = 1 and assessments.generation_and_creation_type = 1) then 'General Quiz'
                when (assessments.assessment_type = 1 and assessments.sub_type = 2 and assessments.generation_and_creation_type = 1) then 'In-Class Automated Quiz'
            	when (assessments.assessment_type = 1 and assessments.sub_type = 3 and assessments.generation_and_creation_type = 1) then 'Post-Class Automated Quiz'
            	when assessments.assessment_type = 1 and assessments.generation_and_creation_type = 4 then 'Instructor Quiz'
            	when assessments.assessment_type = 1 and assessments.generation_and_creation_type = 2 then 'Classroom Quiz'
            	when assessments.assessment_type = 2 then 'Filtering Assessment'
            	when assessments.assessment_type in (3,4) then 'MCQ-Contest'
            	when assessments.assessment_type = 5 then 'Poll Assessment'
            end as assessment_class,
            date(assessments.start_timestamp) as assessment_release_date,
            date(assessment_question_user_mapping.assessment_started_at) as assessment_open_date,
            date(assessment_question_user_mapping.assessment_completed_at) as assessment_submission_date,
            case 
                when assessment_question_user_mapping.assessment_started_at <= assessments.end_timestamp then 'Attempted On Time'
                when assessment_question_user_mapping.assessment_started_at > assessments.end_timestamp then 'Late Attempt'
                when assessment_question_user_mapping.assessment_started_at is null then 'Not Attempted'
            end as assessment_attempt_status,
            case 
                when assessment_late_completed = false then 'On Time Submission'
                when assessment_late_completed = true then 'Late Submission'
                when assessment_late_completed is null then 'Not Submitted'
            end as assessment_submission_status,
            assessments.question_count,
            count(distinct mcq_id) filter (where option_marked_at is not null) as questions_marked,
            count(distinct mcq_id) filter (where (marked_choice - correct_choice) = 0) as questions_correct,
            assessments.max_marks,
            assessment_question_user_mapping.marks_obtained,
            assessment_question_user_mapping.cheated,
            uasm.activity_status_7_days,
            uasm.activity_status_14_days,
            uasm.activity_status_30_days,
            course_user_mapping.user_placement_status
        from
            assessments
        join courses
            on courses.course_id = assessments.course_id and course_structure_id in (1,6,8,11,12,14,18,19,20,22,23,26,34) and date(assessments.start_timestamp) >= '2022-07-01'
        		and courses.course_id in (select distinct wab.lu_course_id from wow_active_batches wab)
        join course_user_mapping
            on course_user_mapping.course_id = courses.course_id and status in (8,9,11,12,30)
        join users_info ui 
        	on ui.user_id = course_user_mapping.user_id
        left join course_user_category_mapping cucm 
        	on cucm.course_id = courses.course_id and cucm.user_id = course_user_mapping.user_id 
        left join assessment_question_user_mapping
            on assessment_question_user_mapping.assessment_id = assessments.assessment_id 
            	and assessment_question_user_mapping.course_user_mapping_id = course_user_mapping.course_user_mapping_id
        left join user_activity_status_mapping uasm 
        	on uasm.user_id = course_user_mapping.user_id
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,24,25,26,27,28,29,30;
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