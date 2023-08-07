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
            'INSERT INTO arl_resume_projects (table_unique_key,'
            'user_id,'
            'user_enrollment_status,'
            'course_id,'
            'course_name,'
            'batch_strength,'
            'assignment_id,'
            'assignment_title,'
            'module_name,'
            'assignment_start_time,'
            'assignment_end_time,'
            'project_opening_status,'
            'project_submission_status,'
            'total_questions_received,'
            'questions_opened,'
            'questions_completed)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (table_unique_key) do update set user_id = EXCLUDED.user_id,'
            'user_enrollment_status = EXCLUDED.user_enrollment_status,'
            'course_id = EXCLUDED.course_id,'
            'course_name = EXCLUDED.course_name,'
            'batch_strength = EXCLUDED.batch_strength,'
            'assignment_id = EXCLUDED.assignment_id,'
            'assignment_title = EXCLUDED.assignment_title,'
            'module_name = EXCLUDED.module_name,'
            'assignment_start_time = EXCLUDED.assignment_start_time,'
            'assignment_end_time = EXCLUDED.assignment_end_time,'
            'project_opening_status = EXCLUDED.project_opening_status,'
            'project_submission_status = EXCLUDED.project_submission_status,'
            'total_questions_received = EXCLUDED.total_questions_received,'
            'questions_opened = EXCLUDED.questions_opened,'
            'questions_completed = EXCLUDED.questions_completed;',
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
            )
        )
    pg_conn.commit()


dag = DAG(
    'ARL_Resume_project_DAG',
    default_args=default_args,
    concurrency=4,
    max_active_tasks=6,
    max_active_runs=6,
    description='ARL DAG fetching data from resume projects of react and JS only, topic_id hardcoded',
    schedule_interval='0 1 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS arl_resume_projects (
            id serial,
            table_unique_key varchar(255) not null PRIMARY KEY,
            user_id bigint,
            user_enrollment_status varchar(255),
            course_id int,
            course_name varchar(255),
            batch_strength int,
            assignment_id bigint,
            assignment_title varchar(255),
            module_name varchar(255),
            assignment_start_time date,
            assignment_end_time date,
            project_opening_status varchar(255),
            project_submission_status varchar(255),
            total_questions_received int,
            questions_opened int,
            questions_completed int
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_result_db',
    sql='''SELECT 
	concat(query.user_id,query.assignment_id) as table_unique_key,
	query.user_id,
	case 
		when course_user_mapping.status not in (5,8,9) then 'Other'
		when course_user_mapping.status in (5,8,9) and course_user_mapping.label_id is null then 'Enrolled Student'
		when course_user_mapping.status in (5,8,9) and course_user_mapping.label_id is not null then 'Deferred Student'
	end as user_enrollment_status,	
	query.course_id,
	query.course_name,
	batch_strength_detail.batch_strength,
	query.assignment_id,
	query.assignment_title,
	query.module_name,
	date(query.start_timestamp) as assignment_start_time,
	date(query.end_timestamp) as assignment_end_time,
	case 
		WHEN aqum.assignment_started_at IS NULL THEN 'Not Opened'
        WHEN aqum.assignment_started_at <= query.end_timestamp THEN 'Opened on time'
        WHEN aqum.assignment_started_at > query.end_timestamp THEN 'Opened late'
	end as project_opening_status,
	CASE 
        WHEN aqum.assignment_completed_at IS NULL THEN 'Not Submitted'
        WHEN aqum.assignment_completed_at <= query.end_timestamp THEN 'Submitted on time'
        WHEN aqum.assignment_completed_at > query.end_timestamp THEN 'Submitted late'
    END AS project_submission_status,
    query.total_questions_received,
    COUNT(DISTINCT aqum.question_id) AS questions_opened,
    COUNT(DISTINCT aqum.question_id) FILTER (WHERE aqum.all_test_case_passed is true) AS questions_completed
FROM
    (select
	a.assignment_id,
	a.title as assignment_title,
	a.course_id,
	c.course_name,
	a.start_timestamp,
	a.end_timestamp,
	arqm.user_id,
	case
		when atm.topic_id = 3180 then 'React'
		when atm.topic_id = 3351 then 'JS'
	end as module_name,
	COUNT(distinct arqm.question_id) as total_questions_received
from
	assignments a
join assignment_topic_mapping atm on
	a.assignment_id = atm.assignment_id and a.assignment_id not in (24745, 24820, 24821)
join courses c on
	a.course_id = c.course_id
join assignment_random_question_mapping arqm on
	a.assignment_id = arqm.assignment_id and atm.topic_id in (3180, 3351)
group by 1,2,3,4,5,6,7,8) AS query
LEFT JOIN assignment_question_user_mapping_new aqum 
    ON query.user_id = aqum.user_id AND query.assignment_id = aqum.assignment_id
left join course_user_mapping
	on course_user_mapping.user_id = query.user_id and course_user_mapping.course_id = query.course_id
left join 
	(select 
		course_id,
		count(distinct user_id) filter (where status in (5,8,9) and label_id is null) as batch_strength
	from
		course_user_mapping cum
	group by 1) batch_strength_detail
		on batch_strength_detail.course_id = query.course_id	
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14;
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