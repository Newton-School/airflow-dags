from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime


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
            'INSERT INTO arl_assignment_reported_question (table_unique_key, user_id,'
            'question_report_date,'
            'question_report_date_week,'
            'assignment_question_id,'
            'question_title,'
            'feedback_question_id,'
            'question_text,'
            'inaccurate_difficulty,'
            'question_description_not_clear,'
            'input_unclear_or_incorrect,'
            'required_topics_not_taught,'
            'expected_output_is_inaccurate,'
            'test_cases_missing_or_wrong,'
            'subjective_answer,'
            'topic_template_id,'
            'module_name,'
            'user_type)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (table_unique_key) do update set question_title = EXCLUDED.question_title,'
            'question_text = EXCLUDED.question_text,'
            'inaccurate_difficulty = EXCLUDED.inaccurate_difficulty,'
            'question_description_not_clear = EXCLUDED.question_description_not_clear,'
            'input_unclear_or_incorrect = EXCLUDED.input_unclear_or_incorrect,'
            'required_topics_not_taught = EXCLUDED.required_topics_not_taught,'
            'expected_output_is_inaccurate = EXCLUDED.expected_output_is_inaccurate,'
            'test_cases_missing_or_wrong = EXCLUDED.test_cases_missing_or_wrong,'
            'subjective_answer = EXCLUDED.subjective_answer,'
            'topic_template_id = EXCLUDED.topic_template_id,'
            'module_name = EXCLUDED.module_name,'
            'user_type = EXCLUDED.user_type;',
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
            )
        )
    pg_conn.commit()


dag = DAG(
    'ARL_assignment_reported_question_dag',
    default_args=default_args,
    concurrency=4,
    max_active_tasks=6,
    max_active_runs=6,
    description='An ARL DAG for reported assignment questions and responses given by the user(s)',
    schedule_interval='35 1 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS arl_assignment_reported_question (
            id serial,
            table_unique_key text not null PRIMARY KEY,
            user_id bigint,
            question_report_date date,
            question_report_date_week timestamp,
            assignment_question_id bigint,
            question_title text,
            feedback_question_id int,
            question_text text,
            inaccurate_difficulty varchar(16),
            question_description_not_clear varchar(16),
            input_unclear_or_incorrect varchar(16),
            required_topics_not_taught varchar(16),
            expected_output_is_inaccurate varchar(16),
            test_cases_missing_or_wrong varchar(16),
            subjective_answer text,
            topic_template_id int,
            module_name text,
            user_type text  
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_result_db',
    sql='''with feedback_raw as
        (select
            user_id,
            date(ffar.completed_at) as question_report_date,
            aq.assignment_question_id,
            aq.question_title,
            ffaq.feedback_question_id,
            ffaq.question_text,
            case when ffar.feedback_answer_id = 190 then 'Yes' else null end inaccurate_difficulty,
            case when ffar.feedback_answer_id = 189 then 'Yes' else null end question_description_not_clear,
            case when ffar.feedback_answer_id = 193 then 'Yes' else null end as input_unclear_or_incorrect,
            case when ffar.feedback_answer_id = 191 then 'Yes' else null end as required_topics_not_taught,
            case when ffar.feedback_answer_id = 194 then 'Yes' else null end as expected_output_is_inaccurate,
            case when ffar.feedback_answer_id = 192 then 'Yes' else null end as test_cases_missing_or_wrong,
            case when ffar.feedback_answer_id is null then ffar.feedback_answer end as subjective_answer
        from 
            feedback_form_all_responses_new ffar 
        join feedback_forms_and_questions ffaq 
            on ffaq.feedback_form_id = ffar.feedback_form_id
                and ffar.feedback_form_id = 4419
                	and ffar.feedback_question_id = ffaq.feedback_question_id
        join assignment_question aq
            on aq.assignment_question_id = ffar.entity_object_id 
                and entity_content_type_id = 62
        where ffaq.feedback_question_id in (335, 336)
        order by completed_at desc)
                
        select
        	concat(feedback_raw.user_id,'_',feedback_raw.assignment_question_id,'_',feedback_raw.feedback_question_id,'_',question_report_date) as table_unique_key,
            feedback_raw.user_id,
            question_report_date,
            date_trunc('week', question_report_date) as question_report_date_week,
            feedback_raw.assignment_question_id,
            feedback_raw.question_title,
            feedback_question_id,
            question_text,
            max(inaccurate_difficulty) as inaccurate_difficulty,
            max(question_description_not_clear) as question_description_not_clear,
            max(input_unclear_or_incorrect) as input_unclear_or_incorrect,
            max(required_topics_not_taught) as required_topics_not_taught,
            max(expected_output_is_inaccurate) as expected_output_is_inaccurate,
            max(test_cases_missing_or_wrong) as test_cases_missing_or_wrong,
            max(subjective_answer) as subjective_answer,
            t.topic_template_id,
            t.template_name as module_name,
            case
            	
            	when feedback_raw.user_id in (select distinct user_id from course_user_mapping where status = 27) then 'Instructor'
            	when feedback_raw.user_id in (select distinct user_id from course_user_mapping where status = 13) then 'Mentor'
            	when feedback_raw.user_id in (select distinct user_id from course_user_mapping where status = 25) then 'Mock Interviewer'
            	when feedback_raw.user_id in (select distinct user_id from course_user_mapping where status in (5,8,9)) then 'Enrolled Student'
            	else 'Other'
            end as user_type
        from
            feedback_raw
        left join course_user_mapping
            on course_user_mapping.user_id = feedback_raw.user_id
    	left join assignment_question aq 
			on aq.assignment_question_id = feedback_raw.assignment_question_id
	    left join topics t 
		    on aq.topic_id  = t.topic_id
		    	and t.topic_template_id in (208, 209, 367, 447, 489, 544, 555, 577, 102, 103, 119, 334, 336, 338, 339, 340, 341, 342, 344, 410, 1232)
        group by 1,2,3,4,5,6,7,8,16,17,18;
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