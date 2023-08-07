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
            'INSERT INTO arl_assignment_question_level (table_unique_key,assignment_id,assignment_name,'
            'start_timestamp,assignment_type,course_id,question_id,question_title,'
            'difficulty_type,topic_id,module_name,opened_in_7_days,history_based_opened_in_7_days,'
            'attempted_in_7_days,history_based_attempted_in_7_days,completed_in_7_days,'
            'history_based_completed_in_7_days,opened_in_8_to_10_days,history_based_opened_in_8_to_10_days,'
            'attempted_in_8_to_10_days,history_based_attempted_in_8_to_10_days,completed_in_8_to_10_days,'
            'history_based_completed_in_8_to_10_days,opened_in_11_to_14_days,history_based_opened_in_11_to_14_days,'
            'attempted_in_11_to_14_days,history_based_attempted_in_11_to_14_days,completed_in_11_to_14_days,'
            'history_based_completed_in_11_to_14_days,opened_in_14_plus_days,history_based_opened_in_14_plus_days,'
            'attempted_in_14_plus_days,history_based_attempted_in_14_plus_days,completed_in_14_plus_days,'
            'history_based_completed_in_14_plus_days,users_with_plagiarism_more_than_99_percent,'
            'history_based_users_with_plagiarism_more_than_99_percent,users_with_plagiarism_more_than_95_percent,'
            'history_based_users_with_plagiarism_more_than_95_percent,users_with_plagiarism_more_than_90_percent,'
            'history_based_users_with_plagiarism_more_than_90_percent)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (table_unique_key) do update set assignment_name=EXCLUDED.assignment_name,'
            'start_timestamp=EXCLUDED.start_timestamp,assignment_type=EXCLUDED.assignment_type,'
            'course_id=EXCLUDED.course_id,question_title=EXCLUDED.question_title,'
            'difficulty_type=EXCLUDED.difficulty_type,topic_id=EXCLUDED.topic_id,module_name=EXCLUDED.module_name,'
            'opened_in_7_days=EXCLUDED.opened_in_7_days,'
            'history_based_opened_in_7_days=EXCLUDED.history_based_opened_in_7_days,'
            'attempted_in_7_days=EXCLUDED.attempted_in_7_days,'
            'history_based_attempted_in_7_days=EXCLUDED.history_based_attempted_in_7_days,'
            'completed_in_7_days=EXCLUDED.completed_in_7_days,'
            'history_based_completed_in_7_days=EXCLUDED.history_based_completed_in_7_days,'
            'opened_in_8_to_10_days=EXCLUDED.opened_in_8_to_10_days,'
            'history_based_opened_in_8_to_10_days=EXCLUDED.history_based_opened_in_8_to_10_days,'
            'attempted_in_8_to_10_days=EXCLUDED.attempted_in_8_to_10_days,'
            'history_based_attempted_in_8_to_10_days=EXCLUDED.history_based_attempted_in_8_to_10_days,'
            'completed_in_8_to_10_days=EXCLUDED.completed_in_8_to_10_days,'
            'history_based_completed_in_8_to_10_days=EXCLUDED.history_based_completed_in_8_to_10_days,'
            'opened_in_11_to_14_days=EXCLUDED.opened_in_11_to_14_days,'
            'history_based_opened_in_11_to_14_days=EXCLUDED.history_based_opened_in_11_to_14_days,'
            'attempted_in_11_to_14_days=EXCLUDED.attempted_in_11_to_14_days,'
            'history_based_attempted_in_11_to_14_days=EXCLUDED.history_based_attempted_in_11_to_14_days,'
            'completed_in_11_to_14_days=EXCLUDED.completed_in_11_to_14_days,'
            'history_based_completed_in_11_to_14_days=EXCLUDED.history_based_completed_in_11_to_14_days,'
            'opened_in_14_plus_days=EXCLUDED.opened_in_14_plus_days,'
            'history_based_opened_in_14_plus_days=EXCLUDED.history_based_opened_in_14_plus_days,'
            'attempted_in_14_plus_days=EXCLUDED.attempted_in_14_plus_days,'
            'history_based_attempted_in_14_plus_days=EXCLUDED.history_based_attempted_in_14_plus_days,'
            'completed_in_14_plus_days=EXCLUDED.completed_in_14_plus_days,'
            'history_based_completed_in_14_plus_days=EXCLUDED.history_based_completed_in_14_plus_days,'
            'users_with_plagiarism_more_than_99_percent=EXCLUDED.users_with_plagiarism_more_than_99_percent,'
            'history_based_users_with_plagiarism_more_than_99_percent=EXCLUDED.history_based_users_with_plagiarism_more_than_99_percent,'
            'users_with_plagiarism_more_than_95_percent=EXCLUDED.users_with_plagiarism_more_than_95_percent,'
            'history_based_users_with_plagiarism_more_than_95_percent=EXCLUDED.history_based_users_with_plagiarism_more_than_95_percent,'
            'users_with_plagiarism_more_than_90_percent=EXCLUDED.users_with_plagiarism_more_than_90_percent,'
            'history_based_users_with_plagiarism_more_than_90_percent=EXCLUDED.history_based_users_with_plagiarism_more_than_90_percent ;',
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
                transform_row[30],
                transform_row[31],
                transform_row[32],
                transform_row[33],
                transform_row[34],
                transform_row[35],
                transform_row[36],
                transform_row[37],
                transform_row[38],
                transform_row[39],
                transform_row[40],
            )
        )
    pg_conn.commit()


dag = DAG(
    'ARL_assignment_question_level',
    default_args=default_args,
    description='An Analytics Reporting Layer DAG for Assignment question level cut',
    schedule_interval='01 0 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS arl_assignment_question_level (
            id serial,
            table_unique_key double precision not null PRIMARY KEY,
            assignment_id int,
            assignment_name varchar(256),
            start_timestamp TIMESTAMP,
            assignment_type varchar(64),
            course_id int,
            question_id int,
            question_title varchar(256),
            difficulty_type varchar(10),
            topic_id int,
            module_name varchar(256),
            opened_in_7_days int,
            history_based_opened_in_7_days int,
            attempted_in_7_days int,
            history_based_attempted_in_7_days int,
            completed_in_7_days int,
            history_based_completed_in_7_days int,
            opened_in_8_to_10_days int,
            history_based_opened_in_8_to_10_days int,
            attempted_in_8_to_10_days int,
            history_based_attempted_in_8_to_10_days int,
            completed_in_8_to_10_days int,
            history_based_completed_in_8_to_10_days int,
            opened_in_11_to_14_days int,
            history_based_opened_in_11_to_14_days int,
            attempted_in_11_to_14_days int,
            history_based_attempted_in_11_to_14_days int,
            completed_in_11_to_14_days int,
            history_based_completed_in_11_to_14_days int,
            opened_in_14_plus_days int,
            history_based_opened_in_14_plus_days int,
            attempted_in_14_plus_days int,
            history_based_attempted_in_14_plus_days int,
            completed_in_14_plus_days int,
            history_based_completed_in_14_plus_days int,
            users_with_plagiarism_more_than_99_percent int,
            history_based_users_with_plagiarism_more_than_99_percent int,
            users_with_plagiarism_more_than_95_percent int,
            history_based_users_with_plagiarism_more_than_95_percent int,
            users_with_plagiarism_more_than_90_percent int,
            history_based_users_with_plagiarism_more_than_90_percent int
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_result_db',
    sql='''With history_based_question_detail as 
                (select 
                a.assignment_id,
                a.title as assignment_name,
                a.start_timestamp,
                case when a.assignment_type = 1 then 'Normal Assignment'
                            when a.assignment_type = 2 then 'Filtering Assignment'
                            when a.assignment_type = 3 then 'Competitive Assignment'
                            when a.assignment_type = 4 then 'Duration Assignment'
                            when a.assignment_type = 5 then 'Milestone Assignment'
                            when a.assignment_type = 6 then 'Question of the Day Assignment'
                            end as assignment_type,
                a.course_id ,
                c.course_name,
                aqm.question_id ,
                aq.question_title ,
                case 
                when aq.difficulty_type = 1 then 'Beginner'
                when aq.difficulty_type = 2 then 'Easy'
                when aq.difficulty_type = 3 then 'Medium'
                when aq.difficulty_type = 4 then 'Hard'
                when aq.difficulty_type = 5 then 'Challenge' end as difficulty_type,
                aq.topic_id ,
                t.template_name as module_name,
                count(distinct aqum.user_id) filter (where aqum.assignment_started_at is not null and aqum.question_started_at <= a.start_timestamp + interval '7 days') as opened_in_7_days,
                count(distinct aqum.user_id) filter (where aqum.assignment_started_at is not null and aqum.max_test_case_passed is not null and aqum.question_started_at <= a.start_timestamp + interval '7 days') as attempted_in_7_days,
                count(distinct aqum.user_id) filter (where aqum.all_test_case_passed is true and aqum.question_completed_at  <= a.start_timestamp + interval '7 days') as completed_in_7_days,
                count(distinct aqum.user_id) filter (where aqum.assignment_started_at is not null and aqum.question_started_at > a.start_timestamp + interval '7 days' and aqum.question_started_at <= a.start_timestamp + interval '10 days') as opened_in_8_to_10_days,
                count(distinct aqum.user_id) filter (where aqum.assignment_started_at is not null and aqum.max_test_case_passed is not null and aqum.question_started_at > a.start_timestamp + interval '7 days' and aqum.question_started_at <= a.start_timestamp + interval '10 days') as attempted_in_8_to_10_days,
                count(distinct aqum.user_id) filter (where aqum.all_test_case_passed is true and aqum.question_completed_at > a.start_timestamp + interval '7 days' and aqum.question_completed_at <= a.start_timestamp + interval '10 days') as completed_in_8_to_10_days,
                count(distinct aqum.user_id) filter (where aqum.assignment_started_at is not null and aqum.question_started_at > a.start_timestamp + interval '10 days' and aqum.question_started_at <= a.start_timestamp + interval '14 days') as opened_in_11_to_14_days,
                count(distinct aqum.user_id) filter (where aqum.assignment_started_at is not null and aqum.max_test_case_passed is not null and aqum.question_started_at > a.start_timestamp + interval '10 days' and aqum.question_started_at <= a.start_timestamp + interval '14 days') as attempted_in_11_to_14_days,
                count(distinct aqum.user_id) filter (where aqum.all_test_case_passed is true and aqum.question_completed_at > a.start_timestamp + interval '10 days' and aqum.question_completed_at <= a.start_timestamp + interval '14 days') as completed_in_11_to_14_days,
                count(distinct aqum.user_id) filter (where aqum.assignment_started_at is not null and aqum.question_started_at > a.start_timestamp + interval '14 days') as opened_in_14_plus_days,
                count(distinct aqum.user_id) filter (where aqum.assignment_started_at is not null and aqum.max_test_case_passed is not null and aqum.question_started_at > a.start_timestamp + interval '14 days') as attempted_in_14_plus_days,
                count(distinct aqum.user_id) filter (where aqum.all_test_case_passed is true and aqum.question_completed_at > a.start_timestamp + interval '14 days') as completed_in_14_plus_days,
                count(distinct aqum.user_id) filter (where aqum.plagiarism_score >= 0.99) as users_with_plagiarism_more_than_99_percent,
                count(distinct aqum.user_id) filter (where aqum.plagiarism_score >= 0.95) as users_with_plagiarism_more_than_95_percent,
                count(distinct aqum.user_id) filter (where aqum.plagiarism_score >= 0.90) as users_with_plagiarism_more_than_90_percent
                from assignments a
                left join courses c  on c.course_id = a.course_id 
                left join assignment_question_mapping aqm on aqm.assignment_id = a.assignment_id 
                left join assignment_question aq  on aq.assignment_question_id  = aqm.question_id 
                left join topics t on t.topic_id = aq.topic_id and t.topic_template_id in (102,103,119,334,336,338,339,340,341,342,344)
                left join assignment_question_user_mapping_new aqum on aqum.assignment_id = aqm.assignment_id and aqum.question_id = aqm.question_id 
                join (select distinct
                                    wud.course_user_mapping_id,
                                    wud.user_id ,
                                    c.course_id,
                                    wud.week_view ,
                                    wud.status
                                from
                                    weekly_user_details wud 
                                join courses c 
                                    on c.course_id = wud.course_id and c.course_structure_id in (1,6,8,11,12,14,18,19,20,22,23,26,32)
                                    and wud.label_mapping_id is null and wud.status in (5,8,9) and wud.unit_type like 'LEARNING') as mod_cum
                        on a.course_id = mod_cum.course_id and date_trunc('week',a.start_timestamp) = mod_cum.week_view
                group by 1,2,3,4,5,6,7,8,9,10,11),
                question_detail as (
                select 
                concat(a.assignment_id,a.course_id,aqm.question_id) as table_unique_key,
                a.assignment_id,
                a.title as assignment_name,
                a.start_timestamp,
                case when a.assignment_type = 1 then 'Normal Assignment'
                            when a.assignment_type = 2 then 'Filtering Assignment'
                            when a.assignment_type = 3 then 'Competitive Assignment'
                            when a.assignment_type = 4 then 'Duration Assignment'
                            when a.assignment_type = 5 then 'Milestone Assignment'
                            when a.assignment_type = 6 then 'Question of the Day Assignment'
                            end as assignment_type,
                a.course_id ,
                c.course_name,
                aqm.question_id ,
                aq.question_title ,
                case 
                when aq.difficulty_type = 1 then 'Beginner'
                when aq.difficulty_type = 2 then 'Easy'
                when aq.difficulty_type = 3 then 'Medium'
                when aq.difficulty_type = 4 then 'Hard'
                when aq.difficulty_type = 5 then 'Challenge' end as difficulty_type,
                aq.topic_id ,
                t.template_name as module_name,
                count(distinct aqum.user_id) filter (where aqum.assignment_started_at is not null and aqum.question_started_at <= a.start_timestamp + interval '7 days') as opened_in_7_days,
                count(distinct aqum.user_id) filter (where aqum.assignment_started_at is not null and aqum.max_test_case_passed is not null and aqum.question_started_at <= a.start_timestamp + interval '7 days') as attempted_in_7_days,
                count(distinct aqum.user_id) filter (where aqum.all_test_case_passed is true and aqum.question_completed_at  <= a.start_timestamp + interval '7 days') as completed_in_7_days,
                count(distinct aqum.user_id) filter (where aqum.assignment_started_at is not null and aqum.question_started_at > a.start_timestamp + interval '7 days' and aqum.question_started_at <= a.start_timestamp + interval '10 days') as opened_in_8_to_10_days,
                count(distinct aqum.user_id) filter (where aqum.assignment_started_at is not null and aqum.max_test_case_passed is not null and aqum.question_started_at > a.start_timestamp + interval '7 days' and aqum.question_started_at <= a.start_timestamp + interval '10 days') as attempted_in_8_to_10_days,
                count(distinct aqum.user_id) filter (where aqum.all_test_case_passed is true and aqum.question_completed_at > a.start_timestamp + interval '7 days' and aqum.question_completed_at <= a.start_timestamp + interval '10 days') as completed_in_8_to_10_days,
                count(distinct aqum.user_id) filter (where aqum.assignment_started_at is not null and aqum.question_started_at > a.start_timestamp + interval '10 days' and aqum.question_started_at <= a.start_timestamp + interval '14 days') as opened_in_11_to_14_days,
                count(distinct aqum.user_id) filter (where aqum.assignment_started_at is not null and aqum.max_test_case_passed is not null and aqum.question_started_at > a.start_timestamp + interval '10 days' and aqum.question_started_at <= a.start_timestamp + interval '14 days') as attempted_in_11_to_14_days,
                count(distinct aqum.user_id) filter (where aqum.all_test_case_passed is true and aqum.question_completed_at > a.start_timestamp + interval '10 days' and aqum.question_completed_at <= a.start_timestamp + interval '14 days') as completed_in_11_to_14_days,
                count(distinct aqum.user_id) filter (where aqum.assignment_started_at is not null and aqum.question_started_at > a.start_timestamp + interval '14 days') as opened_in_14_plus_days,
                count(distinct aqum.user_id) filter (where aqum.assignment_started_at is not null and aqum.max_test_case_passed is not null and aqum.question_started_at > a.start_timestamp + interval '14 days') as attempted_in_14_plus_days,
                count(distinct aqum.user_id) filter (where aqum.all_test_case_passed is true and aqum.question_completed_at > a.start_timestamp + interval '14 days') as completed_in_14_plus_days,
                count(distinct aqum.user_id) filter (where aqum.plagiarism_score >= 0.99) as users_with_plagiarism_more_than_99_percent,
                count(distinct aqum.user_id) filter (where aqum.plagiarism_score >= 0.95) as users_with_plagiarism_more_than_95_percent,
                count(distinct aqum.user_id) filter (where aqum.plagiarism_score >= 0.90) as users_with_plagiarism_more_than_90_percent
                from assignments a
                left join courses c  on c.course_id = a.course_id 
                left join assignment_question_mapping aqm on aqm.assignment_id = a.assignment_id 
                left join assignment_question aq  on aq.assignment_question_id  = aqm.question_id 
                left join topics t on t.topic_id = aq.topic_id and t.topic_template_id in (102,103,119,334,336,338,339,340,341,342,344)
                left join assignment_question_user_mapping_new aqum on aqum.assignment_id = aqm.assignment_id and aqum.question_id = aqm.question_id 
                left join course_user_mapping on course_user_mapping.course_id = c.course_id and course_user_mapping.status in (5,8,9) and course_user_mapping.label_id is null
                group by 1,2,3,4,5,6,7,8,9,10,11,12
                )
                Select distinct question_detail.table_unique_key,
                       question_detail.assignment_id,
                       question_detail.assignment_name,
                       question_detail.start_timestamp,
                       question_detail.assignment_type,
                       question_detail.course_id,
                       question_detail.question_id,
                       question_detail.question_title,
                       question_detail.difficulty_type,
                       question_detail.topic_id,
                       question_detail.module_name,
                       question_detail.opened_in_7_days,
                       history_based_question_detail.opened_in_7_days as history_based_opened_in_7_days,
                       question_detail.attempted_in_7_days,
                       history_based_question_detail.attempted_in_7_days as history_based_attempted_in_7_days,
                       question_detail.completed_in_7_days,
                       history_based_question_detail.completed_in_7_days as  history_based_completed_in_7_days,
                       question_detail.opened_in_8_to_10_days,
                       history_based_question_detail.opened_in_8_to_10_days as history_based_opened_in_8_to_10_days,
                       question_detail.attempted_in_8_to_10_days,
                       history_based_question_detail.attempted_in_8_to_10_days as history_based_attempted_in_8_to_10_days,
                       question_detail.completed_in_8_to_10_days,
                       history_based_question_detail.completed_in_8_to_10_days as history_based_completed_in_8_to_10_days,
                       question_detail.opened_in_11_to_14_days,
                       history_based_question_detail.opened_in_11_to_14_days as history_based_opened_in_11_to_14_days,
                       question_detail.attempted_in_11_to_14_days,
                       history_based_question_detail.attempted_in_11_to_14_days as history_based_attempted_in_11_to_14_days,
                       question_detail.completed_in_11_to_14_days,
                       history_based_question_detail.completed_in_11_to_14_days as history_based_completed_in_11_to_14_days,
                       question_detail.opened_in_14_plus_days,
                       history_based_question_detail.opened_in_14_plus_days as history_based_opened_in_14_plus_days,
                       question_detail.attempted_in_14_plus_days,
                       history_based_question_detail.attempted_in_14_plus_days as history_based_attempted_in_14_plus_days,
                       question_detail.completed_in_14_plus_days,
                       history_based_question_detail.completed_in_14_plus_days as history_based_completed_in_14_plus_days,
                       question_detail.users_with_plagiarism_more_than_99_percent,
                       history_based_question_detail.users_with_plagiarism_more_than_99_percent as history_based_users_with_plagiarism_more_than_99_percent,
                       question_detail.users_with_plagiarism_more_than_95_percent,
                       history_based_question_detail.users_with_plagiarism_more_than_95_percent as history_based_users_with_plagiarism_more_than_95_percent,
                       question_detail.users_with_plagiarism_more_than_90_percent,
                       history_based_question_detail.users_with_plagiarism_more_than_90_percent as history_based_users_with_plagiarism_more_than_90_percent
                from question_detail 
                left join history_based_question_detail 
                  on question_detail.assignment_id = history_based_question_detail.assignment_id and question_detail.question_id = history_based_question_detail.question_id;
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