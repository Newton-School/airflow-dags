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
            'INSERT INTO arl_coding_contest_x_user (table_unique_key,user_id,contest_id,course_id,module_name,'
            'contest_release_date,total_contest_questions,opened_questions,history_based_opened_questions,'
            'attempted_questions,history_based_attempted_questions,completed_questions,'
            'history_based_completed_questions,beginner_and_easy_completed_questions,'
            'history_based_beginner_and_easy_completed_questions,beginner_completed_questions,'
            'history_based_beginner_completed_questions,easy_completed_questions,'
            'history_based_easy_completed_questions,medium_completed_questions,'
            'history_based_medium_completed_questions,hard_completed_questions,'
            'history_based_hard_completed_questions,challenge_completed_questions,'
            'history_based_challenge_completed_questions,hard_and_challenge_completed_questions,'
            'history_based_hard_and_challenge_completed_questions,marks_obtained,history_based_marks_obtained)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (table_unique_key) do update set module_name=EXCLUDED.module_name,'
            'contest_release_date=EXCLUDED.contest_release_date,'
            'total_contest_questions=EXCLUDED.total_contest_questions,'
            'opened_questions=EXCLUDED.opened_questions,'
            'history_based_opened_questions=EXCLUDED.history_based_opened_questions,'
            'attempted_questions=EXCLUDED.attempted_questions,'
            'history_based_attempted_questions=EXCLUDED.history_based_attempted_questions,'
            'completed_questions=EXCLUDED.completed_questions,'
            'history_based_completed_questions=EXCLUDED.history_based_completed_questions,'
            'beginner_and_easy_completed_questions=EXCLUDED.beginner_and_easy_completed_questions,'
            'history_based_beginner_and_easy_completed_questions=EXCLUDED.history_based_beginner_and_easy_completed_questions,'
            'beginner_completed_questions=EXCLUDED.beginner_completed_questions,'
            'history_based_beginner_completed_questions=EXCLUDED.history_based_beginner_completed_questions,'
            'easy_completed_questions=EXCLUDED.easy_completed_questions,'
            'history_based_easy_completed_questions=EXCLUDED.history_based_easy_completed_questions,'
            'medium_completed_questions=EXCLUDED.medium_completed_questions,'
            'history_based_medium_completed_questions=EXCLUDED.history_based_medium_completed_questions,'
            'hard_completed_questions=EXCLUDED.hard_completed_questions,'
            'history_based_hard_completed_questions=EXCLUDED.history_based_hard_completed_questions,'
            'challenge_completed_questions=EXCLUDED.challenge_completed_questions,'
            'history_based_challenge_completed_questions=EXCLUDED.history_based_challenge_completed_questions,'
            'hard_and_challenge_completed_questions=EXCLUDED.hard_and_challenge_completed_questions,'
            'history_based_hard_and_challenge_completed_questions=EXCLUDED.history_based_hard_and_challenge_completed_questions,'
            'marks_obtained=EXCLUDED.marks_obtained,history_based_marks_obtained=EXCLUDED.history_based_marks_obtained;',
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
            )
        )
    pg_conn.commit()


dag = DAG(
    'ARL_coding_contest_x_user',
    default_args=default_args,
    description='An Analytics Reporting Layer DAG for Coding Contest x users',
    schedule_interval='20 1 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS arl_coding_contest_x_user (
            id serial,
            table_unique_key double precision not null PRIMARY KEY,
            user_id bigint,
            contest_id int,
            course_id int,
            module_name varchar(256),
            contest_release_date DATE,
            total_contest_questions int,
            opened_questions int,
            history_based_opened_questions int,
            attempted_questions int,
            history_based_attempted_questions int,
            completed_questions int,
            history_based_completed_questions int,
            beginner_and_easy_completed_questions int,
            history_based_beginner_and_easy_completed_questions int,
            beginner_completed_questions int,
            history_based_beginner_completed_questions int,
            easy_completed_questions int,
            history_based_easy_completed_questions int,
            medium_completed_questions int,
            history_based_medium_completed_questions int,
            hard_completed_questions int,
            history_based_hard_completed_questions int,
            challenge_completed_questions int,
            history_based_challenge_completed_questions int,
            hard_and_challenge_completed_questions int,
            history_based_hard_and_challenge_completed_questions int,
            marks_obtained int,
            history_based_marks_obtained int
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_result_db',
    sql='''with user_details as
            (select aqum.user_id,
                   aqum.assignment_id,
                   c.course_id,
                   c.course_name,
                   t.template_name as module_name,
                   date(a.start_timestamp) as assignment_release_date,
                   count(distinct aqum.id) as opened_questions,
                   count(distinct aqum.id) filter (where a.end_timestamp >= aqum.question_started_at) as attempted_questions,
                   count(distinct aqum.id) filter (where aqum.question_completed_at is not null and (a.end_timestamp >= aqum.question_completed_at)) as completed_questions,
                   count(distinct aqum.id) filter (where aqum.question_completed_at is not null and (a.end_timestamp >= aqum.question_completed_at) and aq.difficulty_type in (1,2)) as beginner_and_easy_completed_questions,
                   count(distinct aqum.id) filter (where aqum.question_completed_at is not null and (a.end_timestamp >= aqum.question_completed_at) and aq.difficulty_type = 1) as beginner_completed_questions,
                   count(distinct aqum.id) filter (where aqum.question_completed_at is not null and (a.end_timestamp >= aqum.question_completed_at) and aq.difficulty_type = 2) as easy_completed_questions,
                   count(distinct aqum.id) filter (where aqum.question_completed_at is not null and (a.end_timestamp >= aqum.question_completed_at) and aq.difficulty_type = 3) as medium_completed_questions,
                   count(distinct aqum.id) filter (where aqum.question_completed_at is not null and (a.end_timestamp >= aqum.question_completed_at) and aq.difficulty_type = 4) as hard_completed_questions,
                   count(distinct aqum.id) filter (where aqum.question_completed_at is not null and (a.end_timestamp >= aqum.question_completed_at) and aq.difficulty_type = 5) as challenge_completed_questions,
                   count(distinct aqum.id) filter (where aqum.question_completed_at is not null and (a.end_timestamp >= aqum.question_completed_at) and aq.difficulty_type in (4,5)) as hard_and_challenge_completed_questions
                from assignment_question_user_mapping aqum
                join assignments a 
                   on a.assignment_id = aqum.assignment_id and a.original_assignment_type in (3,4)
                left join assignment_question aq 
                   on aq.assignment_question_id  = aqum.question_id 
                left join courses c 
                   on c.course_id  = a.course_id
                left join assignment_topic_mapping atm
                   on atm.assignment_id = aqum.assignment_id 
                left join topics t
                   on t.topic_id = atm.topic_id and topic_template_id in (102,103,119,334,336,338,339,340,341,342,344,410)
                left join course_user_mapping on course_user_mapping.course_id = c.course_id and course_user_mapping.status in (5,8,9) and course_user_mapping.label_id is null
                group by 1,2,3,4,5,6
                   ),
            history_based_user_details as 
                  (select aqum.user_id,
                   aqum.assignment_id,
                   c.course_id,
                   c.course_name,
                   t.template_name as module_name,
                   date(a.start_timestamp) as assignment_release_date,
                   count(distinct aqum.id) as opened_questions,
                   count(distinct aqum.id) filter (where a.end_timestamp >= aqum.question_started_at) as attempted_questions,
                   count(distinct aqum.id) filter (where aqum.question_completed_at is not null and (a.end_timestamp >= aqum.question_completed_at)) as completed_questions,
                   count(distinct aqum.id) filter (where aqum.question_completed_at is not null and (a.end_timestamp >= aqum.question_completed_at) and aq.difficulty_type in (1,2)) as beginner_and_easy_completed_questions,
                   count(distinct aqum.id) filter (where aqum.question_completed_at is not null and (a.end_timestamp >= aqum.question_completed_at) and aq.difficulty_type = 1) as beginner_completed_questions,
                   count(distinct aqum.id) filter (where aqum.question_completed_at is not null and (a.end_timestamp >= aqum.question_completed_at) and aq.difficulty_type = 2) as easy_completed_questions,
                   count(distinct aqum.id) filter (where aqum.question_completed_at is not null and (a.end_timestamp >= aqum.question_completed_at) and aq.difficulty_type = 3) as medium_completed_questions,
                   count(distinct aqum.id) filter (where aqum.question_completed_at is not null and (a.end_timestamp >= aqum.question_completed_at) and aq.difficulty_type = 4) as hard_completed_questions,
                   count(distinct aqum.id) filter (where aqum.question_completed_at is not null and (a.end_timestamp >= aqum.question_completed_at) and aq.difficulty_type = 5) as challenge_completed_questions,
                   count(distinct aqum.id) filter (where aqum.question_completed_at is not null and (a.end_timestamp >= aqum.question_completed_at) and aq.difficulty_type in (4,5)) as hard_and_challenge_completed_questions
                from assignment_question_user_mapping aqum
                join assignments a 
                   on a.assignment_id = aqum.assignment_id and a.original_assignment_type in (3,4)
                left join assignment_question aq 
                   on aq.assignment_question_id  = aqum.question_id 
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
                                and wud.label_mapping_id is null and wud.status in (5,8,9) and wud.unit_type like 'LEARNING') as course_user_mapping_new
                    on a.course_id = course_user_mapping_new.course_id and date_trunc('week',a.start_timestamp) = course_user_mapping_new.week_view
                left join courses c 
                   on c.course_id  = a.course_id
                left join assignment_topic_mapping atm
                   on atm.assignment_id = aqum.assignment_id 
                left join topics t
                   on t.topic_id = atm.topic_id and topic_template_id in (102,103,119,334,336,338,339,340,341,342,344,410)
                group by 1,2,3,4,5,6
                  ),
            all_assignment_questions as 
               (Select aqm.assignment_id,
                       count(distinct aqm.question_id) as assignment_question_count
                from assignment_question_mapping aqm
                group by 1
               ),
            marks as (select assignment_id,
                                user_id,
                                _difficulty_type*_all_test_case_passed*_cheated as marks_obtained
                         from
                          (select aqum.assignment_id,
                                  aqum.user_id,
                           case 
                               when aq.difficulty_type in (1,2) then 2
                               when aq.difficulty_type in (3) then 3
                               when aq.difficulty_type in (4,5) then 4 end as _difficulty_type,
                           case when all_test_case_passed = 'true' then 1 else 0 end as _all_test_case_passed,
                           case when cheated = 'true' then 1 else 0 end as _cheated       
                         from assignment_question_user_mapping aqum
                         left join assignment_question aq 
                          on aqum.question_id = aq.assignment_question_id
                          ) as sq
                        ),
            history_based_marks as (select assignment_id,
                                user_id,
                                _difficulty_type*_all_test_case_passed*_cheated as marks_obtained
                         from
                          (select aqum.assignment_id,
                                  aqum.user_id,
                           case 
                               when aq.difficulty_type in (1,2) then 2
                               when aq.difficulty_type in (3) then 3
                               when aq.difficulty_type in (4,5) then 4 end as _difficulty_type,
                           case when all_test_case_passed = 'true' then 1 else 0 end as _all_test_case_passed,
                           case when cheated = 'true' then 1 else 0 end as _cheated       
                         from assignment_question_user_mapping aqum
                         left join assignment_question aq 
                          on aqum.question_id = aq.assignment_question_id
                         join assignments a 
                           on a.assignment_id = aqum.assignment_id and a.original_assignment_type in (3,4)
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
                                and wud.label_mapping_id is null and wud.status in (5,8,9) and wud.unit_type like 'LEARNING') as course_user_mapping_new
                    on a.course_id = course_user_mapping_new.course_id and date_trunc('week',a.start_timestamp) = course_user_mapping_new.week_view
                          ) as sq
                        )
             select distinct concat(user_details.user_id,'0',user_details.assignment_id,module_raw.topic_template_id,user_details.course_id) as table_unique_key, 
                    user_details.user_id,
                    user_details.assignment_id as contest_id,
                    user_details.course_id,
                    user_details.module_name,
                    user_details.assignment_release_date as contest_release_date,
                    all_assignment_questions.assignment_question_count as total_contest_questions,
                    user_details.opened_questions,
                    history_based_user_details.opened_questions as history_based_opened_questions,
                    user_details.attempted_questions,
                    history_based_user_details.attempted_questions as history_based_attempted_questions,
                    user_details.completed_questions,
                    history_based_user_details.completed_questions as history_based_completed_questions,
                    user_details.beginner_and_easy_completed_questions,
                    history_based_user_details.beginner_and_easy_completed_questions as history_based_beginner_and_easy_completed_questions,
                    user_details.beginner_completed_questions,
                    history_based_user_details.beginner_completed_questions as history_based_beginner_completed_questions,
                    user_details.easy_completed_questions,
                    history_based_user_details.easy_completed_questions as history_based_easy_completed_questions,
                    user_details.medium_completed_questions,
                    history_based_user_details.medium_completed_questions as history_based_medium_completed_questions,
                    user_details.hard_completed_questions,
                    history_based_user_details.hard_completed_questions as history_based_hard_completed_questions,
                    user_details.challenge_completed_questions,
                    history_based_user_details.challenge_completed_questions as history_based_challenge_completed_questions,
                    user_details.hard_and_challenge_completed_questions,
                    history_based_user_details.hard_and_challenge_completed_questions as history_based_hard_and_challenge_completed_questions,
                    marks.marks_obtained,
                    history_based_marks.marks_obtained as history_based_marks_obtained
             from user_details 
             left join all_assignment_questions 
                on user_details.assignment_id = all_assignment_questions.assignment_id
             left join marks
                on marks.user_id = user_details.user_id and marks.assignment_id = user_details.assignment_id
             left join history_based_user_details
                on user_details.assignment_id = history_based_user_details.assignment_id and user_details.user_id = history_based_user_details.user_id and user_details.course_id = history_based_user_details.course_id
             left join history_based_marks
                on history_based_marks.assignment_id = user_details.assignment_id and history_based_marks.user_id = user_details.user_id;
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