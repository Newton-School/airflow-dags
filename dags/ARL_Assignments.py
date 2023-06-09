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
            'INSERT INTO arl_assignments (table_unique_key,assignment_id,'
            'assignment_name,assignment_type,assignment_sub_type,course_id,'
            'module_name,original_assignment_type,release_date,questions_opened,'
            'history_based_questions_opened,questions_attempted,history_based_questions_attempted,'
            'questions_completed,history_based_questions_completed,_0_percent_opened_users,'
            'history_based_0_percent_opened_users,_25_percent_opened_users,'
            'history_based_25_percent_opened_users,_50_percent_opened_users,'
            'history_based_50_percent_opened_users,_75_percent_opened_users,'
            'history_based_75_percent_opened_users,_100_percent_opened_users,'
            'history_based_100_percent_opened_users,_0_percent_attempted_users,'
            'history_based_0_percent_attempted_users,_25_percent_attempted_users,'
            'history_based_25_percent_attempted_users,_50_percent_attempted_users,'
            'history_based_50_percent_attempted_users,_75_percent_attempted_users,'
            'history_based_75_percent_attempted_users,_100_percent_attempted_users,'
            'history_based_100_percent_attempted_users,_0_percent_completed_users,'
            'history_based_0_percent_completed_users,_25_percent_completed_users,'
            'history_based_25_percent_completed_users,_50_percent_completed_users,'
            'history_based_50_percent_completed_users,_75_percent_completed_users,'
            'history_based_75_percent_completed_users,_100_percent_completed_users,'
            'history_based_100_percent_completed_users,_90_plag_users,history_based_90_plag_users,'
            '_95_plag_users,history_based_95_plag_users,_99_plag_users,history_based_99_plag_users)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'
            '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (table_unique_key) do update set assignment_name=EXCLUDED.assignment_name,'
            'assignment_type=EXCLUDED.assignment_type,assignment_sub_type=EXCLUDED.assignment_sub_type,'
            'course_id=EXCLUDED.course_id,module_name=EXCLUDED.module_name,'
            'original_assignment_type=EXCLUDED.original_assignment_type,release_date=EXCLUDED.release_date,'
            'questions_opened=EXCLUDED.questions_opened,'
            'history_based_questions_opened=EXCLUDED.history_based_questions_opened,'
            'questions_attempted=EXCLUDED.questions_attempted,'
            'history_based_questions_attempted=EXCLUDED.history_based_questions_attempted,'
            'questions_completed=EXCLUDED.questions_completed,'
            'history_based_questions_completed=EXCLUDED.history_based_questions_completed,'
            '_0_percent_opened_users=EXCLUDED._0_percent_opened_users,'
            'history_based_0_percent_opened_users=EXCLUDED.history_based_0_percent_opened_users,'
            '_25_percent_opened_users=EXCLUDED._25_percent_opened_users,'
            'history_based_25_percent_opened_users=EXCLUDED.history_based_25_percent_opened_users,'
            '_50_percent_opened_users=EXCLUDED._50_percent_opened_users,'
            'history_based_50_percent_opened_users=EXCLUDED.history_based_50_percent_opened_users,'
            '_75_percent_opened_users=EXCLUDED._75_percent_opened_users,'
            'history_based_75_percent_opened_users=EXCLUDED.history_based_75_percent_opened_users,'
            '_100_percent_opened_users=EXCLUDED._100_percent_opened_users,'
            'history_based_100_percent_opened_users=EXCLUDED.history_based_100_percent_opened_users,'
            '_0_percent_attempted_users=EXCLUDED._0_percent_attempted_users,'
            'history_based_0_percent_attempted_users=EXCLUDED.history_based_0_percent_attempted_users,'
            '_25_percent_attempted_users=EXCLUDED._25_percent_attempted_users,'
            'history_based_25_percent_attempted_users=EXCLUDED.history_based_25_percent_attempted_users,'
            '_50_percent_attempted_users=EXCLUDED._50_percent_attempted_users,'
            'history_based_50_percent_attempted_users=EXCLUDED.history_based_50_percent_attempted_users,'
            '_75_percent_attempted_users=EXCLUDED._75_percent_attempted_users,'
            'history_based_75_percent_attempted_users=EXCLUDED.history_based_75_percent_attempted_users,'
            '_100_percent_attempted_users=EXCLUDED._100_percent_attempted_users,'
            'history_based_100_percent_attempted_users=EXCLUDED.history_based_100_percent_attempted_users,'
            '_0_percent_completed_users=EXCLUDED._0_percent_completed_users,'
            'history_based_0_percent_completed_users=EXCLUDED.history_based_0_percent_completed_users,'
            '_25_percent_completed_users=EXCLUDED._25_percent_completed_users,'
            'history_based_25_percent_completed_users=EXCLUDED.history_based_25_percent_completed_users,'
            '_50_percent_completed_users=EXCLUDED._50_percent_completed_users,'
            'history_based_50_percent_completed_users=EXCLUDED.history_based_50_percent_completed_users,'
            '_75_percent_completed_users=EXCLUDED._75_percent_completed_users,'
            'history_based_75_percent_completed_users=EXCLUDED.history_based_75_percent_completed_users,'
            '_100_percent_completed_users=EXCLUDED._100_percent_completed_users,'
            'history_based_100_percent_completed_users=EXCLUDED.history_based_100_percent_completed_users,'
            '_90_plag_users=EXCLUDED._90_plag_users,history_based_90_plag_users=EXCLUDED.history_based_90_plag_users,'
            '_95_plag_users=EXCLUDED._95_plag_users,history_based_95_plag_users=EXCLUDED.history_based_95_plag_users,'
            '_99_plag_users=EXCLUDED._99_plag_users,history_based_99_plag_users=EXCLUDED.history_based_99_plag_users ;',
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
                transform_row[41],
                transform_row[42],
                transform_row[43],
                transform_row[44],
                transform_row[45],
                transform_row[46],
                transform_row[47],
                transform_row[48],
                transform_row[49],
                transform_row[50],
            )
        )
    pg_conn.commit()


dag = DAG(
    'ARL_Assignments',
    default_args=default_args,
    description='An Analytics Reporting Layer DAG for Assignments',
    schedule_interval='45 0 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS arl_assignments (
            id serial,
            table_unique_key double precision not null PRIMARY KEY,
            assignment_id int,
            assignment_name varchar(256),
            assignment_type varchar(64),
            assignment_sub_type varchar(32),
            course_id int,
            module_name varchar(256),
            original_assignment_type int,
            release_date DATE,
            questions_opened int,
            history_based_questions_opened int,
            questions_attempted int,
            history_based_questions_attempted int,
            questions_completed int,
            history_based_questions_completed int,
            _0_percent_opened_users int,
            history_based_0_percent_opened_users int,
            _25_percent_opened_users int,
            history_based_25_percent_opened_users int,
            _50_percent_opened_users int,
            history_based_50_percent_opened_users int,
            _75_percent_opened_users int,
            history_based_75_percent_opened_users int,
            _100_percent_opened_users int,
            history_based_100_percent_opened_users int,
            _0_percent_attempted_users int,
            history_based_0_percent_attempted_users int,
            _25_percent_attempted_users int,
            history_based_25_percent_attempted_users int,
            _50_percent_attempted_users int,
            history_based_50_percent_attempted_users int,
            _75_percent_attempted_users int,
            history_based_75_percent_attempted_users int,
            _100_percent_attempted_users int,
            history_based_100_percent_attempted_users int,
            _0_percent_completed_users int,
            history_based_0_percent_completed_users int,
            _25_percent_completed_users int,
            history_based_25_percent_completed_users int,
            _50_percent_completed_users int,
            history_based_50_percent_completed_users int,
            _75_percent_completed_users int,
            history_based_75_percent_completed_users int,
            _100_percent_completed_users int,
            history_based_100_percent_completed_users int,
            _90_plag_users int,
            history_based_90_plag_users int,
            _95_plag_users int,
            history_based_95_plag_users int,
            _99_plag_users int,
            history_based_99_plag_users int
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_result_db',
    sql='''with module_raw as (
                select atm.assignment_id,
                       t.topic_template_id,
                       t.template_name as module_name
                from assignment_topic_mapping atm 
                left join topics t 
                on atm.topic_id  = t.topic_id 
                where topic_template_id in (102,103,119,334,336,338,339,340,341,342,344,410)
                group by 1,2,3),
            history_based_assignment_details as(
            select a.assignment_id,
                   a.title as assignment_name,
                   case when a.assignment_type = 1 then 'Normal Assignment'
                        when a.assignment_type = 2 then 'Filtering Assignment'
                        when a.assignment_type = 3 then 'Competitive Assignment'
                        when a.assignment_type = 4 then 'Duration Assignment'
                        when a.assignment_type = 5 then 'Milestone Assignment'
                        when a.assignment_type = 6 then 'Question of the Day Assignment'
                        end as assignment_type,
                    case when a.assignment_sub_type = 1 then 'General'
                         when a.assignment_sub_type = 2 then 'In Class'
                         when a.assignment_sub_type = 3 then 'Post Class'
                         when a.assignment_sub_type = 4 then 'Module Contest'
                         when a.assignment_sub_type = 5 then 'Module Assignment'
                         end as assignment_sub_type,
                    a.original_assignment_type,
                    a.course_id,
                    c.course_name,
                    date(a.start_timestamp) as release_date,
                    count(distinct aqum.table_unique_key) as questions_opened,
                    count(distinct aqum.table_unique_key) filter(where aqum.max_test_case_passed is not null ) as questions_attempted,
                    count(distinct aqum.table_unique_key) filter(where aqum.all_test_case_passed is true ) as questions_completed
                 from assignments a 
                 left join courses c 
                   on c.course_id = a.course_id
                 left join assignment_question_mapping aqm
                   on aqm.assignment_id = a.assignment_id 
                 left join assignment_question_user_mapping aqum
                   on aqum.assignment_id  = a.assignment_id
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
                group by 1,2,3,4,5,6,7),
            history_based_users_final_raw as (
              with user_raw as
                 (Select aqum.assignment_id,
                   aqum.user_id,
                   count(distinct question_id) as questions_opened,
                   count(distinct question_id) filter(where max_test_case_passed is not null) as questions_attempted,
                   count(distinct question_id) filter(where all_test_case_passed = 'true') as questions_completed,
                   count(distinct question_id) filter(where plagiarism_score >= 0.90) as plag_score_90,
                   count(distinct question_id) filter(where plagiarism_score >= 0.95) as plag_score_95,
                   count(distinct question_id) filter(where plagiarism_score >= 0.99) as plag_score_99
               from assignment_question_user_mapping aqum
               left join assignments on aqum.assignment_id = assignments.assignment_id
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
                    on assignments.course_id = course_user_mapping_new.course_id and date_trunc('week',assignments.start_timestamp) = course_user_mapping_new.week_view
               group by 1,2),
            assignment_questions as 
               (Select aqm.assignment_id,
                       count(distinct aqm.question_id) as assignment_question_count
                from assignment_question_mapping aqm
                group by 1
               ),
            user_raw2 as
               (
               Select user_raw.assignment_id,
                    user_raw.user_id,
                    user_raw.questions_opened*100/assignment_questions.assignment_question_count as opened_question_percent,
                    user_raw.questions_attempted*100/assignment_questions.assignment_question_count as attempted_question_percent,
                    user_raw.questions_completed*100/assignment_questions.assignment_question_count as completed_question_percent,
                    plag_score_90,
                    plag_score_95,
                    plag_score_99
             from assignment_questions
             left join user_raw
               on user_raw.assignment_id = assignment_questions.assignment_id
               )
             select assignment_id,
                    count(distinct user_id) filter(where opened_question_percent = 0) as _0_percent_opened_users,
                    count(distinct user_id) filter(where opened_question_percent >= 25) as _25_percent_opened_users,
                    count(distinct user_id) filter(where opened_question_percent >= 50) as _50_percent_opened_users,
                    count(distinct user_id) filter(where opened_question_percent >= 75) as _75_percent_opened_users,
                    count(distinct user_id) filter(where opened_question_percent = 100) as _100_percent_opened_users,
                    count(distinct user_id) filter(where attempted_question_percent = 0) as _0_percent_attempted_users,
                    count(distinct user_id) filter(where attempted_question_percent >= 25) as _25_percent_attempted_users,
                    count(distinct user_id) filter(where attempted_question_percent >= 50) as _50_percent_attempted_users,
                    count(distinct user_id) filter(where attempted_question_percent >= 75) as _75_percent_attempted_users,
                    count(distinct user_id) filter(where attempted_question_percent = 100) as _100_percent_attempted_users,
                    count(distinct user_id) filter(where completed_question_percent = 0) as _0_percent_completed_users,
                    count(distinct user_id) filter(where completed_question_percent >= 25) as _25_percent_completed_users,
                    count(distinct user_id) filter(where completed_question_percent >= 50) as _50_percent_completed_users,
                    count(distinct user_id) filter(where completed_question_percent >= 75) as _75_percent_completed_users,
                    count(distinct user_id) filter(where completed_question_percent = 100) as _100_percent_completed_users,
                    count(distinct user_id) filter(where plag_score_90 > 0) as _90_plag_users,
                    count(distinct user_id) filter(where plag_score_95 > 0) as _95_plag_users,
                    count(distinct user_id) filter(where plag_score_99 > 0) as _99_plag_users
             from user_raw2
             group by 1
             ),
            assignment_details as (
             select a.assignment_id,
                   a.title as assignment_name,
                   case when a.assignment_type = 1 then 'Normal Assignment'
                        when a.assignment_type = 2 then 'Filtering Assignment'
                        when a.assignment_type = 3 then 'Competitive Assignment'
                        when a.assignment_type = 4 then 'Duration Assignment'
                        when a.assignment_type = 5 then 'Milestone Assignment'
                        when a.assignment_type = 6 then 'Question of the Day Assignment'
                        end as assignment_type,
                    case when a.assignment_sub_type = 1 then 'General'
                         when a.assignment_sub_type = 2 then 'In Class'
                         when a.assignment_sub_type = 3 then 'Post Class'
                         when a.assignment_sub_type = 4 then 'Module Contest'
                         when a.assignment_sub_type = 5 then 'Module Assignment'
                         end as assignment_sub_type,
                    a.original_assignment_type,
                    a.course_id,
                    c.course_name,
                    date(a.start_timestamp) as release_date,
                    count(distinct aqum.table_unique_key) as questions_opened,
                    count(distinct aqum.table_unique_key) filter(where aqum.max_test_case_passed is not null ) as questions_attempted,
                    count(distinct aqum.table_unique_key) filter(where aqum.all_test_case_passed is true ) as questions_completed
                 from assignments a 
                 left join courses c 
                   on c.course_id = a.course_id
                 left join assignment_question_mapping aqm
                   on aqm.assignment_id = a.assignment_id 
                 left join assignment_question_user_mapping aqum
                   on aqum.assignment_id  = a.assignment_id
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
                group by 1,2,3,4,5,6,7),
            user_final_raw as (
              with user_raw as
                 (Select aqum.assignment_id,
                   aqum.user_id,
                   count(distinct question_id) as questions_opened,
                   count(distinct question_id) filter(where max_test_case_passed is not null) as questions_attempted,
                   count(distinct question_id) filter(where all_test_case_passed = 'true') as questions_completed,
                   count(distinct question_id) filter(where plagiarism_score >= 0.90) as plag_score_90,
                   count(distinct question_id) filter(where plagiarism_score >= 0.95) as plag_score_95,
                   count(distinct question_id) filter(where plagiarism_score >= 0.99) as plag_score_99
               from assignment_question_user_mapping aqum
               left join assignments on aqum.assignment_id = assignments.assignment_id
               group by 1,2),
            assignment_questions as 
               (Select aqm.assignment_id,
                       count(distinct aqm.question_id) as assignment_question_count
                from assignment_question_mapping aqm
                group by 1
               ),
            user_raw2 as
               (
               Select user_raw.assignment_id,
                    user_raw.user_id,
                    user_raw.questions_opened*100/assignment_questions.assignment_question_count as opened_question_percent,
                    user_raw.questions_attempted*100/assignment_questions.assignment_question_count as attempted_question_percent,
                    user_raw.questions_completed*100/assignment_questions.assignment_question_count as completed_question_percent,
                    plag_score_90,
                    plag_score_95,
                    plag_score_99
             from assignment_questions
             left join user_raw
               on user_raw.assignment_id = assignment_questions.assignment_id
               )
             select assignment_id,
                    count(distinct user_id) filter(where opened_question_percent = 0) as _0_percent_opened_users,
                    count(distinct user_id) filter(where opened_question_percent >= 25) as _25_percent_opened_users,
                    count(distinct user_id) filter(where opened_question_percent >= 50) as _50_percent_opened_users,
                    count(distinct user_id) filter(where opened_question_percent >= 75) as _75_percent_opened_users,
                    count(distinct user_id) filter(where opened_question_percent = 100) as _100_percent_opened_users,
                    count(distinct user_id) filter(where attempted_question_percent = 0) as _0_percent_attempted_users,
                    count(distinct user_id) filter(where attempted_question_percent >= 25) as _25_percent_attempted_users,
                    count(distinct user_id) filter(where attempted_question_percent >= 50) as _50_percent_attempted_users,
                    count(distinct user_id) filter(where attempted_question_percent >= 75) as _75_percent_attempted_users,
                    count(distinct user_id) filter(where attempted_question_percent = 100) as _100_percent_attempted_users,
                    count(distinct user_id) filter(where completed_question_percent = 0) as _0_percent_completed_users,
                    count(distinct user_id) filter(where completed_question_percent >= 25) as _25_percent_completed_users,
                    count(distinct user_id) filter(where completed_question_percent >= 50) as _50_percent_completed_users,
                    count(distinct user_id) filter(where completed_question_percent >= 75) as _75_percent_completed_users,
                    count(distinct user_id) filter(where completed_question_percent = 100) as _100_percent_completed_users,
                    count(distinct user_id) filter(where plag_score_90 > 0) as _90_plag_users,
                    count(distinct user_id) filter(where plag_score_95 > 0) as _95_plag_users,
                    count(distinct user_id) filter(where plag_score_99 > 0) as _99_plag_users
             from user_raw2
             group by 1) 
             select 
                      concat(assignment_details.assignment_id,assignment_details.course_id,extract(day from assignment_details.release_date),extract(month from assignment_details.release_date),extract(year from assignment_details.release_date),module_raw.topic_template_id) as table_unique_key,
                      assignment_details.assignment_id,
                      assignment_details.assignment_name,
                      assignment_details.assignment_type,
                      assignment_details.assignment_sub_type,
                      assignment_details.course_id,
                      module_raw.module_name as module_name,
                      assignment_details.original_assignment_type,
                      assignment_details.release_date,
                      assignment_details.questions_opened as questions_opened,
                      history_based_assignment_details.questions_opened as history_based_questions_opened,
                      assignment_details.questions_attempted as questions_attempted,
                      history_based_assignment_details.questions_attempted as history_based_questions_attempted,
                      assignment_details.questions_completed as questions_completed,
                      history_based_assignment_details.questions_completed as history_based_questions_completed,
                      user_final_raw._0_percent_opened_users as _0_percent_opened_users,
                      history_based_users_final_raw._0_percent_opened_users as  history_based_0_percent_opened_users,
                      user_final_raw._25_percent_opened_users as _25_percent_opened_users,
                      history_based_users_final_raw._25_percent_opened_users as history_based_25_percent_opened_users,
                      user_final_raw._50_percent_opened_users as _50_percent_opened_users,
                      history_based_users_final_raw._50_percent_opened_users as history_based_50_percent_opened_users,
                      user_final_raw._75_percent_opened_users as _75_percent_opened_users,
                      history_based_users_final_raw._75_percent_opened_users as history_based_75_percent_opened_users,
                      user_final_raw._100_percent_opened_users as _100_percent_opened_users,
                      history_based_users_final_raw._100_percent_opened_users as history_based_100_percent_opened_users,
                      user_final_raw._0_percent_attempted_users as _0_percent_attempted_users,
                      history_based_users_final_raw._0_percent_attempted_users as history_based_0_percent_attempted_users,
                      user_final_raw._25_percent_attempted_users as _25_percent_attempted_users,
                      history_based_users_final_raw._25_percent_attempted_users as history_based_25_percent_attempted_users,
                      user_final_raw._50_percent_attempted_users as _50_percent_attempted_users,
                      history_based_users_final_raw._50_percent_attempted_users as history_based_50_percent_attempted_users,
                      user_final_raw._75_percent_attempted_users as _75_percent_attempted_users,
                      history_based_users_final_raw._75_percent_attempted_users as history_based_75_percent_attempted_users,
                      user_final_raw._100_percent_attempted_users as _100_percent_attempted_users,
                      history_based_users_final_raw._100_percent_attempted_users as history_based_100_percent_attempted_users,
                      user_final_raw._0_percent_completed_users as _0_percent_completed_users,
                      history_based_users_final_raw._0_percent_completed_users as history_based_0_percent_completed_users,
                      user_final_raw._25_percent_completed_users as _25_percent_completed_users,
                      history_based_users_final_raw._25_percent_completed_users as history_based_25_percent_completed_users,
                      user_final_raw._50_percent_completed_users as _50_percent_completed_users,
                      history_based_users_final_raw._50_percent_completed_users as history_based_50_percent_completed_users,
                      user_final_raw._75_percent_completed_users as _75_percent_completed_users,
                      history_based_users_final_raw._75_percent_completed_users as history_based_75_percent_completed_users,
                      user_final_raw._100_percent_completed_users as _100_percent_completed_users,
                      history_based_users_final_raw._100_percent_completed_users as history_based_100_percent_completed_users,
                      user_final_raw._90_plag_users as _90_plag_users,
                      history_based_users_final_raw._90_plag_users as history_based_90_plag_users,
                      user_final_raw._95_plag_users as _95_plag_users,
                      history_based_users_final_raw._95_plag_users as history_based_95_plag_users,
                      user_final_raw._99_plag_users as _99_plag_users,
                      history_based_users_final_raw._99_plag_users as history_based_99_plag_users  
             from assignment_details
             left join user_final_raw 
               on assignment_details.assignment_id = user_final_raw.assignment_id
             left join history_based_assignment_details
               on history_based_assignment_details.assignment_id = assignment_details.assignment_id
             left join history_based_users_final_raw
               on history_based_users_final_raw.assignment_id = assignment_details.assignment_id
             left join module_raw
               on module_raw.assignment_id = assignment_details.assignment_id;
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