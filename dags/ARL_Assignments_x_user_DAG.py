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
            'INSERT INTO arl_assignments_x_users (table_unique_key,user_id,assignment_id,'
            'assignment_title, assignment_release_date,course_id,'
            'total_assignment_questions,module_name,opened_questions,history_based_opened_questions,'
            'attempted_questions,history_based_attempted_questions,completed_questions,'
            'history_based_completed_questions,questions_with_plag_score_99,'
            'history_based_questions_with_plag_score_99,questions_with_plag_score_95,'
            'history_based_questions_with_plag_score_95,questions_with_plag_score_90,'
            'history_based_questions_with_plag_score_90)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (table_unique_key) do update set assignment_title = EXCLUDED.assignment_title,'
            'total_assignment_questions=EXCLUDED.total_assignment_questions,'
            'module_name=EXCLUDED.module_name,opened_questions=EXCLUDED.opened_questions,'
            'history_based_opened_questions=EXCLUDED.history_based_opened_questions,'
            'attempted_questions=EXCLUDED.attempted_questions,'
            'history_based_attempted_questions=EXCLUDED.history_based_attempted_questions,'
            'completed_questions=EXCLUDED.completed_questions,'
            'history_based_completed_questions=EXCLUDED.history_based_completed_questions,'
            'questions_with_plag_score_99=EXCLUDED.questions_with_plag_score_99,'
            'history_based_questions_with_plag_score_99=EXCLUDED.history_based_questions_with_plag_score_99,'
            'questions_with_plag_score_95=EXCLUDED.questions_with_plag_score_95,'
            'history_based_questions_with_plag_score_95=EXCLUDED.history_based_questions_with_plag_score_95,'
            'questions_with_plag_score_90=EXCLUDED.questions_with_plag_score_90,'
            'history_based_questions_with_plag_score_90=EXCLUDED.history_based_questions_with_plag_score_90 ;',
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
            )
        )
    pg_conn.commit()


dag = DAG(
    'ARL_Assignments_x_user',
    default_args=default_args,
    description='An Analytics Reporting Layer DAG for Assignments x user',
    schedule_interval='45 0 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS arl_assignments_x_users (
            id serial,
            table_unique_key double precision not null PRIMARY KEY,
            user_id bigint,
            assignment_id int,
            assignment_title varchar(1028),
            assignment_release_date DATE,
            course_id int,
            total_assignment_questions int,
            module_name varchar(256),
            opened_questions int,
            history_based_opened_questions int,
            attempted_questions int,
            history_based_attempted_questions int,
            completed_questions int,
            history_based_completed_questions int,
            questions_with_plag_score_99 int,
            history_based_questions_with_plag_score_99 int,
            questions_with_plag_score_95 int,
            history_based_questions_with_plag_score_95 int,
            questions_with_plag_score_90 int,
            history_based_questions_with_plag_score_90 int
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_result_db',
    sql='''with 
            user_details as
                (select aqum.user_id,
                       aqum.assignment_id,
                       a.title as assignment_title,
                       date(a.start_timestamp) as assignment_release_date,
                       c.course_id,
                       c.course_name,
                       count(distinct aqum.id) as opened_questions,
                       count(distinct aqum.id) filter(where aqum.max_test_case_passed is not null) as attempted_questions,
                       count(distinct aqum.id) filter(where aqum.all_test_case_passed = 'true') as completed_questions,
                       count(distinct aqum.id) filter(where plagiarism_score >= 0.90) as questions_with_plag_score_90,
                       count(distinct aqum.id) filter(where plagiarism_score >= 0.95) as questions_with_plag_score_95,
                       count(distinct aqum.id) filter(where plagiarism_score >= 0.99) as questions_with_plag_score_99
                    from assignment_question_user_mapping aqum
                    left join assignments a
                       on a.assignment_id = aqum.assignment_id 
                    left join courses c 
                       on c.course_id  = a.course_id
                    left join course_user_mapping on course_user_mapping.course_id = c.course_id and course_user_mapping.status in (5,8,9) and course_user_mapping.label_id is null
                    where a.original_assignment_type = 1
                    group by 1,2,3,4,5,6),
            history_based_user_details as 
                (select aqum.user_id,
                       aqum.assignment_id,
                       a.title as assignment_title,
                       date(a.start_timestamp) as assignment_release_date,
                       c.course_id,
                       c.course_name,
                       count(distinct aqum.id) as opened_questions,
                       count(distinct aqum.id) filter(where aqum.max_test_case_passed is not null) as attempted_questions,
                       count(distinct aqum.id) filter(where aqum.all_test_case_passed = 'true') as completed_questions,
                       count(distinct aqum.id) filter(where plagiarism_score >= 0.90) as questions_with_plag_score_90,
                       count(distinct aqum.id) filter(where plagiarism_score >= 0.95) as questions_with_plag_score_95,
                       count(distinct aqum.id) filter(where plagiarism_score >= 0.99) as questions_with_plag_score_99
                    from assignment_question_user_mapping aqum
                    left join assignments a 
                       on a.assignment_id = aqum.assignment_id 
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
                    where a.original_assignment_type = 1
                    group by 1,2,3,4,5,6),
            all_assignment_questions as 
               (Select aqm.assignment_id,
                       count(distinct aqm.question_id) as assignment_question_count
                from assignment_question_mapping aqm
                group by 1
               ),
            module_raw as (
                select atm.assignment_id,
                       t.topic_template_id,
                       t.template_name as module_name
                from assignment_topic_mapping atm 
                left join topics t 
                on atm.topic_id  = t.topic_id 
                where topic_template_id in (102,103,119,334,336,338,339,340,341,342,344,410)
                group by 1,2,3)
             select concat(user_details.user_id,'0',user_details.assignment_id,module_raw.topic_template_id,user_details.course_id) as table_unique_key,
                    user_details.user_id,
                    user_details.assignment_id,
                    user_details.assignment_title,
                    user_details.assignment_release_date,
                    user_details.course_id,
                    all_assignment_questions.assignment_question_count as total_assignment_questions,
                    module_raw.module_name,
                    user_details.opened_questions,
                    history_based_user_details.opened_questions as history_based_opened_questions,
                    user_details.attempted_questions,
                    history_based_user_details.attempted_questions as history_based_attempted_questions,
                    user_details.completed_questions,
                    history_based_user_details.completed_questions as history_based_completed_questions,
                    user_details.questions_with_plag_score_99,
                    history_based_user_details.questions_with_plag_score_99 as history_based_questions_with_plag_score_99,
                    user_details.questions_with_plag_score_95,
                    history_based_user_details.questions_with_plag_score_95 as history_based_questions_with_plag_score_95,
                    user_details.questions_with_plag_score_90,
                    history_based_user_details.questions_with_plag_score_90 as history_based_questions_with_plag_score_90
             from user_details 
             left join all_assignment_questions 
                on user_details.assignment_id = all_assignment_questions.assignment_id
             left join history_based_user_details
                on user_details.assignment_id = history_based_user_details.assignment_id and user_details.user_id = history_based_user_details.user_id and user_details.course_id = history_based_user_details.course_id
            left join module_raw
                on module_raw.assignment_id = user_details.assignment_id;
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