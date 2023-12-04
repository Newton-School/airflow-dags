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
            'INSERT INTO arl_contests_x_users (table_unique_key,'
            'course_id,'
            'course_name,'
            'course_structure_class,'
            'assignment_id,'
            'assignment_title,'
            'contest_type,'
            'topic_template_id,'
            'module_name,'
            'assignment_release_date,'
            'hidden,'
            'user_id,'
            'student_name,'
            'lead_type,'
            'label_mapping_status,'
            'student_category,'
            'question_count,'
            'opened_questions,'
            'attempted_questions,'
            'completed_questions,'
            'beginner_completed_questions,'
            'easy_completed_questions,'
            'medium_completed_questions,'
            'hard_completed_questions,'
            'challenge_completed_questions,'
            'questions_with_plag_score_90,'
            'questions_with_plag_score_95,'
            'questions_with_plag_score_99,'
            'opened_questions_unique,'
            'attempted_questions_unique,'
            'completed_questions_unique,'
            'beginner_completed_questions_unique,'
            'easy_completed_questions_unique,'
            'medium_completed_questions_unique,'
            'hard_completed_questions_unique,'
            'challenge_completed_questions_unique,'
            'plag_score_99_unique,'
            'plag_score_95_unique,'
            'plag_score_90_unique,'
            'activity_status_7_days,'
            'activity_status_14_days,'
            'activity_status_30_days,'
            'user_placement_status,'
            'admin_course_id,'
            'admin_unit_name)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'
            '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (table_unique_key) do update set course_id = EXCLUDED.course_id,'
            'course_name = EXCLUDED.course_name,'
            'course_structure_class = EXCLUDED.course_structure_class,'
            'assignment_title = EXCLUDED.assignment_title,'
            'contest_type = EXCLUDED.contest_type,'
            'topic_template_id = EXCLUDED.topic_template_id,'
            'module_name = EXCLUDED.module_name,'
            'assignment_release_date = EXCLUDED.assignment_release_date,'
            'hidden = EXCLUDED.hidden,'
            'student_name = EXCLUDED.student_name,'
            'lead_type = EXCLUDED.lead_type,'
            'label_mapping_status = EXCLUDED.label_mapping_status,'
            'student_category = EXCLUDED.student_category,'
            'question_count = EXCLUDED.question_count,'
            'opened_questions = EXCLUDED.opened_questions,'
            'attempted_questions = EXCLUDED.attempted_questions,'
            'completed_questions = EXCLUDED.completed_questions,'
            'beginner_completed_questions = EXCLUDED.beginner_completed_questions,'
            'easy_completed_questions = EXCLUDED.easy_completed_questions,'
            'medium_completed_questions = EXCLUDED.medium_completed_questions,'
            'hard_completed_questions = EXCLUDED.hard_completed_questions,'
            'challenge_completed_questions = EXCLUDED.challenge_completed_questions,'
            'questions_with_plag_score_90 = EXCLUDED.questions_with_plag_score_90,'
            'questions_with_plag_score_95 = EXCLUDED.questions_with_plag_score_95,'
            'questions_with_plag_score_99 = EXCLUDED.questions_with_plag_score_99,'
            'opened_questions_unique = EXCLUDED.opened_questions_unique,'
            'attempted_questions_unique = EXCLUDED.attempted_questions_unique,'
            'completed_questions_unique = EXCLUDED.completed_questions_unique,'
            'beginner_completed_questions_unique = EXCLUDED.beginner_completed_questions_unique,'
            'easy_completed_questions_unique = EXCLUDED.easy_completed_questions_unique,'
            'medium_completed_questions_unique = EXCLUDED.medium_completed_questions_unique,'
            'hard_completed_questions_unique = EXCLUDED.hard_completed_questions_unique,'
            'challenge_completed_questions_unique = EXCLUDED.challenge_completed_questions_unique,'
            'plag_score_99_unique = EXCLUDED.plag_score_99_unique,'
            'plag_score_95_unique = EXCLUDED.plag_score_95_unique,'
            'plag_score_90_unique = EXCLUDED.plag_score_90_unique,'
            'activity_status_7_days = EXCLUDED.activity_status_7_days,'
            'activity_status_14_days = EXCLUDED.activity_status_14_days,'
            'activity_status_30_days = EXCLUDED.activity_status_30_days,'
            'user_placement_status = EXCLUDED.user_placement_status,'
            'admin_course_id = EXCLUDED.admin_course_id,'
            'admin_unit_name = EXCLUDED.admin_unit_name;',
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
            )
        )
    pg_conn.commit()


dag = DAG(
    'ARL_contests_x_users_dag',
    default_args=default_args,
    description='An Analytics Reporting Layer DAG for Contests at user level ',
    concurrency=4,
    max_active_tasks=6,
    max_active_runs=6,
    schedule_interval='45 0 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS arl_contests_x_users (
            id serial,
            table_unique_key double precision not null PRIMARY KEY,
            course_id int,
            course_name text,
            course_structure_class text,
            assignment_id bigint,
            assignment_title text,
            contest_type text,
            topic_template_id int, 
            module_name text, 
            assignment_release_date date,
            hidden boolean, 
            user_id bigint, 
            student_name text, 
            lead_type text,
            label_mapping_status text,
            student_category text,
            question_count int, 
            opened_questions int,
            attempted_questions int,
            completed_questions int,
            beginner_completed_questions int,
            easy_completed_questions int,
            medium_completed_questions int,
            hard_completed_questions int,
            challenge_completed_questions int,
            questions_with_plag_score_90 int,
            questions_with_plag_score_95 int,
            questions_with_plag_score_99 int,
            opened_questions_unique int,
            attempted_questions_unique int,
            completed_questions_unique int,
            beginner_completed_questions_unique int,
            easy_completed_questions_unique int,
            medium_completed_questions_unique int,
            hard_completed_questions_unique int,
            challenge_completed_questions_unique int,
            plag_score_99_unique int,
            plag_score_95_unique int,
            plag_score_90_unique int,
            activity_status_7_days text,
            activity_status_14_days text,
            activity_status_30_days text,
            user_placement_status text,
            admin_course_id int,
            admin_unit_name text
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_result_db',
    sql='''select
                    concat(a.course_id, a.assignment_id, cum.user_id, topic_template_id) as table_unique_key,
                    a.course_id,
                    c.course_name,
                    c.course_structure_class,
                    a.assignment_id,
                    a.title as assignment_title,
                    case 
                        when a.is_group = true then 'Group Contest'
                        when a.assignment_sub_type = 1 and a.is_group = false then 'Weekend Contest'
                        when a.assignment_sub_type = 4 and a.is_group = false then 'Module Contest'
                        else 'Contest Mapping Error'
                    end as contest_type,
                    module_mapping.topic_template_id,
                    module_mapping.module_name,
                    date(a.start_timestamp) as assignment_release_date,
                    a.hidden,
                    cum.user_id,
                    concat(ui.first_name,' ', ui.last_name) as student_name,
                    ui.lead_type,
                    case 
                        when cum.label_id is null and cum.status in (8,9) then 'Enrolled Student'
                        when cum.label_id is not null and cum.status in (8,9) then 'Label Marked Student'
                        when c.course_structure_id in (1,18) and cum.status in (11,12) then 'ISA Cancelled Student'
                        when c.course_structure_id not in (1,18) and cum.status in (30) then 'Deferred Student'
                        when c.course_structure_id not in (1,18) and cum.status in (11) then 'Foreclosed Student'
                        when c.course_structure_id not in (1,18) and cum.status in (12) then 'Reject by NS-Ops'
                        else 'Mapping Error'
                   end as label_mapping_status,
                   cucm.student_category,
                   a.question_count,
                   count(distinct aqum.question_id) filter(where a.end_timestamp >= aqum.question_started_at  and aqum.question_started_at  >= a.start_timestamp) as opened_questions,
                   count(distinct aqum.question_id) filter(where aqum.max_test_case_passed is not null and a.end_timestamp >= aqum.question_started_at  and aqum.question_started_at  >= a.start_timestamp) as attempted_questions,
                   count(distinct aqum.question_id) filter (where aqum.all_test_case_passed = 'true' and a.end_timestamp >= aqum.question_started_at  and aqum.question_started_at  >= a.start_timestamp) as completed_questions,
                   count(distinct aqum.question_id) filter (where aqum.all_test_case_passed = 'true' and (a.end_timestamp >= aqum.question_completed_at) and aq.difficulty_type = 1) as beginner_completed_questions,
                   count(distinct aqum.question_id) filter (where aqum.all_test_case_passed = 'true' and (a.end_timestamp >= aqum.question_completed_at) and aq.difficulty_type = 2) as easy_completed_questions,
                   count(distinct aqum.question_id) filter (where aqum.all_test_case_passed = 'true' and (a.end_timestamp >= aqum.question_completed_at) and aq.difficulty_type = 3) as medium_completed_questions,
                   count(distinct aqum.question_id) filter (where aqum.all_test_case_passed = 'true' and (a.end_timestamp >= aqum.question_completed_at) and aq.difficulty_type = 4) as hard_completed_questions,
                   count(distinct aqum.question_id) filter (where aqum.all_test_case_passed = 'true' and (a.end_timestamp >= aqum.question_completed_at) and aq.difficulty_type = 5) as challenge_completed_questions,
                   count(distinct aqum.question_id) filter (where plagiarism_score >= 0.90 and aqum.all_test_case_passed = 'true' and a.end_timestamp >= aqum.question_started_at  and aqum.question_started_at  >= a.start_timestamp) as questions_with_plag_score_90,
                   count(distinct aqum.question_id) filter (where plagiarism_score >= 0.95 and aqum.all_test_case_passed = 'true' and a.end_timestamp >= aqum.question_started_at  and aqum.question_started_at  >= a.start_timestamp) as questions_with_plag_score_95,
                   count(distinct aqum.question_id) filter (where plagiarism_score >= 0.99 and aqum.all_test_case_passed = 'true' and a.end_timestamp >= aqum.question_started_at  and aqum.question_started_at  >= a.start_timestamp) as questions_with_plag_score_99,
                  case
                        when dense_rank () over (partition by a.assignment_id, cum.user_id order by topic_template_id) = 1 then count(distinct aqum.question_id)
                        else null
                   end as opened_questions_unique,
                  case
                        when dense_rank () over (partition by a.assignment_id, cum.user_id order by topic_template_id) = 1 then (count(distinct aqum.question_id) filter (where aqum.max_test_case_passed is not null))
                        else null
                   end as attempted_questions_unique,
                   case 
                        when dense_rank () over (partition by a.assignment_id, cum.user_id order by topic_template_id) = 1 then (count(distinct aqum.question_id) filter(where aqum.all_test_case_passed = 'true'))
                        else null
                   end as completed_questions_unique,
                   
                   case 
                        when dense_rank () over (partition by a.assignment_id, cum.user_id order by topic_template_id) = 1 then (count(distinct aqum.question_id) filter(where aqum.all_test_case_passed = 'true' and (a.end_timestamp >= aqum.question_completed_at) and aq.difficulty_type = 1))
                        else null
                   end as beginner_completed_questions_unique,
                   
                       case 
                        when dense_rank () over (partition by a.assignment_id, cum.user_id order by topic_template_id) = 1 then (count(distinct aqum.question_id) filter(where aqum.all_test_case_passed = 'true' and (a.end_timestamp >= aqum.question_completed_at) and aq.difficulty_type = 2))
                        else null
                   end as easy_completed_questions_unique,
            
                       case 
                        when dense_rank () over (partition by a.assignment_id, cum.user_id order by topic_template_id) = 1 then (count(distinct aqum.question_id) filter(where aqum.all_test_case_passed = 'true' and (a.end_timestamp >= aqum.question_completed_at) and aq.difficulty_type = 3))
                        else null
                   end as medium_completed_questions_unique,
            
                       case 
                        when dense_rank () over (partition by a.assignment_id, cum.user_id order by topic_template_id) = 1 then (count(distinct aqum.question_id) filter(where aqum.all_test_case_passed = 'true' and (a.end_timestamp >= aqum.question_completed_at) and aq.difficulty_type = 4))
                        else null
                   end as hard_completed_questions_unique,
            
                       case 
                        when dense_rank () over (partition by a.assignment_id, cum.user_id order by topic_template_id) = 1 then (count(distinct aqum.question_id) filter(where aqum.all_test_case_passed = 'true' and (a.end_timestamp >= aqum.question_completed_at) and aq.difficulty_type = 5))
                        else null
                   end as challenge_completed_questions_unique,
            
                   
                   case 
                        when dense_rank () over (partition by a.assignment_id, cum.user_id order by topic_template_id) = 1 then (count(distinct aqum.question_id) filter (where plagiarism_score >= 0.99))
                        else null
                   end as plag_score_99_unique,
                   
                   case 
                        when dense_rank () over (partition by a.assignment_id, cum.user_id order by topic_template_id) = 1 then (count(distinct aqum.question_id) filter (where plagiarism_score >= 0.95))
                        else null
                   end as plag_score_95_unique,
                   
                  case 
                        when dense_rank () over (partition by a.assignment_id, cum.user_id order by topic_template_id) = 1 then (count(distinct aqum.question_id) filter (where plagiarism_score >= 0.90))
                        else null
                   end as plag_score_90_unique,
                   uasm.activity_status_7_days,
                   uasm.activity_status_14_days,
                   uasm.activity_status_30_days,
                   cum.user_placement_status,
                   cum.admin_course_id,
                   cum.admin_unit_name 
                from
                    assignments a
                join courses c
                    on c.course_id = a.course_id and a.original_assignment_type in (3,4) and date(a.start_timestamp) >= '2022-06-01'
                        and c.course_structure_id in (1,6,7,8,11,12,13,14,18,19,20,22,23,26,34,44,47,50,51,52,53,54,55,56,57,58,59,60)
                join course_user_mapping cum 
                    on cum.course_id = c.course_id and cum.status in (8,9,11,12,30)
                left join users_info ui
                    on ui.user_id = cum.user_id 
                left join course_user_category_mapping cucm 
                    on cucm.user_id = cum.user_id and cum.course_id = cucm.course_id 
                left join user_activity_status_mapping uasm 
                    on uasm.user_id = cum.user_id
                left join assignment_question_user_mapping_new aqum
                    on aqum.user_id = cum.user_id and a.assignment_id = aqum.assignment_id
                left join assignment_question aq
                    on aq.assignment_question_id = aqum.question_id
                left join 
                    (select
                        atm.assignment_id,
                        t.topic_template_id,
                        t.template_name as module_name
                    from 
                        assignment_topic_mapping atm 
                    left join topics t 
                        on atm.topic_id  = t.topic_id 
                    where topic_template_id in (102,103,119,334,336,338,339,340,341,342,344,410)
                    group by 1,2,3) module_mapping
                        on module_mapping.assignment_id = a.assignment_id
                group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,40,41,42,43,44,45;
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