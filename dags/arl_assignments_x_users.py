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
            'INSERT INTO arl_assignments_x_users (table_unique_key,'
            'course_id,'
            'course_name,'
            'course_structure_class,'
            'assignment_id,'
            'assignment_title,'
            'normal_assignment_type,'
            'topic_template_id,'
            'module_name,'
            'assignment_release_date,'
            'user_id,'
            'student_name,'
            'lead_type,'
            'label_mapping_status,'
            'student_category,'
            'assignment_question_count,'
            'opened_questions,'
            'attempted_questions,'
            'completed_questions,'
            'questions_with_plag_score_90,'
            'questions_with_plag_score_95,'
            'questions_with_plag_score_99,'
            'hidden,'
            'opened_questions_unique,'
            'attempted_questions_unique,'
            'completed_questions_unique,'
            'plag_score_99_unique,'
            'plag_score_95_unique,'
            'plag_score_90_unique)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (table_unique_key) do update set course_id = EXCLUDED.course_id,'
            'course_name = EXCLUDED.course_name,'
            'course_structure_class = EXCLUDED.course_structure_class,'
            'assignment_id = EXCLUDED.assignment_id,'
            'assignment_title = EXCLUDED.assignment_title,'
            'normal_assignment_type = EXCLUDED.normal_assignment_type,'
            'topic_template_id = EXCLUDED.topic_template_id,'
            'module_name = EXCLUDED.module_name,'
            'assignment_release_date = EXCLUDED.assignment_release_date,'
            'user_id = EXCLUDED.user_id,'
            'student_name = EXCLUDED.student_name,'
            'lead_type = EXCLUDED.lead_type,'
            'label_mapping_status = EXCLUDED.label_mapping_status,'
            'student_category = EXCLUDED.student_category,'
            'assignment_question_count = EXCLUDED.assignment_question_count,'
            'opened_questions = EXCLUDED.opened_questions,'
            'attempted_questions = EXCLUDED.attempted_questions,'
            'completed_questions = EXCLUDED.completed_questions,'
            'questions_with_plag_score_90 = EXCLUDED.questions_with_plag_score_90,'
            'questions_with_plag_score_95 = EXCLUDED.questions_with_plag_score_95,'
            'questions_with_plag_score_99 = EXCLUDED.questions_with_plag_score_99,'
            'hidden = EXCLUDED.hidden,'
            'opened_questions_unique = EXCLUDED.opened_questions_unique,'
            'attempted_questions_unique = EXCLUDED.attempted_questions_unique,'
            'completed_questions_unique = EXCLUDED.completed_questions_unique,'
            'plag_score_99_unique = EXCLUDED.plag_score_99_unique,'
            'plag_score_95_unique = EXCLUDED.plag_score_95_unique,'
            'plag_score_90_unique = EXCLUDED.plag_score_90_unique;',
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
    'ARL_assignments_x_users_dag',
    default_args=default_args,
    concurrency=4,
    max_active_tasks=6,
    max_active_runs=6,
    description='An Analytics Reporting Layer DAG for assignments per user per assignment data',
    schedule_interval='35 0 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS arl_assignments_x_users (
            id serial,
            table_unique_key text not null PRIMARY KEY,
            course_id int,
            course_name text,
            course_structure_class text,
            assignment_id bigint,
            assignment_title text,
            normal_assignment_type text,
            topic_template_id int,
            module_name text,
            assignment_release_date date,
            user_id bigint,
            student_name text,
            lead_type text, 
            label_mapping_status text,
            student_category text,
            assignment_question_count int,
            opened_questions int,
            attempted_questions int,
            completed_questions int,
            questions_with_plag_score_90 int,
            questions_with_plag_score_95 int,
            questions_with_plag_score_99 int,
            hidden boolean,
            opened_questions_unique int,
            attempted_questions_unique int,
            completed_questions_unique int,
            plag_score_99_unique int,
            plag_score_95_unique int,
            plag_score_90_unique int
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
                    when a.assignment_sub_type = 1 then 'General Assignment'
                    when a.assignment_sub_type = 2 then 'In-Class Assignment'
                    when a.assignment_sub_type = 3 then 'Post-Class Assignment'
                    when a.assignment_sub_type = 5 then 'Module Assignment'
                end as normal_assignment_type,
                module_mapping.topic_template_id,
                module_mapping.module_name,
                date(a.start_timestamp) as assignment_release_date,
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
               all_assignment_questions.assignment_question_count,
               count(distinct aqum.question_id) as opened_questions,
               count(distinct aqum.question_id) filter(where aqum.max_test_case_passed is not null) as attempted_questions,
               count(distinct aqum.question_id) filter(where aqum.all_test_case_passed = 'true') as completed_questions,
               count(distinct aqum.question_id) filter(where plagiarism_score >= 0.90) as questions_with_plag_score_90,
               count(distinct aqum.question_id) filter(where plagiarism_score >= 0.95) as questions_with_plag_score_95,
               count(distinct aqum.question_id) filter(where plagiarism_score >= 0.99) as questions_with_plag_score_99,
               a.hidden,
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
               end as plag_score_90_unique
            from
                assignments a 
            join courses c 
                on c.course_id = a.course_id and a.original_assignment_type = 1
            join course_user_mapping cum 
                on cum.course_id = c.course_id and cum.status in (8,9,11,12,30)
            left join users_info ui 
                on ui.user_id = cum.user_id 
            left join course_user_category_mapping cucm 
                on cucm.user_id = cum.user_id and cum.course_id = cucm.course_id 
            left join assignment_question_user_mapping aqum 
                on aqum.user_id = cum.user_id and a.assignment_id = aqum.assignment_id
            left join 
                (select 
                    aqm.assignment_id,
                    count(distinct aqm.question_id) as assignment_question_count
                from 
                    assignment_question_mapping aqm 
                group by 1) all_assignment_questions
                    on all_assignment_questions.assignment_id = a.assignment_id
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
            group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,23;
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