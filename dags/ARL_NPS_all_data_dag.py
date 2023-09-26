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
            'INSERT INTO arl_nps_info (table_unique_key,user_id,'
                'student_name,'
                'lead_type,'
                'label_mapping_status,'
                'course_id,'
                'course_name,'
                'course_structure_class,'
                'course_structure_id,'
                'student_category,'
                'admin_course_id,'
                'admin_unit_name,'
                'form_fill_date,'
                'nps_rating,'
                'student_nps_class,'
                'lecture_session,'
                'mentor_session,'
                'mock_interviews,'
                'assignments,'
                'support_from_ns_team,'
                'contests,'
                'curriculum,'
                'pace_of_the_course,'
                'other,'
                'subjective_feedback,'
                'course_start_timestamp,'
                'activity_status_7_days,'
                'activity_status_14_days,'
                'activity_status_30_days)'
                'VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                'on conflict (table_unique_key) do update set student_name = EXCLUDED.student_name,'
                'lead_type = EXCLUDED.lead_type,'
                'label_mapping_status = EXCLUDED.label_mapping_status,'
                'course_name = EXCLUDED.course_name,'
                'course_structure_class = EXCLUDED.course_structure_class,'
                'course_structure_id = EXCLUDED.course_structure_id,'
                'student_category = EXCLUDED.student_category,'
                'admin_unit_name = EXCLUDED.admin_unit_name,'
                'nps_rating = EXCLUDED.nps_rating,'
                'student_nps_class = EXCLUDED.student_nps_class,'
                'lecture_session = EXCLUDED.lecture_session,'
                'mentor_session = EXCLUDED.mentor_session,'
                'mock_interviews = EXCLUDED.mock_interviews,'
                'assignments = EXCLUDED.assignments,'
                'support_from_ns_team = EXCLUDED.support_from_ns_team,'
                'contests = EXCLUDED.contests,'
                'curriculum = EXCLUDED.curriculum,'
                'pace_of_the_course = EXCLUDED.pace_of_the_course,'
                'other = EXCLUDED.other,'
                'subjective_feedback = EXCLUDED.subjective_feedback,'
                'course_start_timestamp = EXCLUDED.course_start_timestamp,'
                'activity_status_7_days = EXCLUDED.activity_status_7_days,'
                'activity_status_14_days = EXCLUDED.activity_status_14_days,'
                'activity_status_30_days = EXCLUDED.activity_status_30_days;',
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
    'arl_nps_info_dag',
    default_args=default_args,
    concurrency=4,
    max_active_tasks=6,
    max_active_runs=6,
    description='ARL dag for all NPS data with feedback_form_id = 4428 and feedback_question_id in (308,319,320,321,323)',
    schedule_interval='15 1 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS arl_nps_info (
            id serial,
            table_unique_key text not null PRIMARY KEY,
            user_id bigint,
            student_name text,
            lead_type text,
            label_mapping_status text,
            course_id int,
            course_name text,
            course_structure_class text,
            course_structure_id int,
            student_category text,
            admin_course_id int,
            admin_unit_name text,
            form_fill_date date,
            nps_rating int,
            student_nps_class text,
            lecture_session text,
            mentor_session text,
            mock_interviews text,
            assignments text,
            support_from_ns_team text,
            contests text,
            curriculum text,
            pace_of_the_course text,
            other text,
            subjective_feedback text,
            course_start_timestamp timestamp,
            activity_status_7_days text,
            activity_status_14_days text,
            activity_status_30_days text
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_result_db',
    sql='''with raw_data as 
                (select distinct 
                    ffar.user_id,
                    concat(ui.first_name, ' ', ui.last_name) as student_name,
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
                    c.course_id,
                    c.course_name,
                    c.course_start_timestamp,
                    cum.admin_course_id,
                    cum.admin_unit_name,
                    c.course_structure_class,
                    c.course_structure_id,
                    cucm.student_category,
                    date(ffar.completed_at) as form_fill_date,
                    case
                        when ffar.feedback_question_id = 308 then cast(feedback_answer as int) 
                    end as nps_rating,
                    case
                        when ffar.feedback_question_id = 308 and feedback_answer in ('9','10') then 'Promoter'
                        when ffar.feedback_question_id = 308 and feedback_answer in ('7','8') then 'Passive'
                        when ffar.feedback_question_id = 308 and feedback_answer in ('1','2','3','4','5','6') then 'Detractor'
                    end as student_nps_class,
                    
                    case 
                        when ffar.feedback_question_id = 323 then feedback_answer
                    end as subjective_feedback,
                    case 
                        when ffar.feedback_answer_id = 162 then 'Yes'
                        else null
                    end as lecture_session,
                    case 
                        when ffar.feedback_answer_id = 163 then 'Yes'
                        else null
                    end as mentor_session,
                    case 
                        when ffar.feedback_answer_id = 164 then 'Yes'
                        else null
                    end as mock_interviews,
                    case 
                        when ffar.feedback_answer_id = 165 then 'Yes'
                        else null
                    end as assignments,
                    case 
                        when ffar.feedback_answer_id = 166 then 'Yes'
                        else null
                    end as support_from_NS_team,
                    case 
                        when ffar.feedback_answer_id = 167 then 'Yes'
                        else null
                    end as contests,
                    case 
                        when ffar.feedback_answer_id = 168 then 'Yes'
                        else null
                    end as curriculum,
                    case 
                        when ffar.feedback_answer_id = 169 then 'Yes'
                        else null
                    end as other,
                    case 
                        when ffar.feedback_answer_id = 170 then 'Yes'
                        else null
                    end as pace_of_the_course
                from 
                    feedback_form_all_responses_new ffar
                join course_user_mapping cum 
                    on cum.user_id = ffar.user_id and cum.status in (8,9,11,12,30)
                        and feedback_form_id = 4428
                join courses c 
                    on c.course_id = cum.course_id and lower(c.unit_type) like 'learning' 
                        and c.course_structure_id in (1,6,7,8,11,12,14,18,19,20,22,23,26,44,47)
                left join users_info ui
                    on ui.user_id = ffar.user_id
                left join course_user_category_mapping cucm 
                    on cucm.user_id = ffar.user_id and cum.course_id = cucm.course_id
                left join feedback_forms_and_questions ffaq
                    on ffaq.feedback_form_id = ffar.feedback_form_id and ffaq.feedback_question_id = ffar.feedback_question_id)
                    
            select 
                concat(raw_data.user_id, course_id, admin_course_id, extract('year' from form_fill_date), extract('month' from form_fill_date),extract('day' from form_fill_date)) as table_unique_key,
                raw_data.user_id,
                raw_data.student_name,
                raw_data.lead_type,
                label_mapping_status,
                course_id,
                course_name,
                course_structure_class,
                course_structure_id,
                student_category,
                admin_course_id,
                admin_unit_name,
                form_fill_date,
                max(nps_rating) as nps_rating,
                max(student_nps_class) as student_nps_class,
                max(lecture_session) as lecture_session,
                max(mentor_session) as mentor_session,
                max(mock_interviews) as mock_interviews,
                max(assignments) as assignments,
                max(support_from_ns_team) as support_from_ns_team,
                max(contests) as contests,
                max(curriculum) as curriculum,
                max(pace_of_the_course) as pace_of_the_course,
                max(other) as other,
                max(subjective_feedback) as subjective_feedback,
                course_start_timestamp,
                uasm.activity_status_7_days,
                uasm.activity_status_14_days,
                uasm.activity_status_30_days
            from 
                raw_data
            left join user_activity_status_mapping uasm
            	on uasm.user_id = raw_data.user_id
            group by 1,2,3,4,5,6,7,8,9,10,11,12,13,26,27,28,29;
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