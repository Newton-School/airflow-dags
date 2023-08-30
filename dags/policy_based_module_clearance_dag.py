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
            'INSERT INTO arl_policy_based_module_clearance (table_unique_key,'
            'status,'
            'label_mapping_status,'
            'user_id,'
            'student_name,'
            'admin_course_id,'
            'admin_unit_name,'
            'topic_template_id,'
            'module_name,'
            'contest_completion_percent,'
            'module_contest_clearance_status,'
            'lectures_conducted,'
            'overall_lectures_attended,'
            'live_lectures_attended,'
            'overall_attendance,'
            'lecture_module_clearance_status,'
            'total_questions,'
            'questions_opened,'
            'questions_attempted,'
            'questions_comp,'
            'plag_at_99,'
            'overall_comp,'
            'assignment_module_clearance_status,'
            'overall_module_clearance)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (table_unique_key) do update set status = EXCLUDED.status,'
            'label_mapping_status = EXCLUDED.label_mapping_status,'
            'user_id = EXCLUDED.user_id,'
            'student_name = EXCLUDED.student_name,'
            'admin_course_id = EXCLUDED.admin_course_id,'
            'admin_unit_name = EXCLUDED.admin_unit_name,'
            'topic_template_id = EXCLUDED.topic_template_id,'
            'module_name = EXCLUDED.module_name,'
            'contest_completion_percent = EXCLUDED.contest_completion_percent,'
            'module_contest_clearance_status = EXCLUDED.module_contest_clearance_status,'
            'lectures_conducted = EXCLUDED.lectures_conducted,'
            'overall_lectures_attended = EXCLUDED.overall_lectures_attended,'
            'live_lectures_attended = EXCLUDED.live_lectures_attended,'
            'overall_attendance = EXCLUDED.overall_attendance,'
            'lecture_module_clearance_status = EXCLUDED.lecture_module_clearance_status,'
            'total_questions = EXCLUDED.total_questions,'
            'questions_opened = EXCLUDED.questions_opened,'
            'questions_attempted = EXCLUDED.questions_attempted,'
            'questions_comp = EXCLUDED.questions_comp,'
            'plag_at_99 = EXCLUDED.plag_at_99,'
            'overall_comp = EXCLUDED.overall_comp,'
            'assignment_module_clearance_status = EXCLUDED.assignment_module_clearance_status,'
            'overall_module_clearance = EXCLUDED.overall_module_clearance;',
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
            )
        )
    pg_conn.commit()


dag = DAG(
    'ARL_policy_based_module_clearance_dag',
    default_args=default_args,
    description='An Analytics Reporting Layer DAG to give policy based module clearance data on the basis of attendance, module contest and assignment completion',
    schedule_interval='50 4 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS arl_policy_based_module_clearance (
            id serial,
            table_unique_key text not null PRIMARY KEY,
            status int,
            label_mapping_status text,
            user_id bigint,
            student_name text,
            admin_course_id int,
            admin_unit_name text,
            topic_template_id int,
            module_name text,
            contest_completion_percent real,
            module_contest_clearance_status text,
            lectures_conducted int,
            overall_lectures_attended int,
            live_lectures_attended int,
            overall_attendance real,
            lecture_module_clearance_status text,
            total_questions int,
            questions_opened int,
            questions_attempted int,
            questions_comp int,
            plag_at_99 int,
            overall_comp real,
            assignment_module_clearance_status text ,
            overall_module_clearance text
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_result_db',
    sql='''-- user overall attendance 
            with module_attendance as 
                (select
                    user_id,
                    student_name,
                    admin_unit_name,
                    lead_type,
                    activity_status_14_days,
                    template_name,
                    count(distinct lecture_id) as lectures_conducted,
                    count(distinct lecture_id) filter (where overall_attendance = 1) as overall_lectures_attended,
                    count(distinct lecture_id) filter (where live_attendance = 1) as live_lectures_attended
                from
                    lecture_course_user_reports lcur
                where mandatory = true
                and template_name is not null
                group by 1,2,3,4,5,6),
            module_contest_data as 
                (with raw as 
                    (select 
                        assignment_id,
                        assignment_title,
                        assignment_release_date,
                        admin_course_id,
                        admin_unit_name,
                        user_id,
                        student_name,
                        topic_template_id,
                        module_name,
                        question_count,
                        completed_questions,
                        completed_questions * 1.0 / question_count as comp_percent,
                        case 
                            when completed_questions * 1.0 / question_count >= 0.4 then 'Cleared'
                            else 'Not Cleared'
                        end as contest_clearance_status
                    from
                        arl_contests_x_users acxu
                    where module_name is not null 
                    and hidden = false
                    group by 1,2,3,4,5,6,7,8,9,10,11,12) 
                
                select 
                    user_id,
                    student_name,
                    admin_course_id,
                    admin_unit_name, 
                    topic_template_id,
                    module_name,
                    100 * max(comp_percent) as contest_completion_percent,
                    case 
                        when max(comp_percent) >= 0.4 then 'Cleared'
                        else 'Not Cleared'
                    end as module_contest_clearance_status
                from
                    raw
                group by 1,2,3,4,5,6),
            
            -- overall assignment completion percent
            assignment_data as 
                (with lu_data as 
                    (with total_questions as 
                        (select 
                            c.course_id,
                            c.course_name,
                            t.template_name,
                            count(distinct aqm.question_id) as total_questions
                        from 
                            assignment_question_mapping_new_logic aqm
                        join assignments a 
                            on a.assignment_id = aqm.assignment_id and a.hidden = false
                                and a.original_assignment_type = 1
                        join courses c 
                            on c.course_id = a.course_id
                        join assignment_question aq
                            on aq.assignment_question_id = aqm.question_id
                        join topics t 
                            on t.topic_id = aq.topic_id 
                                and t.topic_template_id in (102,103,119,334,336,338,339,340,341,342,344,410)
                        group by 1,2,3)
                    select
                        aaxuqsa.course_id,
                        aaxuqsa.course_name,
                        aaxuqsa.user_id,
                        student_name,
                        cum.admin_course_id,
                        cum.admin_unit_name,
                        module_name,
                        total_questions.total_questions,
                        count(distinct aaxuqsa.question_id) filter (where question_started_at is not null) as questions_opened,
                        count(distinct aaxuqsa.question_id) filter (where max_test_case_passed is not null) as questions_attempted,
                        count(distinct aaxuqsa.question_id) filter (where all_test_case_passed is true) as questions_comp,
                        count(distinct aaxuqsa.question_id) filter (where aaxuqsa.plagiarism_score >= 0.99) as plag_at_99
                    from
                        arl_assignments_x_users_ques_started_at aaxuqsa
                    join total_questions
                        on total_questions.course_id = aaxuqsa.course_id
                            and aaxuqsa.module_name = total_questions.template_name
                    join assignment_question_mapping_new_logic aqm
                        on aqm.assignment_id = aaxuqsa.assignment_id
                            and aqm.question_id = aaxuqsa.question_id 
                    left join course_user_mapping cum
                        on cum.course_id = aaxuqsa.course_id 
                            and cum.user_id = aaxuqsa.user_id
                    group by 1,2,3,4,5,6,7,8)
                select 
                    user_id,
                    student_name,
                    admin_course_id,
                    admin_unit_name,
                    module_name, 
                    sum(total_questions) as total_questions,
                    sum(questions_opened) as questions_opened, 
                    sum(questions_attempted) as questions_attempted,
                    sum(questions_comp) as questions_comp,
                    sum(plag_at_99) as plag_at_99,
                    sum(questions_comp) * 100.0 / sum(total_questions) as overall_comp
                from
                    lu_data
                group by 1,2,3,4,5
                having sum(total_questions) <> 0),
                
            all_combined as
                (select 
                    module_contest_data.user_id,
                    module_contest_data.student_name,
                    module_contest_data.admin_course_id,
                    module_contest_data.admin_unit_name, 
                    module_contest_data.topic_template_id,
                    module_contest_data.module_name,
                    module_contest_data.contest_completion_percent,
                    module_contest_data.module_contest_clearance_status,		
                    module_attendance.lectures_conducted,
                    module_attendance.overall_lectures_attended,
                    module_attendance.live_lectures_attended,
                    module_attendance.overall_lectures_attended * 100.0 / module_attendance.lectures_conducted as overall_attendance,
                    case 
                        when module_attendance.overall_lectures_attended * 100.0 / module_attendance.lectures_conducted >= 70 then 'Module Cleared'
                        else 'Not Cleared'
                    end as lecture_module_clearance_status,
                    assignment_data.total_questions,
                    assignment_data.questions_opened,
                    assignment_data.questions_attempted,
                    assignment_data.questions_comp,
                    assignment_data.plag_at_99,
                    assignment_data.overall_comp,
                    case 
                        when assignment_data.overall_comp >= 70 then 'Module Cleared'
                        else 'Not Cleared'
                    end as assignment_module_clearance_status
                from
                    module_contest_data 
                left join module_attendance
                    on module_contest_data.user_id = module_attendance.user_id 
                        and module_contest_data.admin_unit_name = module_attendance.admin_unit_name 
                            and module_contest_data.module_name = module_attendance.template_name
                left join assignment_data
                    on assignment_data.user_id = module_contest_data.user_id 
                        and assignment_data.admin_unit_name = module_contest_data.admin_unit_name
                            and assignment_data.module_name = module_contest_data.module_name)
            select
                concat(all_combined.user_id, all_combined.admin_course_id, all_combined.topic_template_id) as table_unique_key, 
                cum.status,
                case 
                    when cum.label_id is null and cum.status in (8,9) then 'Enrolled Student'
                    when cum.label_id is not null and cum.status in (8,9) then 'Label Marked Student'
                    when c.course_structure_id in (1,18) and cum.status in (11,12) then 'ISA Cancelled Student'
                    when c.course_structure_id not in (1,18) and cum.status in (30) then 'Deferred Student'
                    when c.course_structure_id not in (1,18) and cum.status in (11) then 'Foreclosed Student'
                    when c.course_structure_id not in (1,18) and cum.status in (12) then 'Reject by NS-Ops'
                    else 'Mapping Error'
                end as label_mapping_status,
                all_combined.*,
                case 
                    when module_contest_clearance_status like 'Cleared' and assignment_module_clearance_status like 'Module Cleared'
                        and lecture_module_clearance_status like 'Module Cleared' then 'Module Cleared'
                    else 'Module NOT Cleared'
                end as overall_module_clearance
            from
                all_combined
            join course_user_mapping cum
                on cum.user_id = all_combined.user_id
                    and cum.course_name = all_combined.admin_unit_name
            join courses c 
                on c.course_id = cum.course_id 
            where lower(c.course_name) not like '%simulation course%'
            and lower(c.course_name) not like '%testing course%'
            group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23;
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