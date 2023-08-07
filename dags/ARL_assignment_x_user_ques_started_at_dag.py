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
            'INSERT INTO arl_assignments_x_users_ques_started_at (table_unique_key,'
            'user_id,'
            'student_name,'
            'student_category,'
            'lead_type,'
            'activity_status_7_days,'
            'activity_status_14_days,'
            'activity_status_30_days,'
            'assignment_id,'
            'assignment_title,'
            'release_date,'
            'normal_assignment_type,'
            'course_id,'
            'course_name,'
            'course_structure_class,'
            'admin_unit_name,'
            'label_mapping_status,'
            'enrolled_students,'
            'label_marked_students,'
            'isa_cancelled,'
            'deferred_students,'
            'foreclosed_students,'
            'rejected_by_ns_ops,'
            'question_id,'
            'topic_template_id,'
            'module_name,'
            'question_started_at,'
            'question_completed_at,'
            'max_test_case_passed,'
            'all_test_case_passed,'
            'plagiarism_score)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (table_unique_key) do update set student_name = EXCLUDED.student_name,'
            'student_category = EXCLUDED.student_category,'
            'lead_type = EXCLUDED.lead_type,'
            'activity_status_7_days = EXCLUDED.activity_status_7_days,'
            'activity_status_14_days = EXCLUDED.activity_status_14_days,'
            'activity_status_30_days = EXCLUDED.activity_status_30_days,'
            'assignment_title = EXCLUDED.assignment_title,'
            'release_date = EXCLUDED.release_date,'
            'normal_assignment_type = EXCLUDED.normal_assignment_type,'
            'course_name = EXCLUDED.course_name,'
            'course_structure_class = EXCLUDED.course_structure_class,'
            'admin_unit_name = EXCLUDED.admin_unit_name,'
            'label_mapping_status = EXCLUDED.label_mapping_status,'
            'enrolled_students = EXCLUDED.enrolled_students,'
            'label_marked_students = EXCLUDED.label_marked_students,'
            'isa_cancelled = EXCLUDED.isa_cancelled,'
            'deferred_students = EXCLUDED.deferred_students,'
            'foreclosed_students = EXCLUDED.foreclosed_students,'
            'rejected_by_ns_ops = EXCLUDED.rejected_by_ns_ops,'
            'topic_template_id = EXCLUDED.topic_template_id,'
            'module_name = EXCLUDED.module_name,'
            'question_started_at = EXCLUDED.question_started_at,'
            'question_completed_at = EXCLUDED.question_completed_at,'
            'max_test_case_passed = EXCLUDED.max_test_case_passed,'
            'all_test_case_passed = EXCLUDED.all_test_case_passed,'
            'plagiarism_score = EXCLUDED.plagiarism_score;',
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
            )
        )
    pg_conn.commit()


dag = DAG(
    'ARL_assignments_x_user_ques_started_at_dag',
    default_args=default_args,
    concurrency=4,
    max_active_tasks=6,
    max_active_runs=6,
    description='An Analytics Reporting Layer DAG for assignments per user per assignment question data at started_at and release date level',
    schedule_interval='35 0 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS arl_assignments_x_users_ques_started_at (
            id serial,
            table_unique_key text not null PRIMARY KEY,
            user_id bigint,
            student_name text,
            student_category text,
            lead_type text,
            activity_status_7_days text,
            activity_status_14_days text,
            activity_status_30_days text,
            assignment_id bigint,
            assignment_title text,
            release_date date,
            normal_assignment_type text,
            course_id int,
            course_name text,
            course_structure_class text,
            admin_unit_name text,
            label_mapping_status text,
            enrolled_students int,
            label_marked_students int,
            isa_cancelled int,
            deferred_students int,
            foreclosed_students int,
            rejected_by_ns_ops int,
            question_id bigint,
            topic_template_id int, 
            module_name text,
            question_started_at timestamp,
            question_completed_at timestamp,
            max_test_case_passed int,
            all_test_case_passed boolean,
            plagiarism_score real
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_result_db',
    sql='''with batch_strength_details as 
            (select 
                c.course_id,
                c.course_structure_id,
                count(distinct cum.user_id) filter (where cum.label_id is null and cum.status in (8,9)) as enrolled_students,
                count(distinct cum.user_id) filter (where cum.label_id is not null and cum.status in (8,9)) as label_marked_students,
                count(distinct cum.user_id) filter (where c.course_structure_id in (1,18) and cum.status in (11,12)) as isa_cancelled,
                count(distinct cum.user_id) filter (where c.course_structure_id not in (1,18) and cum.status in (30)) as deferred_students,
                count(distinct cum.user_id) filter (where c.course_structure_id not in (1,18) and cum.status in (11)) as foreclosed_students,
                count(distinct cum.user_id) filter (where c.course_structure_id not in (1,18) and cum.status in (12)) as rejected_by_ns_ops
            from
                courses c 
            join course_user_mapping cum 
                on cum.course_id = c.course_id and cum.status in (8,9,11,12,30)
                    and c.course_structure_id in (1,6,8,11,12,13,14,18,19,20,22,23,26,34)
            group by 1,2),
            
        questions_details as
            (select 
                cum.user_id,
                concat(ui.first_name,' ', ui.last_name) as student_name,
                cucm.student_category,
                ui.lead_type,
                uasm.activity_status_7_days,
                uasm.activity_status_14_days,
                uasm.activity_status_30_days,
                a.assignment_id,
                a.title as assignment_title,
                date(a.start_timestamp) as release_date,
                case 
                    when a.assignment_sub_type = 1 then 'General Assignment'
                    when a.assignment_sub_type = 2 then 'In-Class Assignment'
                    when a.assignment_sub_type = 3 then 'Post-Class Assignment'
                    when a.assignment_sub_type = 5 then 'Module Assignment'
                end as normal_assignment_type,
                c.course_id,
                c.course_name,
                c.course_structure_class,
                cum.admin_unit_name,
                case 
                    when cum.label_id is null and cum.status in (8,9) then 'Enrolled Student'
                    when cum.label_id is not null and cum.status in (8,9) then 'Label Marked Student'
                    when c.course_structure_id in (1,18) and cum.status in (11,12) then 'ISA Cancelled Student'
                    when c.course_structure_id not in (1,18) and cum.status in (30) then 'Deferred Student'
                    when c.course_structure_id not in (1,18) and cum.status in (11) then 'Foreclosed Student'
                    when c.course_structure_id not in (1,18) and cum.status in (12) then 'Reject by NS-Ops'
                    else 'Mapping Error'
                end as label_mapping_status,
                enrolled_students,
                label_marked_students,
                isa_cancelled,
                deferred_students,
                foreclosed_students,
                rejected_by_ns_ops,
                aqm.question_id,
                t.topic_template_id,
                t.template_name as module_name,
                question_started_at,
                question_completed_at,
                max_test_case_passed,
                aqum.all_test_case_passed,
                aqum.plagiarism_score
            from
                assignments a 
            join courses c 
                on c.course_id = a.course_id and c.course_structure_id in (1,6,8,11,12,13,14,18,19,20,22,23,26,34)
                    and a.original_assignment_type = 1 and a.hidden = false
            join course_user_mapping cum 
                on cum.course_id = c.course_id and cum.status in (8,9,11,12,30)
            left join course_user_category_mapping cucm 
                on cucm.user_id = cum.user_id 
                    and c.course_id = cucm.course_id 
            left join user_activity_status_mapping uasm 
                on uasm.user_id = cum.user_id 
            left join users_info ui 
                on ui.user_id = cum.user_id 
            join assignment_question_mapping aqm 
                on aqm.assignment_id = a.assignment_id 
            left join assignment_question_user_mapping_new aqum
                on a.assignment_id = aqum.assignment_id and aqum.user_id = cum.user_id
                    and aqum.question_id = aqm.question_id 
            left join batch_strength_details
                on c.course_id = batch_strength_details.course_id
            left join assignment_question aq 
                on aq.assignment_question_id = aqm.question_id
            left join topics t 
                on aq.topic_id  = t.topic_id 
                    and t.topic_template_id in (102,103,119,334,336,338,339,340,341,342,344,410)
            group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30)
            
        select 
            concat(user_id, assignment_id, topic_template_id, question_id) as table_unique_key,
            *
        from questions_details
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31;
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