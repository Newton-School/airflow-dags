from airflow import DAG
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

assignment_per_dags = Variable.get("assignment_per_dag", 4000)

total_number_of_sub_dags = Variable.get("total_number_of_sub_dags", 5)

total_number_of_extraction_cps_dags = Variable.get("total_number_of_extraction_cps_dags", 10)

dag = DAG(
    'ARL_assignments_x_user_ques_started_at_limit_offset_dag',
    default_args=default_args,
    concurrency=4,
    max_active_tasks=6,
    max_active_runs=6,
    description='An Analytics Reporting Layer DAG for assignments per user per assignment question data at started_at and release date level',
    schedule_interval='0 2 * * *',
    catchup=False
)

# Root Level Create Table
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
            plagiarism_score real,
            user_placement_status text
        );
    ''',
    dag=dag
)


# Leaf Level Abstraction
def extract_data_to_nested(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres_result_db')
    pg_conn = pg_hook.get_conn()
    ti = kwargs['ti']
    current_assignment_sub_dag_id = kwargs['current_assignment_sub_dag_id']
    current_cps_sub_dag_id = kwargs['current_cps_sub_dag_id']
    transform_data_output = ti.xcom_pull(
        task_ids=f'transforming_data_{current_assignment_sub_dag_id}.extract_and_transform_individual_assignment_sub_dag_{current_assignment_sub_dag_id}_cps_sub_dag_{current_cps_sub_dag_id}.transform_data')
    for transform_row in transform_data_output:
        pg_cursor = pg_conn.cursor()
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
            'plagiarism_score,'
            'user_placement_status)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
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
            'plagiarism_score = EXCLUDED.plagiarism_score,'
            'user_placement_status = EXCLUDED.user_placement_status;',
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
            )
        )
        pg_conn.commit()
        pg_cursor.close()
    pg_conn.close()


def number_of_rows_per_assignment_sub_dag_func(start_assignment_id, end_assignment_id):
    return PostgresOperator(
        task_id='number_of_rows_per_assignment_sub_dag',
        postgres_conn_id='postgres_result_db',
        dag=dag,
        sql=''' select count(table_unique_key) from
        (select * from
            (with batch_strength_details as
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
                        and c.course_structure_id in (1,6,7,8,11,12,13,14,18,19,20,22,23,26,34,44)
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
                    aqum.plagiarism_score,
                    cum.user_placement_status
                from
                    assignments a 
                join courses c 
                    on c.course_id = a.course_id and c.course_structure_id in (1,6,7,8,11,12,13,14,18,19,20,22,23,26,34,44)
                        and a.original_assignment_type = 1 and a.hidden = false
                            and (a.assignment_id between %d and %d)
                join course_user_mapping cum 
                    on cum.course_id = c.course_id and cum.status in (8,9,11,12,30)
                left join course_user_category_mapping cucm 
                    on cucm.user_id = cum.user_id 
                        and c.course_id = cucm.course_id 
                left join user_activity_status_mapping uasm 
                    on uasm.user_id = cum.user_id 
                left join users_info ui 
                    on ui.user_id = cum.user_id 
                join assignment_question_mapping_new_logic aqm 
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
                group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31)
                
            select 
                concat(user_id, assignment_id, topic_template_id, question_id) as table_unique_key,
                *
            from questions_details
            group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32) final_query
        ) query_rows;
            ''' % (start_assignment_id, end_assignment_id),
    )


# Python Limit Offset generator
def limit_offset_generator_func(**kwargs):
    ti = kwargs['ti']
    current_assignment_sub_dag_id = kwargs['current_assignment_sub_dag_id']
    current_cps_sub_dag_id = kwargs['current_cps_sub_dag_id']
    count_cps_rows = ti.xcom_pull(
        task_ids=f'transforming_data_{current_assignment_sub_dag_id}.number_of_rows_per_assignment_sub_dag')
    print(count_cps_rows)
    total_count_rows = count_cps_rows[0][0]
    return {
        "limit": total_count_rows // total_number_of_extraction_cps_dags,
        "offset": current_cps_sub_dag_id * (total_count_rows // total_number_of_extraction_cps_dags) + 1,
    }


# TODO: Add Count Logic
def transform_data_per_query(start_assignment_id, end_assignment_id, cps_sub_dag_id, current_assignment_sub_dag_id):
    return PostgresOperator(
        task_id='transform_data',
        postgres_conn_id='postgres_result_db',
        dag=dag,
        params={
            'current_cps_sub_dag_id': cps_sub_dag_id,
            'current_assignment_sub_dag_id': current_assignment_sub_dag_id,
            'task_key': f'transforming_data_{current_assignment_sub_dag_id}.extract_and_transform_individual_assignment_sub_dag_{current_assignment_sub_dag_id}_cps_sub_dag_{cps_sub_dag_id}.limit_offset_generator'
        },
        sql=''' select * from
        (with batch_strength_details as 
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
                    and c.course_structure_id in (1,6,7,8,11,12,13,14,18,19,20,22,23,26,34,44)
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
                aqum.plagiarism_score,
                cum.user_placement_status
            from
                assignments a 
            join courses c 
                on c.course_id = a.course_id and c.course_structure_id in (1,6,7,8,11,12,13,14,18,19,20,22,23,26,34,44)
                    and a.original_assignment_type = 1 and a.hidden = false
                        and (a.assignment_id between %d and %d)
            join course_user_mapping cum 
                on cum.course_id = c.course_id and cum.status in (8,9,11,12,30)
            left join course_user_category_mapping cucm 
                on cucm.user_id = cum.user_id 
                    and c.course_id = cucm.course_id 
            left join user_activity_status_mapping uasm 
                on uasm.user_id = cum.user_id 
            left join users_info ui 
                on ui.user_id = cum.user_id 
            join assignment_question_mapping_new_logic aqm 
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
            group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31)
            
        select 
            concat(user_id, assignment_id, topic_template_id, question_id) as table_unique_key,
            *
        from questions_details
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32) final_query
        limit {{ ti.xcom_pull(task_ids=params.task_key, key='return_value').limit }} 
        offset {{ ti.xcom_pull(task_ids=params.task_key, key='return_value').offset }}
        ;
            ''' % (start_assignment_id, end_assignment_id),
    )


for assignment_sub_dag_id in range(int(total_number_of_sub_dags)):
    with TaskGroup(group_id=f"transforming_data_{assignment_sub_dag_id}", dag=dag) as assignment_sub_dag_task_group:
        assignment_start_id = assignment_sub_dag_id * int(assignment_per_dags) + 1
        assignment_end_id = (assignment_sub_dag_id + 1) * int(assignment_per_dags)
        number_of_rows_per_assignment_sub_dag = number_of_rows_per_assignment_sub_dag_func(assignment_start_id,
                                                                                           assignment_end_id)

        for cps_sub_dag_id in range(int(total_number_of_extraction_cps_dags)):
            with TaskGroup(
                    group_id=f"extract_and_transform_individual_assignment_sub_dag_{assignment_sub_dag_id}_cps_sub_dag_{cps_sub_dag_id}",
                    dag=dag) as cps_sub_dag:
                limit_offset_generator = PythonOperator(
                    task_id='limit_offset_generator',
                    python_callable=limit_offset_generator_func,
                    provide_context=True,
                    op_kwargs={
                        'current_assignment_sub_dag_id': assignment_sub_dag_id,
                        'current_cps_sub_dag_id': cps_sub_dag_id,
                    },
                    dag=dag,
                )

                transform_data = transform_data_per_query(assignment_start_id, assignment_end_id, cps_sub_dag_id,
                                                          assignment_sub_dag_id)

                extract_python_data = PythonOperator(
                    task_id='extract_python_data',
                    python_callable=extract_data_to_nested,
                    provide_context=True,
                    op_kwargs={
                        'current_assignment_sub_dag_id': assignment_sub_dag_id,
                        'current_cps_sub_dag_id': cps_sub_dag_id
                    },
                    dag=dag,
                )

                limit_offset_generator >> transform_data >> extract_python_data

            number_of_rows_per_assignment_sub_dag >> cps_sub_dag

    create_table >> assignment_sub_dag_task_group