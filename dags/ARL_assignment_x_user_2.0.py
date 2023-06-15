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

assignment_per_dags = Variable.get("assignment_per_dag", 4000)

total_number_of_sub_dags = Variable.get("total_number_of_sub_dags", 5)

total_number_of_extraction_cps_dags = Variable.get("total_number_of_extraction_cps_dags", 10)

dag = DAG(
    'ARL_Assignments_x_user_2.0',
    default_args=default_args,
    concurrency=4,
    max_active_tasks=6,
    max_active_runs=6,
    description='An Analytics Reporting Layer DAG for Assignments x user',
    schedule_interval='45 0 * * *',
    catchup=False
)

# Root Level Create Table
create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS arl_assignments_x_users_2 (
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
             'INSERT INTO arl_assignments_x_users_2 (table_unique_key,user_id,assignment_id,'
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
        pg_cursor.close()
    pg_conn.close()


def number_of_rows_per_assignment_sub_dag_func(start_assignment_id, end_assignment_id):
    return PostgresOperator(
        task_id='number_of_rows_per_assignment_sub_dag',
        postgres_conn_id='postgres_result_db',
        dag=dag,
        sql='''  select count(table_unique_key) from(
        with 
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
                       on a.assignment_id = aqum.assignment_id and (a.assignment_id between %d and %d)
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
                       on a.assignment_id = aqum.assignment_id and (a.assignment_id between %d and %d)
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
                where (aqm.assignment_id between %d and %d)
                group by 1
               ),
            module_raw as (
                select atm.assignment_id,
                       t.topic_template_id,
                       t.template_name as module_name
                from assignment_topic_mapping atm 
                left join topics t 
                on atm.topic_id  = t.topic_id 
                where topic_template_id in (102,103,119,334,336,338,339,340,341,342,344,410)  and (atm.assignment_id between %d and %d)
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
                on module_raw.assignment_id = user_details.assignment_id) query_rows;
            ''' % (start_assignment_id, end_assignment_id,start_assignment_id, end_assignment_id,start_assignment_id, end_assignment_id,start_assignment_id, end_assignment_id),
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
        sql=''' with 
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
                       on a.assignment_id = aqum.assignment_id and (a.assignment_id between %d and %d)
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
                       on a.assignment_id = aqum.assignment_id and (a.assignment_id between %d and %d)
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
                where (aqm.assignment_id between %d and %d)
                group by 1
               ),
            module_raw as (
                select atm.assignment_id,
                       t.topic_template_id,
                       t.template_name as module_name
                from assignment_topic_mapping atm 
                left join topics t 
                on atm.topic_id  = t.topic_id 
                where topic_template_id in (102,103,119,334,336,338,339,340,341,342,344,410)  and (atm.assignment_id between %d and %d)
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
            ''' % (start_assignment_id, end_assignment_id,start_assignment_id, end_assignment_id,start_assignment_id, end_assignment_id,start_assignment_id, end_assignment_id),
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

