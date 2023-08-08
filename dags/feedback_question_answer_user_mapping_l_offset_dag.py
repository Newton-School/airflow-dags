from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from datetime import datetime


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
    'feedback_question_answer_user_mapping_limit_offset_dag',
    default_args=default_args,
    concurrency=4,
    max_active_tasks=6,
    max_active_runs=6,
    description='per user per feedback question response',
    schedule_interval='45 21 * * *',
    catchup=False
)

# Root Level Create Table
create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS feedback_form_all_responses_new (
            table_unique_key text not null PRIMARY KEY,
            fuqam_id bigint,
            m2m_id bigint,
            feedback_form_user_mapping_id bigint,
            feedback_form_user_mapping_hash text,
            user_id bigint,
            feedback_form_id bigint,
            course_id int,
            feedback_question_id int,
            created_at timestamp,
            completed_at timestamp,
            entity_content_type_id int,
            entity_object_id bigint,
            feedback_answer_id int,
            feedback_form_user_question_answer_mapping_id bigint,
            feedback_answer text
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
            'INSERT INTO feedback_form_all_responses_new (table_unique_key,'
            'fuqam_id,m2m_id,feedback_form_user_mapping_id,feedback_form_user_mapping_hash,user_id,feedback_form_id,'
            'course_id,feedback_question_id, created_at, completed_at,'
            'entity_content_type_id,entity_object_id,feedback_answer_id,'
            'feedback_form_user_question_answer_mapping_id,feedback_answer)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (table_unique_key) do update set feedback_form_user_mapping_id = EXCLUDED.feedback_form_user_mapping_id,'
            'fuqam_id = EXCLUDED.fuqam_id,'
            'm2m_id = EXCLUDED.m2m_id,'
            'user_id = EXCLUDED.user_id,'
            'feedback_form_id = EXCLUDED.feedback_form_id,'
            'feedback_form_user_mapping_hash = EXCLUDED.feedback_form_user_mapping_hash,'
            'course_id = EXCLUDED.course_id,'
            'feedback_question_id = EXCLUDED.feedback_question_id,'
            'created_at = EXCLUDED.created_at,'
            'completed_at = EXCLUDED.completed_at,'
            'entity_content_type_id = EXCLUDED.entity_content_type_id,'
            'entity_object_id = EXCLUDED.entity_object_id,'
            'feedback_answer_id = EXCLUDED.feedback_answer_id,'
            'feedback_form_user_question_answer_mapping_id = EXCLUDED.feedback_form_user_question_answer_mapping_id,'
            'feedback_answer = EXCLUDED.feedback_answer;',
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
            )
        )
        pg_conn.commit()
        pg_cursor.close()
    pg_conn.close()


def number_of_rows_per_assignment_sub_dag_func(start_assignment_id, end_assignment_id):
    return PostgresOperator(
        task_id='number_of_rows_per_assignment_sub_dag',
        postgres_conn_id='postgres_read_replica',
        dag=dag,
        sql='''select count(table_unique_key) from 
        (with raw as
            (Select
                feedback_feedbackformuserquestionanswermapping.id as fuqam_id,
                feedback_feedbackformuserquestionanswerm2m.id as m2m_id,
                feedback_feedbackformusermapping.id as feedback_form_user_mapping_id,
                feedback_feedbackformusermapping.hash as feedback_form_user_mapping_hash,
                feedback_feedbackformusermapping.filled_by_id as user_id,
                feedback_feedbackform.id as feedback_form_id,
                feedback_feedbackformusermapping.course_id,
                feedback_feedbackquestion.id as feedback_question_id,
                feedback_feedbackformusermapping.created_at,
                feedback_feedbackformusermapping.completed_at,
                feedback_feedbackformusermapping.entity_content_type_id,
                feedback_feedbackformusermapping.entity_object_id,
                feedback_feedbackanswer.id as feedback_answer_id,
                feedback_feedbackformuserquestionanswermapping.id as feedback_form_user_question_answer_mapping_id,
                case
                    when feedback_feedbackanswer.text is null then feedback_feedbackformuserquestionanswermapping.other_answer
                    else feedback_feedbackanswer.text 
                end as feedback_answer
                
            from
                feedback_feedbackformusermapping
            
            join feedback_feedbackform
                on feedback_feedbackform.id = feedback_feedbackformusermapping.feedback_form_id
                    and (feedback_feedbackform.id between %d and %d)
            
            join feedback_feedbackformuserquestionanswermapping
                on feedback_feedbackformusermapping.id = feedback_feedbackformuserquestionanswermapping.feedback_form_user_mapping_id
            
            left join feedback_feedbackformuserquestionanswerm2m
                on feedback_feedbackformuserquestionanswermapping.id = feedback_feedbackformuserquestionanswerm2m.feedback_form_user_question_answer_mapping_id
            
            left join feedback_feedbackanswer
                on feedback_feedbackformuserquestionanswerm2m.feedback_answer_id = feedback_feedbackanswer.id
                
            left join feedback_feedbackquestion
                on feedback_feedbackformuserquestionanswermapping.feedback_question_id = feedback_feedbackquestion.id)
        select
            (case 
                    when m2m_id is null then concat(feedback_form_user_mapping_id,1,fuqam_id)
                    else concat(feedback_form_user_mapping_id,2,m2m_id) 
                end) as table_unique_key,
            raw.*
        from
            raw) query_rows;
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
        postgres_conn_id='postgres_read_replica',
        dag=dag,
        params={
            'current_cps_sub_dag_id': cps_sub_dag_id,
            'current_assignment_sub_dag_id': current_assignment_sub_dag_id,
            'task_key': f'transforming_data_{current_assignment_sub_dag_id}.extract_and_transform_individual_assignment_sub_dag_{current_assignment_sub_dag_id}_cps_sub_dag_{cps_sub_dag_id}.limit_offset_generator'
        },
        sql='''with raw as
            (Select
                feedback_feedbackformuserquestionanswermapping.id as fuqam_id,
                feedback_feedbackformuserquestionanswerm2m.id as m2m_id,
                feedback_feedbackformusermapping.id as feedback_form_user_mapping_id,
                feedback_feedbackformusermapping.hash as feedback_form_user_mapping_hash,
                feedback_feedbackformusermapping.filled_by_id as user_id,
                feedback_feedbackform.id as feedback_form_id,
                feedback_feedbackformusermapping.course_id,
                feedback_feedbackquestion.id as feedback_question_id,
                feedback_feedbackformusermapping.created_at,
                feedback_feedbackformusermapping.completed_at,
                feedback_feedbackformusermapping.entity_content_type_id,
                feedback_feedbackformusermapping.entity_object_id,
                feedback_feedbackanswer.id as feedback_answer_id,
                feedback_feedbackformuserquestionanswermapping.id as feedback_form_user_question_answer_mapping_id,
                case
                    when feedback_feedbackanswer.text is null then feedback_feedbackformuserquestionanswermapping.other_answer
                    else feedback_feedbackanswer.text 
                end as feedback_answer
                
            from
                feedback_feedbackformusermapping
            
            join feedback_feedbackform
                on feedback_feedbackform.id = feedback_feedbackformusermapping.feedback_form_id
                    and (feedback_feedbackform.id between %d and %d)
            
            join feedback_feedbackformuserquestionanswermapping
                on feedback_feedbackformusermapping.id = feedback_feedbackformuserquestionanswermapping.feedback_form_user_mapping_id
            
            left join feedback_feedbackformuserquestionanswerm2m
                on feedback_feedbackformuserquestionanswermapping.id = feedback_feedbackformuserquestionanswerm2m.feedback_form_user_question_answer_mapping_id
            
            left join feedback_feedbackanswer
                on feedback_feedbackformuserquestionanswerm2m.feedback_answer_id = feedback_feedbackanswer.id
                
            left join feedback_feedbackquestion
                on feedback_feedbackformuserquestionanswermapping.feedback_question_id = feedback_feedbackquestion.id)
        select
            (case 
                    when m2m_id is null then concat(feedback_form_user_mapping_id,1,fuqam_id)
                    else concat(feedback_form_user_mapping_id,2,m2m_id) 
                end) as table_unique_key,
            raw.*
        from
            raw
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