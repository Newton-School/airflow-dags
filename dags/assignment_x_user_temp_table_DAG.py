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

dag = DAG(
    'assignment_x_user_temptable_transformation_DAG',
    default_args=default_args,
    description='A DAG for assignment x users table transformation',
    schedule_interval='0 23 * * *',
    catchup=False
)
create_temp_table = PostgresOperator(
    task_id='create_temp_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TEMP TABLE IF NOT EXISTS questions_released (
            id serial not null PRIMARY KEY,
            user_id bigint not null,
            course_id bigint,
            assignment_id bigint,
            question_id bigint
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data_questions_released',
    postgres_conn_id='postgres_read_replica',
    sql='''
        (select
                distinct courses_courseusermapping.user_id,
                courses_course.id as course_id,
                assignments_assignment.id as assignment_id,
                aaq.id as question_id
            from 
                assignments_assignment
            join courses_course
                on courses_course.id = assignments_assignment.course_id
            left join courses_courseusermapping on courses_courseusermapping.course_id = courses_course.id
            left join courses_coursestructure
                on courses_coursestructure.id = courses_course.course_structure_id
            join assignments_assignmentquestionmapping aaqm
                on aaqm.assignment_id = assignments_assignment.id
            join assignments_assignmentquestion aaq
                on aaq.id = aaqm.assignment_question_id
            order by 1,2)
            
            union 
    
            (select
                    courses_courseusermapping.user_id,
                    courses_course.id as course_id,
                    assignments_assignment.id  as assignment_id,
                    assignments_assignmentcourseuserrandomassignedquestionmapping.assignment_question_id as question_id
                from
                    assignments_assignment
                left join courses_course 
                    on courses_course.id = assignments_assignment.course_id
                left join courses_courseusermapping on courses_courseusermapping.course_id = courses_course.id
                left join courses_coursestructure
                    on courses_coursestructure.id = courses_course.course_structure_id
                left join assignments_assignmentcourseuserrandomassignedquestionmapping 
                    on assignments_assignmentcourseuserrandomassignedquestionmapping.course_user_mapping_id = courses_courseusermapping.id
                    and assignments_assignmentcourseuserrandomassignedquestionmapping.assignment_id = assignments_assignment.id
                
                where assignments_assignment.original_assignment_type in (3,4));
        ''',
    dag=dag
)
def extract_exception_logs(**kwargs):
    transform_data_output = kwargs['ti'].xcom_pull(task_ids=extract_python_data)
    for logs in transform_data_output:
        print(logs)


def extract_data_to_nested(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres_result_db')
    pg_conn = pg_hook.get_conn()
    pg_cursor = pg_conn.cursor()
    ti = kwargs['ti']
    transform_data_output = ti.xcom_pull(task_ids='transform_data_questions_released')
    sql_execution_errors = []
    for transform_row in transform_data_output:
        try:
            pg_cursor.execute(
                'INSERT INTO questions_released (user_id,course_id,assignment_id,question_id)'
                'VALUES (%s,%s,%s,%s);',
                (
                    transform_row[0],
                    transform_row[1],
                    transform_row[2],
                    transform_row[3],
                )
            )
        except Exception as err:
            sql_execution_errors.append(str(err))
    pg_conn.commit()
    return sql_execution_errors


extract_python_data = PythonOperator(
    task_id='extract_python_data',
    python_callable=extract_data_to_nested,
    provide_context=True,
    dag=dag
)

# extract_data = PostgresOperator(
#     task_id='extract_data',
#     postgres_conn_id='postgres_result_db',
#     sql='''SELECT * FROM {{ task_instance.xcom_pull(task_ids='transform_data') }}''',
#     dag=dag
# )

create_temp_table >> transform_data >> extract_python_data
