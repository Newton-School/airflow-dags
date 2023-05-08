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
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 16),
}
assignment_per_dags = Variable.get("assignment_per_dag", 40)
total_number_of_sub_dags = Variable.get("total_number_of_sub_dags", 15)


def extract_data_to_nested(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres_result_db')
    pg_conn = pg_hook.get_conn()
    pg_cursor = pg_conn.cursor()
    ti = kwargs['ti']
    current_task_index = kwargs['current_task_index']
    transform_data_output = ti.xcom_pull(task_ids=f'transforming_data_{current_task_index}.transform_data')
    for transform_row in transform_data_output:
        print(transform_row)
        pg_cursor.execute(
            'INSERT INTO assignment_random_question_mapping (table_unique_key,user_id,course_id,assignment_id,question_id)'
            'VALUES (%s,%s,%s,%s,%s)'
            'on conflict (table_unique_key) do update set course_id = EXCLUDED.course_id,'
            'assignment_id=EXCLUDED.assignment_id,question_id=EXCLUDED.question_id,user_id=EXCLUDED.user_id ;',
            (
                transform_row[0],
                transform_row[1],
                transform_row[2],
                transform_row[3],
                transform_row[4],
            )
        )
    pg_conn.commit()


dag = DAG(
    'assignment_random_questions_released_DAG',
    default_args=default_args,
    description='A DAG for assignment questions released though the random flow',
    schedule_interval='30 16 * * *',
    catchup=False
)


def transform_data_per_query(start_assignment_id, end_assignment_id):
    return PostgresOperator(
        task_id='transform_data',
        postgres_conn_id='postgres_read_replica',
        dag=dag,
        sql='''select distinct cast(concat(assignments_assignment.id, row_number() over(order by assignments_assignment.id)) as double precision) as table_unique_key,
                    courses_courseusermapping.user_id,
                    courses_course.id as course_id,
                    assignments_assignment.id  as assignment_id,
                    assignments_assignmentcourseuserrandomassignedquestionmapping.assignment_question_id as question_id
                from
                    assignments_assignment
                left join courses_course 
                    on courses_course.id = assignments_assignment.course_id and assignments_assignment.random_assignment_questions = true
                left join courses_courseusermapping on courses_courseusermapping.course_id = courses_course.id
                left join assignments_assignmentcourseuserrandomassignedquestionmapping 
                    on assignments_assignmentcourseuserrandomassignedquestionmapping.course_user_mapping_id = courses_courseusermapping.id
                    and assignments_assignmentcourseuserrandomassignedquestionmapping.assignment_id = assignments_assignment.id

                where (assignments_assignment.id between %d and %d)
        ;
            ''' % (start_assignment_id, end_assignment_id),
    )


create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS assignment_random_question_mapping (
            id serial not null,
            table_unique_key double precision not null PRIMARY KEY,
            user_id bigint,
            course_id bigint,
            assignment_id bigint,
            question_id bigint
        );
    ''',
    dag=dag
)
for i in range(int(total_number_of_sub_dags)):
    with TaskGroup(group_id=f"transforming_data_{i}", dag=dag) as sub_dag_task_group:
        transform_data = transform_data_per_query(i * int(assignment_per_dags) + 1, (i + 1) * int(assignment_per_dags))
        extract_python_data = PythonOperator(
            task_id='extract_python_data',
            python_callable=extract_data_to_nested,
            provide_context=True,
            op_kwargs={'current_task_index': i},
            dag=dag,
        )
        transform_data >> extract_python_data
    create_table >> sub_dag_task_group