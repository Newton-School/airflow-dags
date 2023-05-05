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
            'INSERT INTO mentor_mentee_mapping (user_id,'
            'lead_type,email)'
            'VALUES (%s,%s,%s)'
            'on conflict (user_id) do update set lead_type = EXCLUDED.lead_type,'
            'email = EXCLUDED.email;',
            (
                transform_row[0],
                transform_row[1],
                transform_row[2]
            )
        )
    pg_conn.commit()


dag = DAG(
    'fresh_deferred_lead_type_dag',
    default_args=default_args,
    description='Classifies userid as fresh or deferred',
    schedule_interval='00 16 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS fresh_deferred_lead_type (
            user_id not null PRIMARY KEY,
            lead_type varchar(16),
            email varchar (256)
    );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''select distinct 
    user_id,
    case 
        when other_status is null then 'Fresh'
        else 'Deferred'
    end as lead_type,
    email

from

    (with raw_data as
    
        (select
            courses_course.id as course_id,
            courses_course.title as batch_name,
            courses_course.start_timestamp,
            courses_course.end_timestamp,
            user_id,
            courses_courseusermapping.status,
            email
        from
            courses_courseusermapping
        join courses_course
            on courses_course.id = courses_courseusermapping.course_id and courses_courseusermapping.status not in (13,14,18,27) and courses_course.course_type in (1,6)
        join auth_user
            on auth_user.id = courses_courseusermapping.user_id
        where date(courses_course.start_timestamp) < date(current_date)
        and courses_course.course_structure_id in (1,6,8,11,12,13,14,18,19,20,22,23,26)
        and unit_type like 'LEARNING'),
    
    non_studying as 
        (select 
            * 
        from
            raw_data
        where status in (11,30)),
        
    studying as 
        (select 
            *
        from
            raw_data
        where status in (5,8,9))
        
    select
        studying.*,
        non_studying.status as other_status,
        non_studying.course_id as other_course_id,
        non_studying.start_timestamp as other_st,
        non_studying.end_timestamp as other_et
    from
        studying
    left join non_studying
        on non_studying.user_id = studying.user_id
    group by 1,2,3,4,5,6,7,8,9,10,11) raw;
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