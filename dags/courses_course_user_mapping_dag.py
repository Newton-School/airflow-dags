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
                'INSERT INTO course_user_mapping (course_user_mapping_id,user_id,course_id,course_name,unit_type,admin_course_user_mapping_id,admin_unit_name,admin_course_id,created_at,status,label_id,utm_campaign,utm_source,utm_medium,hash) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                'on conflict (course_user_mapping_id) do update set status = EXCLUDED.status,label_id = EXCLUDED.label_id,admin_course_user_mapping_id = EXCLUDED.admin_course_user_mapping_id;',
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
                 )
        )
    pg_conn.commit()


dag = DAG(
    'courses_cum_dag',
    default_args=default_args,
    description='Course user mapping detailed version',
    schedule_interval='0 19 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS course_user_mapping (
            course_user_mapping_id bigint not null PRIMARY KEY,
            user_id bigint,
            course_id bigint,
            course_name varchar(128),
            unit_type varchar(16),
            admin_course_user_mapping_id bigint,
            admin_unit_name varchar(128),
            admin_course_id bigint,
            created_at timestamp,
            status int,
            label_id bigint,
            utm_campaign varchar(256),
            utm_source varchar(256),
            utm_medium varchar(256),
            hash varchar(100)
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''with raw as 

        (select 
            courses_courseusermapping.id as course_user_mapping_id,
            courses_courseusermapping.user_id,
            courses_course.id as course_id,
            courses_course.title as course_name,
            courses_course.unit_type,
            courses_courseusermapping.admin_course_user_mapping_id,
            cast(courses_courseusermapping.created_at as varchar) as created_at,
            courses_courseusermapping.status, 
            courses_courseuserlabelmapping.id as label_id,
            (courses_courseusermapping.utm_param_json->'utm_source'::text) #>> '{}' as utm_source,
            (courses_courseusermapping.utm_param_json->'utm_medium'::text) #>> '{}' as utm_medium,
            (courses_courseusermapping.utm_param_json->'utm_campaign'::text) #>> '{}' as utm_campaign,
            courses_courseusermapping.hash
        from
            courses_courseusermapping
        left join courses_courseuserlabelmapping
            on courses_courseuserlabelmapping.course_user_mapping_id = courses_courseusermapping.id and courses_courseuserlabelmapping.label_id = 677
        left join courses_course 
            on courses_course.id = courses_courseusermapping.course_id)
        
select
    raw.course_user_mapping_id, 
    raw.user_id,
    raw.course_id,
    raw.course_name,
    raw.unit_type,
    raw.admin_course_user_mapping_id,
    r_one.course_name as admin_unit_name,
    r_one.course_id as admin_course_id,
    raw.created_at,
    raw.status, 
    raw.label_id,
    raw.utm_campaign,
    raw.utm_source,
    raw.utm_medium,
    raw.hash 
from
    raw
left join raw as r_one
    on raw.admin_course_user_mapping_id = r_one.course_user_mapping_id
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15;
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