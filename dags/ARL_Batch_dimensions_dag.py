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
            'INSERT INTO arl_course_dimensions (course_id,course_name,unit_type,'
            'course_structure_class,course_structure_name,course_start_date,course_age,'
            'admin_course_id,admin_unit_name,admin_course_structure_class,'
            'admin_course_structure_name,admin_course_start_date,admin_unit_age,'
            'current_batch_strength,initial_batch_strength)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (course_id) do update set course_name=EXCLUDED.course_name,'
            'unit_type=EXCLUDED.unit_type,course_structure_class=EXCLUDED.course_structure_class,'
            'course_structure_name=EXCLUDED.course_structure_name,course_start_date=EXCLUDED.course_start_date,'
            'course_age=EXCLUDED.course_age,admin_course_id=EXCLUDED.admin_course_id,'
            'admin_unit_name=EXCLUDED.admin_unit_name,'
            'admin_course_structure_class=EXCLUDED.admin_course_structure_class,'
            'admin_course_structure_name=EXCLUDED.admin_course_structure_name,'
            'admin_course_start_date=EXCLUDED.admin_course_start_date,'
            'admin_unit_age=EXCLUDED.admin_unit_age,current_batch_strength=EXCLUDED.current_batch_strength,'
            'initial_batch_strength=EXCLUDED.initial_batch_strength ;',
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
    'ARL_Course_dimensions',
    default_args=default_args,
    description='An Analytics Reporting Layer DAG for Course Dimensions',
    schedule_interval='40 0 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS arl_course_dimensions (
            course_id int not null PRIMARY KEY,
            course_name varchar(256),
            unit_type varchar(16),
            course_structure_class varchar(128),
            course_structure_name varchar(128),
            course_start_date DATE,
            course_age int,
            admin_course_id int,
            admin_unit_name varchar(256),
            admin_course_structure_class varchar(128),
            admin_course_structure_name varchar(128),
            admin_course_start_date DATE,
            admin_unit_age int,
            current_batch_strength int,
            initial_batch_strength int
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_result_db',
    sql='''with weekly_batch_strength as(
            select
            course_id,
            week_view,
            dense_rank() over (partition by course_id order by week_view)as rn,
            count(distinct user_id) filter (where status in (5,8,9) and label_mapping_id is null) as initial_batch_strength
            from weekly_user_details
            group by 1,2
            ),
            current_batch_strength as (
            select
            course_user_mapping.course_id,
            count(distinct course_user_mapping.user_id) filter (where (courses.course_structure_id in (6,8,11,12,13,19,14,20,22,23,26) and course_user_mapping.status in (8)) or (courses.course_structure_id in (1,18) and course_user_mapping.status in (5,9))) as cbs
            from course_user_mapping
            left join courses on courses.course_id = course_user_mapping.course_id
            group by 1
            ),
            raw as(
            select
            distinct course_user_mapping.course_id,
            course_user_mapping.course_name,
            courses.unit_type,
            courses.course_structure_class,
            courses.course_structure_name,
            date(courses.course_start_timestamp) as course_start_date,
            round(extract('day' from (date_trunc('month',now()) - date_trunc('month',courses.course_start_timestamp)))/30) as course_age,
            course_user_mapping.admin_course_id,
            course_user_mapping.admin_unit_name,
            cc.course_structure_class as admin_course_structure_class,
            cc.course_structure_name as admin_course_structure_name,
            date(cc.course_start_timestamp) as admin_course_start_date,
            round(extract('day' from (date_trunc('month',now()) - date_trunc('month',cc.course_start_timestamp)))/30) as au_age,
            dense_rank() over (partition by course_user_mapping.course_id order by course_user_mapping.admin_course_id) as rn
            from course_user_mapping
            left join courses on courses.course_id = course_user_mapping.course_id
            left join courses as cc on cc.course_id = course_user_mapping.admin_course_id
            order by course_user_mapping.course_id,course_user_mapping.admin_course_id
            )
            select
            distinct raw.course_id,
            course_name,
            unit_type,
            course_structure_class,
            course_structure_name,
            course_start_date,
            course_age,
            admin_course_id,
            admin_unit_name,
            admin_course_structure_class,
            admin_course_structure_name,
            admin_course_start_date,
            au_age as admin_unit_age,
            current_batch_strength.cbs as current_batch_strength,
            weekly_batch_strength.initial_batch_strength
            from raw
            left join current_batch_strength on current_batch_strength.course_id = raw.course_id
            left join weekly_batch_strength on weekly_batch_strength.course_id = raw.course_id and weekly_batch_strength.rn = 1
            where raw.rn = 1;
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