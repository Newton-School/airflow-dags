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
            'INSERT INTO live_lectures_all_details (table_unique_key,'
            'lecture_id,'
            'lecture_title,'
            'lecture_date,'
            'mandatory,'
            'course_id,'
            'course_name,'
            'course_structure_class,'
            'instructor_user_id,'
            'instructor_name,'
            'inst_min_join_time,'
            'inst_max_leave_time,'
            'inst_total_time_in_mins,'
            'user_id,'
            'student_name,'
            'user_enrollment_status,'
            'user_min_join_time,'
            'user_max_leave_time,'
            'user_total_time_in_mins,'
            'user_total_overlap_time_in_mins)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (table_unique_key) do update set lecture_id = EXCLUDED.lecture_id,'
            'lecture_title = EXCLUDED.lecture_title,'
            'lecture_date = EXCLUDED.lecture_date,'
            'mandatory = EXCLUDED.mandatory,'
            'course_id = EXCLUDED.course_id,'
            'course_name = EXCLUDED.course_name,'
            'course_structure_class = EXCLUDED.course_structure_class,'
            'instructor_user_id = EXCLUDED.instructor_user_id,'
            'instructor_name = EXCLUDED.instructor_name,'
            'inst_min_join_time = EXCLUDED.inst_min_join_time,'
            'inst_max_leave_time = EXCLUDED.inst_max_leave_time,'
            'inst_total_time_in_mins = EXCLUDED.inst_total_time_in_mins,'
            'user_id = EXCLUDED.user_id,'
            'student_name = EXCLUDED.student_name,'
            'user_enrollment_status = EXCLUDED.user_enrollment_status,'
            'user_min_join_time = EXCLUDED.user_min_join_time,'
            'user_max_leave_time = EXCLUDED.user_max_leave_time,'
            'user_total_time_in_mins = EXCLUDED.user_total_time_in_mins,'
            'user_total_overlap_time_in_mins = EXCLUDED.user_total_overlap_time_in_mins;',
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


dag = DAG(
    'Live_lectures_all_details_dag',
    default_args=default_args,
    concurrency=4,
    max_active_tasks=6,
    max_active_runs=6,
    description='An extended version of live lecture course user report table',
    schedule_interval='50 0 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS live_lectures_all_details (
            id serial,
            table_unique_key varchar(255) NOT NULL PRIMARY KEY,
            lecture_id bigint,
            lecture_title varchar(255),
            lecture_date DATE,
            mandatory boolean,
            course_id int,
            course_name varchar(255),
            course_structure_class varchar(255),
            instructor_user_id bigint,
            instructor_name varchar(255),
            inst_min_join_time timestamp,
            inst_max_leave_time timestamp,
            inst_total_time_in_mins int,
            user_id bigint,
            student_name varchar(255),
            user_enrollment_status varchar(255),
            user_min_join_time timestamp,
            user_max_leave_time timestamp,
            user_total_time_in_mins int,
            user_total_overlap_time_in_mins int
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_result_db',
    sql='''with raw_data as
            (select 
                lecture_id,
                course_user_mapping_id,
                join_time,
                leave_time,
                user_type,
                overlapping_time_seconds,
                overlapping_time_minutes
            from
                lecture_engagement_time let
            group by 1,2,3,4,5,6,7),
        user_raw_data as 
            (select 
                lecture_id,
                raw_data.course_user_mapping_id,
                case 
                    when cum.label_id is null and cum.status in (5,8,9) then 'Enrolled Student'
                    when cum.label_id is not null and cum.status in (5,8,9) then 'Label Mapped User'
                    when cum.status not in (5,8,9) then 'Other'
                end as user_enrollment_status,
                user_type,
                join_time,
                leave_time,
                extract('epoch' from (leave_time - join_time)) as time_diff,
                overlapping_time_seconds
            from
                raw_data
            join course_user_mapping cum
                on cum.course_user_mapping_id = raw_data.course_user_mapping_id and lower(user_type) like 'user'),
        inst_raw_data as 
            (select 
                lecture_id,
                raw_data.course_user_mapping_id,
                user_type,
                join_time,
                leave_time,
                extract('epoch' from (leave_time - join_time)) as time_diff
            from
                raw_data
            where lower(user_type) like 'instructor'),
        
        inst_data as 
            (select 
                 lecture_id,
                 course_user_mapping_id,
                 user_type,
                 min(join_time) as min_join_time,
                 max(leave_time) as max_leave_time,
                 cast(sum(time_diff) as int) as total_time_in_seconds,
                 cast(sum(time_diff) / 60 as int) as total_time_in_mins
            from 
                inst_raw_data
            group by 1,2,3),
        user_data as 
            (select 
                 user_raw_data.lecture_id,
                 cum2.user_id as instructor_user_id,
                 concat(ui2.first_name,' ',ui2.last_name) as instructor_name,
                 inst_data.min_join_time as inst_min_join_time,
                 inst_data.max_leave_time as inst_max_leave_time,
                 inst_data.total_time_in_mins as inst_total_time_in_mins,
                 cum.user_id,
                 concat(ui.first_name,' ',ui.last_name) as student_name,
                 user_raw_data.user_enrollment_status,
                 min(user_raw_data.join_time) as user_min_join_time,
                 max(user_raw_data.leave_time) as user_max_leave_time,
                 cast(sum(user_raw_data.time_diff) / 60 as int) as user_total_time_in_mins,
                 cast(sum(user_raw_data.overlapping_time_seconds) / 60 as int) as user_total_overlap_time_in_mins
            from 
                user_raw_data
            left join inst_data
                on inst_data.lecture_id = user_raw_data.lecture_id
            left join course_user_mapping cum 
                on cum.course_user_mapping_id = user_raw_data.course_user_mapping_id
            left join course_user_mapping cum2 
                on cum2.course_user_mapping_id = inst_data.course_user_mapping_id
            left join users_info ui 
                on ui.user_id = cum.user_id 
            left join users_info ui2 
                on ui2.user_id = cum2.user_id
            group by 1,2,3,4,5,6,7,8,9)
        select 
            concat(l.lecture_id,c.course_id,user_data.user_id,user_data.instructor_user_id) as table_unique_key,
            l.lecture_id,
            l.lecture_title,
            date(l.start_timestamp) as lecture_date,
            l.mandatory,
            c.course_id,
            c.course_name,
            c.course_structure_class,
            user_data.instructor_user_id,
            user_data.instructor_name,
            user_data.inst_min_join_time,
            user_data.inst_max_leave_time,
            user_data.inst_total_time_in_mins,
            user_data.user_id,
            user_data.student_name,
            user_data.user_enrollment_status,
            user_data.user_min_join_time,
            user_data.user_max_leave_time,
            user_data.user_total_time_in_mins,
            user_data.user_total_overlap_time_in_mins
        from
            lectures l
        join courses c
            on c.course_id = l.course_id 
        left join user_data
            on user_data.lecture_id = l.lecture_id
        where l.start_timestamp >= '2022-06-01';
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