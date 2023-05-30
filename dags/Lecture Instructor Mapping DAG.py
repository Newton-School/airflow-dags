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
            'INSERT INTO lecture_instructor_mapping (lecture_id,'
            'inst_course_user_mapping_id,report_type,inst_min_join_time,'
            'inst_max_leave_time,duration_time_in_mins)'
            'VALUES (%s,%s,%s,%s,%s,%s)'
            'on conflict (lecture_id) do update set inst_course_user_mapping_id = EXCLUDED.inst_course_user_mapping_id,'
            'inst_min_join_time = EXCLUDED.inst_min_join_time,'
            'inst_max_leave_time = EXCLUDED.inst_max_leave_time,'
            'duration_time_in_mins = EXCLUDED.duration_time_in_mins;',
            (
                transform_row[0],
                transform_row[1],
                transform_row[2],
                transform_row[3],
                transform_row[4],
                transform_row[5]
            )
        )
    pg_conn.commit()


dag = DAG(
    'lecture_instructor_mapping_dag',
    default_args=default_args,
    description='Lecture and instructor mapping',
    schedule_interval='30 20 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS lecture_instructor_mapping (
            lecture_id bigint not null PRIMARY KEY,
            inst_course_user_mapping_id bigint,
            report_type int,
            inst_min_join_time timestamp,
            inst_max_leave_time timestamp,
            duration_time_in_mins int
    );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''with inst_time as

    (select
        lecture_id,
        course_user_mapping_id as inst_course_user_mapping_id,
        report_type,
        inst_min_join_time,
        inst_max_leave_time,
        (duration_time_in_secs/60) as duration_time_in_mins
    from
            (with raw_mapping as

                    (select 
                        trainers_courseinstructormapping.course_id,
                        trainers_instructor.user_id,
                        courses_courseusermapping.status,
                        courses_courseusermapping.id as cum_id
                    from
                        trainers_instructor
                    join trainers_courseinstructormapping
                        on trainers_courseinstructormapping.instructor_id = trainers_instructor.id
                    join courses_courseusermapping
                        on courses_courseusermapping.user_id = trainers_instructor.user_id and courses_courseusermapping.course_id = trainers_courseinstructormapping.course_id
                    left join auth_user
                        on auth_user.id = trainers_instructor.user_id
                    group by 1,2,3,4),


            lectures as 

            (select 
                video_sessions_lecture.id as lecture_id,
                date(video_sessions_lecture.start_timestamp) as lecture_date,
                video_sessions_lecturecourseuserreport.course_user_mapping_id,
                video_sessions_lecturecourseuserreport.report_type,
                min(video_sessions_lecturecourseuserreport.join_time) as inst_min_join_time,
                max(video_sessions_lecturecourseuserreport.leave_time) as inst_max_leave_time,
                sum(duration) filter (where report_type = 4) as duration_time_in_secs
            from
                video_sessions_lecture
            left join video_sessions_lecturecourseuserreport
                on video_sessions_lecturecourseuserreport.lecture_id = video_sessions_lecture.id
            join raw_mapping
                on raw_mapping.cum_id = video_sessions_lecturecourseuserreport.course_user_mapping_id and video_sessions_lecture.course_id = raw_mapping.course_id
            group by 1,2,3,4)


            select
                lectures.*,
                dense_rank() over (partition by lecture_id order by duration_time_in_secs desc,inst_min_join_time) as d_rank
            from
                lectures
            where duration_time_in_secs is not null
            order by 2) a
    where d_rank = 1
    order by 1)
    
select * from inst_time;
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