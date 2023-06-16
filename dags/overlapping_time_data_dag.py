from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 16),
}


def extract_data_to_nested(**kwargs):
    # Fetch data from the query
    pg_hook = PostgresHook(postgres_conn_id='postgres_read_replica')
    pg_conn = pg_hook.get_conn()
    pg_cursor = pg_conn.cursor()

    pg_cursor.execute("""
        with vsl_cur_raw as
    (select
        lecture_id,
        course_user_mapping_id,
        join_time,
        leave_time,
        duration,
        report_type
    from
        video_sessions_lecturecourseuserreport
    where report_type = 4
    group by 1,2,3,4,5,6),

course_inst_mapping_raw as
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
    
inst_lecture_details as
    (select 
        video_sessions_lecture.id as lecture_id,
        date(video_sessions_lecture.start_timestamp) as lecture_date,
        course_inst_mapping_raw.user_id as inst_user_id,
        vsl_cur_raw.course_user_mapping_id,
        vsl_cur_raw.report_type,
        min(vsl_cur_raw.join_time) as inst_min_join_time,
        max(vsl_cur_raw.leave_time) as inst_max_leave_time,
        sum(duration) filter (where report_type = 4) as duration_in_secs,
        cast(extract (epoch from (max(vsl_cur_raw.leave_time) - min(vsl_cur_raw.join_time))) as int) as time_diff_in_secs
    from
        video_sessions_lecture
    join vsl_cur_raw
        on vsl_cur_raw.lecture_id = video_sessions_lecture.id
    join course_inst_mapping_raw
        on course_inst_mapping_raw.cum_id = vsl_cur_raw.course_user_mapping_id and video_sessions_lecture.course_id = course_inst_mapping_raw.course_id
    group by 1,2,3,4,5),

inst_labeling as 
    (select
        *,
        dense_rank()over (partition by lecture_id order by least(duration_in_secs, time_diff_in_secs) desc, inst_min_join_time) as d_rank
    from
        inst_lecture_details),

lecture_inst_mapping as
    (select 
        lecture_id,
        lecture_date,
        inst_user_id,
        course_user_mapping_id as inst_cum_id,
        report_type,
        inst_min_join_time,
        inst_max_leave_time,
        least(duration_in_secs, time_diff_in_secs)/60 as inst_total_time_in_mins
    from
        inst_labeling
    where d_rank = 1),

inst_details as 
    (select distinct 
        row_number()over (order by lecture_id) as key_value,
        lecture_id,
        inst_user_id,
        inst_cum_id
    from 
        lecture_inst_mapping)
    
select
    
    cast(concat(vsl_cur_raw.lecture_id, extract('minute' from vsl_cur_raw.join_time), vsl_cur_raw.course_user_mapping_id, extract('second' from vsl_cur_raw.join_time)) as double precision) as table_unique_key,
    vsl_cur_raw.lecture_id,
    vsl_cur_raw.course_user_mapping_id,
    vsl_cur_raw.join_time,
    vsl_cur_raw.leave_time,
    case 
        when inst_details.key_value is null then 'User'
        else 'Instructor'
    end as user_type
    
from
    vsl_cur_raw
left join inst_details
    on inst_details.lecture_id = vsl_cur_raw.lecture_id and inst_details.inst_cum_id = vsl_cur_raw.course_user_mapping_id
order by 2 desc, 6, 4;
    """)

    rows = pg_cursor.fetchall()

    column_names = ['lecture_id', 'course_user_mapping_id', 'join_time', 'leave_time', 'user_type']
    df = pd.DataFrame(rows, columns=column_names)

    # datatype conversion
    df['join_time'] = pd.to_datetime(df['join_time'])
    df['leave_time'] = pd.to_datetime(df['leave_time'])

    # Sorting the dataframe
    df.sort_values(['lecture_id', 'course_user_mapping_id', 'join_time'], inplace=True)

    df['overlapping_time_seconds'] = 0
    df['overlapping_time_minutes'] = 0
    for i, row in df.iterrows():
        if row['user_type'] == 'User':
            user_mapping_id = row['course_user_mapping_id']
            user_start_time = row['join_time']
            user_end_time = row['leave_time']
            overlap_seconds = 0

            instructors = df[(df['user_type'] == 'Instructor') & (df['lecture_id'] == row['lecture_id'])]

            for _, instructor_row in instructors.iterrows():
                instructor_start_time = instructor_row['join_time']
                instructor_end_time = instructor_row['leave_time']

                overlap_start_time = max(user_start_time, instructor_start_time)
                overlap_end_time = min(user_end_time, instructor_end_time)

                if overlap_start_time < overlap_end_time:
                    overlap_seconds += (overlap_end_time - overlap_start_time).total_seconds()

            df.loc[i, 'overlapping_time_seconds'] = overlap_seconds
            df.loc[i, 'overlapping_time_minutes'] = overlap_seconds / 60
    ti = kwargs['ti']
    ti.xcom_push(key='overlapping_time_df', value=df)


def insert_overlapping_time(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres_result_db')
    pg_conn = pg_hook.get_conn()
    pg_cursor = pg_conn.cursor()

    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='extract_data_to_nested', key='overlapping_time_df')

    for _, row in df.iterrows():
        pg_cursor.execute(
            'INSERT INTO live_lectures_engagement_time (lecture_id, course_user_mapping_id, join_time, leave_time, user_type, overlapping_time_seconds, overlapping_time_minutes) '
            'VALUES (%s, %s, %s, %s, %s, %s, %s)',
            (
                row['lecture_id'],
                row['course_user_mapping_id'],
                row['join_time'],
                row['leave_time'],
                row['user_type'],
                row['overlapping_time_seconds'],
                row['overlapping_time_minutes']
            )
        )

    conn.commit()


dag = DAG(
    'et_data_and_insert_overlapping_time',
    default_args=default_args,
    description='Live lecture ET data with overlapping time calculation and table creation/update',
    schedule_interval='30 20 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql="""
    CREATE TABLE IF NOT EXISTS live_lectures_engagement_time (
        lecture_id bigint,
        course_user_mapping_id bigint,
        join_time timestamp,
        leave_time timestamp,
        user_type varchar(32),
        overlapping_time_seconds int,
        overlapping_time_minutes real
    )
    """,
    dag=dag
)

extract_data = PythonOperator(
    task_id='extract_data_to_nested',
    python_callable=extract_data_to_nested,
    provide_context=True,
    dag=dag
)

insert_data = PythonOperator(
    task_id='insert_overlapping_time',
    python_callable=insert_overlapping_time,
    provide_context=True,
    dag=dag
)

create_table >> extract_data >> insert_data