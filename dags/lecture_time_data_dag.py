from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import numpy as np

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 16),
}

# DATA CLEANING FUNCTION
def remove_redundant_rows(df):
    df = df.copy()
    df = df.reset_index()  # Create a copy of the DataFrame to avoid modifying the original data
    try:
        join_time = df.iloc[0]["join_time"]
    except:
        print(df)
    leave_time = df.iloc[0]["leave_time"]
    primary_index = 0

    rows_to_drop = []  # Maintain a list of rows to drop

    for i, row in df.iterrows():
        if join_time < row['join_time'] and row["join_time"] < leave_time:
            if row["leave_time"] > leave_time:
                leave_time = row["leave_time"]
                df.at[primary_index, "leave_time"] = leave_time
            rows_to_drop.append(i)  # Add the row index to the list of rows to drop
        elif leave_time < row["join_time"]:
            primary_index = i
            join_time = row["join_time"]
            leave_time = row["leave_time"]

    df.drop(rows_to_drop, inplace=True)  # Drop the redundant rows from the DataFrame

    return df


# OVERLAPPING TIME FUNCTION
def calculate_student_instructor_overlapping_time(student_join_time, student_leave_time, instructor_times):
    overlapping_time = 0
    for instructor_time in instructor_times:
        instructor_join_time = pd.Timestamp(instructor_time[0])
        instructor_leave_time = pd.Timestamp(instructor_time[1])

        # Check for overlap between student and instructor times
        if student_join_time <= instructor_leave_time and student_leave_time >= instructor_join_time:
            overlap_start = max(student_join_time, instructor_join_time)
            overlap_end = min(student_leave_time, instructor_leave_time)
            overlap_time = max(overlap_end - overlap_start, pd.Timedelta(0))  # Ensure positive time difference
            overlapping_time += (overlap_end - overlap_start).total_seconds()

    # print("Bhai Nan aaya hai", student_leave_time, student_join_time, instructor_times)
    return overlapping_time


def fetch_data_and_preprocess(**kwargs):
    # Fetch data from the query
    pg_hook = PostgresHook(postgres_conn_id='postgres_read_replica')
    pg_conn = pg_hook.get_conn()
    pg_cursor = pg_conn.cursor()

    result_hook = PostgresHook(postgres_conn_id='postgres_result_db')
    result_conn = result_hook.get_conn()
    result_cursor = result_conn.cursor()

    result_cursor.execute("""
    select distinct lecture_id from lecture_engagement_time;
        """)

    inserted_lecture_id = list(result_cursor.fetchall())
    new_inserted_lecture_id = []
    for insert_lecture in inserted_lecture_id:
        new_inserted_lecture_id.append(insert_lecture[0])
    # print(inserted_lecture_id)
    # print(new_inserted_lecture_id)
    # print(len(inserted_lecture_id))

    query = """
    select distinct lecture_id from
        (with vsl_cur_raw as
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
            and lecture_id > 30934
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
                and vsl_cur_raw.lecture_id not in %s 
        order by 1 desc, 5, 2) q;
    """

    #print(query)
    # print(inserted_lecture_id)
    # pg_cursor.execute(query)
    pg_cursor.execute(query, (tuple(new_inserted_lecture_id),))

    rows = pg_cursor.fetchall()
    print(rows)
    for row_data in rows:
        lecture_id = row_data[0]
        if lecture_id in new_inserted_lecture_id:
            print(lecture_id)
    # col_names = ['lecture_id']
    # column_names = ['lecture_id', 'course_user_mapping_id', 'join_time', 'leave_time', 'user_type']
    # df = pd.DataFrame(rows, columns=column_names)

    # MAIN CODE AND CALLING OF FUNCTIONS
    # new_df = pd.DataFrame()  # copy of the original df
    # for i, row in df.groupby('lecture_id'):
    #     instructor_dataframe = row[row['user_type'] == 'Instructor']
    #     instructor_dataframe = instructor_dataframe.sort_values('join_time')
    #     if instructor_dataframe.shape[0] == 0:
    #         continue
    #     instructor_dataframe = remove_redundant_rows(instructor_dataframe)
    #     instructor_times = np.array(instructor_dataframe[['join_time', 'leave_time']].values.tolist())
    #     # concat_instructor_dataframe
    #     new_df = pd.concat([new_df, instructor_dataframe])
    #     for j, row2 in row.groupby('course_user_mapping_id'):
    #         user_type = row2['user_type'].values[0]
    #         if user_type == 'User':
    #             row2 = row2.sort_values('join_time')
    #             student_dataframe = remove_redundant_rows(row2)
    #             # print(student_dataframe[['join_time', 'leave_time']])
    #             for k, row3 in student_dataframe.iterrows():
    #                 student_join_time = row3['join_time']
    #                 student_leave_time = row3['leave_time']
    #
    #                 # Get the index of the row in the original dataframe (df)
    #                 original_index = row3.name
    #
    #                 # Check if the row is redundant based on its index
    #                 is_redundant = original_index != k
    #
    #                 # Set user_type to None for redundant rows
    #                 user_type = row3['user_type'] if not is_redundant else None
    #
    #                 overlapping_time_seconds = calculate_student_instructor_overlapping_time(student_join_time,
    #                                                                                          student_leave_time,
    #                                                                                          instructor_times)
    #                 student_dataframe.at[original_index, 'overlapping_time_seconds'] = round(overlapping_time_seconds,
    #                                                                                          0)
    #                 student_dataframe.at[original_index, 'overlapping_time_minutes'] = round(
    #                     overlapping_time_seconds / 60, 2)
    #             new_df = pd.concat([new_df, student_dataframe])
    #
    # # result_df = result_df.drop(['index'], axis=1)
    # column_positioning = ['lecture_id', 'course_user_mapping_id', 'user_type', 'join_time', 'leave_time',
    #                       'overlapping_time_seconds', 'overlapping_time_minutes']
    # new_df = new_df.reindex(columns=column_positioning)
    # # result_df = result_df.reindex(columns=column_positioning)
    #
    # ti = kwargs['ti']
    # ti.xcom_push(key='preprocessed_data_df', value=new_df)

def insert_preprocessed_data(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres_result_db')
    pg_conn = pg_hook.get_conn()
    pg_cursor = pg_conn.cursor()

    ti = kwargs['ti']
    df_cleaned = ti.xcom_pull(task_ids='fetch_data_and_preprocess', key='preprocessed_data_df')

    for _, row in df_cleaned.iterrows():
        # print("Insert Pre Processed Data", row)
        pg_cursor.execute(
            'INSERT INTO lecture_engagement_time (lecture_id, course_user_mapping_id, user_type, join_time, leave_time, overlapping_time_seconds, overlapping_time_minutes) '
            'VALUES (%s, %s, %s, %s, %s, %s, %s);',
            (
                row['lecture_id'],
                row['course_user_mapping_id'],
                row['user_type'],
                row['join_time'],
                row['leave_time'],
                row['overlapping_time_seconds'],
                row['overlapping_time_minutes']
            )
        )

    pg_conn.commit()

dag = DAG(
    'lecture_time_dag',
    default_args=default_args,
    concurrency=4,
    max_active_tasks=6,
    max_active_runs=6,
    description='Live lecture ET data with overlapping time calculation and table creation/update',
    schedule_interval='0 1 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql="""
    CREATE TABLE IF NOT EXISTS lecture_engagement_time (
        id serial not null PRIMARY KEY,
        lecture_id bigint,
        course_user_mapping_id bigint,
        join_time timestamp,
        leave_time timestamp,
        user_type varchar(32),
        overlapping_time_seconds real,
        overlapping_time_minutes real
    )
    """,
    dag=dag
)

fetch_data = PythonOperator(
    task_id='fetch_data_and_preprocess',
    python_callable=fetch_data_and_preprocess,
    provide_context=True,
    dag=dag
)

insert_data = PythonOperator(
    task_id='insert_preprocessed_data',
    python_callable=insert_preprocessed_data,
    provide_context=True,
    dag=dag
)

create_table >> fetch_data
# >> insert_data