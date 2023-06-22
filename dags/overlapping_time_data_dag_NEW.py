import numpy as np
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
def remove_redundant_rows(df):
    # Group the data at the lecture_id level
    grouped_lecture_df = df.groupby('lecture_id')

    # Initialize cleaned dataframe
    df_cleaned = pd.DataFrame()

    # Process data at the lecture_id level
    for lecture_id, lecture_group in grouped_lecture_df:
        lecture_group = lecture_group.sort_values('join_time')

        join_time = lecture_group.iloc[0]['join_time']
        leave_time = lecture_group.iloc[0]['leave_time']
        primary_index = 0

        rows_to_drop = []  # Maintain a list of rows to drop

        for i, row in lecture_group.iterrows():
            if join_time < row['join_time'] and row["join_time"] < leave_time:
                if row["leave_time"] > leave_time:
                    leave_time = row["leave_time"]
                    lecture_group.at[primary_index, "leave_time"] = leave_time
                rows_to_drop.append(i)  # Add the row index to the list of rows to drop
            elif leave_time < row["join_time"]:
                primary_index = i
                join_time = row["join_time"]
                leave_time = row["leave_time"]

        lecture_group.drop(rows_to_drop, inplace=True)  # Drop the redundant rows from the DataFrame
        df_cleaned = pd.concat([df_cleaned, lecture_group])

    return df_cleaned


def calculate_overlapping_time(df_user_cleaned, df_inst_cleaned):
    # Overlapping time calculation logic
    for _, row in df_user_cleaned.iterrows():
      user_start_time = row['join_time']
      user_end_time = row['leave_time']
      overlap_seconds = 0
      for _, instructor_row in df_inst_cleaned.iterrows():
          instructor_start_time = instructor_row['join_time']
          instructor_end_time = instructor_row['leave_time']

          overlap_start_time = max(user_start_time, instructor_start_time)
          overlap_end_time = min(user_end_time, instructor_end_time)

          if overlap_start_time < overlap_end_time:
              overlap_seconds += (overlap_end_time - overlap_start_time).total_seconds()

      return overlap_seconds
    else:
      return 0

# def calculate_overlapping_time(row, df_cleaned):
#     # Overlapping time calculation logic
#     if row['user_type'] == 'User':
#         user_start_time = row['join_time']
#         user_end_time = row['leave_time']
#         overlap_seconds = 0
#
#         instructors = df_cleaned[
#             (df_cleaned['user_type'] == 'Instructor') & (df_cleaned['lecture_id'] == row['lecture_id'])]
#
#         for _, instructor_row in instructors.iterrows():
#             instructor_start_time = instructor_row['join_time']
#             instructor_end_time = instructor_row['leave_time']
#
#             overlap_start_time = max(user_start_time, instructor_start_time)
#             overlap_end_time = min(user_end_time, instructor_end_time)
#
#             if overlap_start_time < overlap_end_time:
#                 overlap_seconds += (overlap_end_time - overlap_start_time).total_seconds()
#
#         return overlap_seconds
#     else:
#         return 0
def preprocess_and_analyze_data(df):
    # Preprocessing and analysis logic
    # Group the data at the lecture_id level
    grouped_lecture_df = df.groupby('lecture_id')

    # Initialize instructor and user dataframes
    df_inst_cleaned = pd.DataFrame()
    df_user_cleaned = pd.DataFrame()

    # Process data at the lecture_id level
    for lecture_id, lecture_group in grouped_lecture_df:
        lecture_group = lecture_group.sort_values('join_time')

        # Split data into instructor and user dataframes
        instructors = lecture_group[lecture_group['user_type'] == 'Instructor']
        users = lecture_group[lecture_group['user_type'] == 'User']

        # Remove redundant rows for instructors
        instructors_cleaned = remove_redundant_rows(instructors)
        df_inst_cleaned = pd.concat([df_inst_cleaned, instructors_cleaned])

        # Group user data at the course_user_mapping_id level
        grouped_user_df = users.groupby('course_user_mapping_id')

        # Process data at the course_user_mapping_id level
        for course_user_mapping_id, user_group in grouped_user_df:
            # user_group = user_group.sort_values('join_time')
            temp_user_cleaned = remove_redundant_rows(user_group)
            df_user_cleaned = pd.concat([df_user_cleaned, temp_user_cleaned])

        # Concatenate cleaned instructor data
        df_inst_cleaned = pd.concat([df_inst_cleaned, instructors])

    return df_inst_cleaned, df_user_cleaned

    # Concatenate cleaned data for instructors and users
    # df_cleaned = pd.concat([df_inst_cleaned, df_user_cleaned])

# def preprocess_and_analyze_data(df):
#     # Preprocessing and analysis logic
#     # Preprocessing Step: Remove redundant rows for instructors and users based on lecture_id and course_user_mapping_id
#     df_inst = df[df["user_type"] == "Instructor"].sort_values("join_time")
#     df_user = df[df["user_type"] == "User"].sort_values("join_time")
#
#     # Remove redundant rows for instructors
#     df_inst_cleaned = pd.DataFrame()
#     lecture_ids = df_inst["lecture_id"].unique()
#     for lecture_id in lecture_ids:
#         temp_inst = df_inst[df_inst["lecture_id"] == lecture_id].sort_values("join_time")
#         temp_inst_cleaned = remove_redundant_rows(temp_inst)
#         df_inst_cleaned = pd.concat([df_inst_cleaned, temp_inst_cleaned])
#
#     # Remove redundant rows for users
#     df_user_cleaned = pd.DataFrame()
#     course_user_mapping_ids = df_user["course_user_mapping_id"].unique()
#     lecture_ids = df_user["lecture_id"].unique()
#     for course_user_mapping_id in course_user_mapping_ids:
#         for lecture_id in lecture_ids:
#             temp_user = df_user[(df_user["course_user_mapping_id"] == course_user_mapping_id) & (df_user["lecture_id"] == lecture_id)].sort_values("join_time")
#             temp_user_cleaned = remove_redundant_rows(temp_user)
#             df_user_cleaned = pd.concat([df_user_cleaned, temp_user_cleaned])
#
#     # Concatenate cleaned data for instructors and users
#     df_cleaned = pd.concat([df_inst_cleaned, df_user_cleaned])

    # Analysis Step: Calculate overlapping time intervals between instructors and users for the same lecture_id

    # df_cleaned['overlapping_time_seconds'] = df_cleaned.apply(calculate_overlapping_time, args=(df_cleaned,), axis=1)
    # df_cleaned['overlapping_time_minutes'] = df_cleaned['overlapping_time_seconds'] / 60
    # return df_cleaned

def calculate_student_instructor_overlapping_time(student_join_time, student_leave_time, instructor_times):
    total_time = 0
    for time_range in instructor_times:
        inst_join_time, inst_leave_time = time_range
        # logic for calculating time based on 4 values
        total_time += max(min(student_leave_time, inst_leave_time) - max (student_join_time,inst_join_time),0)
    return total_time


def fetch_data_and_preprocess(**kwargs):
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
order by 1 desc, 5, 2;
    """)

    rows = pg_cursor.fetchall()

    column_names = ['lecture_id', 'course_user_mapping_id', 'join_time', 'leave_time', 'user_type']
    df = pd.DataFrame(rows, columns=column_names)

    # Call preprocess_and_analyze_data function
    # df_cleaned = preprocess_and_analyze_data(df)

    for i, row in df.groupby('lecture_id'):
        instructor_dataframe = row[row['user_type'] == 'Instructor']
        instructor_dataframe = instructor_dataframe.sort_values('join_time')
        instructor_dataframe = remove_redundant_rows(instructor_dataframe)
        instructor_times = np.array(instructor_dataframe[['join_time', 'leave_time']].values.tolist())
        for j, row2 in row.groupby('course_user_mapping_id'):
            user_type = row2['user_type'].values[0]
            if user_type == 'User':
                row2 = row2.sort_values('join_time')
                student_dataframe = remove_redundant_rows(row2)
                for k, row3 in student_dataframe.iterrows():
                    student_join_time = row3['join_time']
                    student_leave_time = row3['leave_time']
                    overlapping_time_seconds = calculate_student_instructor_overlapping_time(student_join_time, student_leave_time, instructor_times)
                    df.at[k, 'overlapping_time_seconds'] = overlapping_time_seconds
                    df.at[k, 'overlapping_time_minutes'] = overlapping_time_seconds / 60


    ti = kwargs['ti']
    ti.xcom_push(key='preprocessed_data_df', value=df)

def insert_preprocessed_data(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres_result_db')
    pg_conn = pg_hook.get_conn()
    pg_cursor = pg_conn.cursor()

    ti = kwargs['ti']
    df_cleaned = ti.xcom_pull(task_ids='fetch_data_and_preprocess', key='preprocessed_data_df')

    for _, row in df_cleaned.iterrows():
        pg_cursor.execute(
            'INSERT INTO live_lectures_engagement_time (lecture_id, course_user_mapping_id, join_time, leave_time, user_type, overlapping_time_seconds, overlapping_time_minutes) '
            'VALUES (%s, %s, %s, %s, %s, %s, %s);',
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

    pg_conn.commit()


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
        id serial not null PRIMARY KEY,
        lecture_id bigint,
        course_user_mapping_id bigint,
        join_time timestamp,
        leave_time timestamp,
        user_type varchar(32),
        overlapping_time_seconds bigint,
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

create_table >> fetch_data >> insert_data