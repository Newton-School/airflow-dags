from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import numpy as np

default_args = {
    'owner': 'airflow',
    'max_active_tasks': 6,
    'max_active_runs': 6,
    'concurrency': 4,
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
def calculate_student_expert_overlapping_time(student_join_time, student_leave_time, expert_times):
    overlapping_time = 0
    for expert_time in expert_times:
        expert_join_time = pd.Timestamp(expert_time[0])
        expert_leave_time = pd.Timestamp(expert_time[1])

        # Check for overlap between student and expert times
        if student_join_time <= expert_leave_time and student_leave_time >= expert_join_time:
            overlap_start = max(student_join_time, expert_join_time)
            overlap_end = min(student_leave_time, expert_leave_time)
            overlapping_time += (overlap_end - overlap_start).total_seconds()

    return overlapping_time


def fetch_data_and_preprocess(**kwargs):
    # Fetch data from the query
    pg_hook = PostgresHook(postgres_conn_id='postgres_read_replica')
    pg_conn = pg_hook.get_conn()
    pg_cursor = pg_conn.cursor()

    # result_hook = PostgresHook(postgres_conn_id='postgres_result_db')
    # result_conn = result_hook.get_conn()
    # result_cursor = result_conn.cursor()
    #
    # result_cursor.execute("""
    # select distinct lecture_id from lecture_engagement_time;
    #     """)

    # inserted_lecture_id = list(result_cursor.fetchall())

    query = """
        with raw_data as 
            (select 
                one_to_one_id,
                course_user_mapping_id,
                join_time,
                leave_time,
                report_type,
                video_sessions_onetoone.one_to_one_type
            from
                video_sessions_onetoonecourseuserreport
            join video_sessions_onetoone
                on video_sessions_onetoone.id = video_sessions_onetoonecourseuserreport.one_to_one_id
            where report_type = 4
            group by 1,2,3,4,5,6),
        
        booked_with_mapping as
            (select 
                one_to_one_id,
                user_id,
                concat(first_name, ' ', last_name) as stakeholder_name,
                course_user_mapping_id,
                join_time,
                leave_time,
                report_type,
                raw_data.one_to_one_type,
                'Expert' as stakeholder_type
            from
                raw_data
            join video_sessions_onetoone
                on video_sessions_onetoone.id = raw_data.one_to_one_id
            join courses_courseusermapping
                on courses_courseusermapping.id = raw_data.course_user_mapping_id
                    and video_sessions_onetoone.booked_with_id = courses_courseusermapping.user_id
            join auth_user
                on auth_user.id = courses_courseusermapping.user_id),
                
        booked_by_mapping as 
            (select 
                one_to_one_id,
                user_id,
                concat(first_name, ' ', last_name) as stakeholder_name,
                course_user_mapping_id,
                join_time,
                leave_time,
                report_type,
                raw_data.one_to_one_type,
                'User' as stakeholder_type
            from
                raw_data
            join video_sessions_onetoone
                on video_sessions_onetoone.id = raw_data.one_to_one_id
            join courses_courseusermapping
                on courses_courseusermapping.id = raw_data.course_user_mapping_id
                    and video_sessions_onetoone.booked_by_id = courses_courseusermapping.user_id
            join auth_user
                on auth_user.id = courses_courseusermapping.user_id),
        
        final_data as 
        
            (select * from booked_with_mapping
            
            union
            
            select * from booked_by_mapping)
        
        select * from final_data
        where join_time >= '2023-07-25';
    """


    pg_cursor.execute(query)
    # , (inserted_lecture_id,))

    rows = pg_cursor.fetchall()
    column_names = ['one_to_one_id', 'user_id', 'stakeholder_name', 'course_user_mapping_id',
                    'join_time', 'leave_time', 'report_type', 'one_to_one_type', 'stakeholder_type']
    df = pd.DataFrame(rows, columns=column_names)

    # MAIN CODE AND CALLING OF FUNCTIONS
    new_df = pd.DataFrame()  # copy of the original df
    for i, row in df.groupby('one_to_one_id'):
        expert_dataframe = row[row['stakeholder_type'] == 'Expert']
        expert_dataframe = expert_dataframe.sort_values('join_time')
        if expert_dataframe.shape[0] == 0:
            continue
        expert_dataframe = remove_redundant_rows(expert_dataframe)
        expert_times = np.array(expert_dataframe[['join_time', 'leave_time']].values.tolist())
        # concat_instructor_dataframe
        new_df = pd.concat([new_df, expert_dataframe])
        for j, row2 in row.groupby('course_user_mapping_id'):
            stakeholder_type = row2['stakeholder_type'].values[0]
            if stakeholder_type == 'User':
                row2 = row2.sort_values('join_time')
                student_dataframe = remove_redundant_rows(row2)
                for k, row3 in student_dataframe.iterrows():
                    student_join_time = row3['join_time']
                    student_leave_time = row3['leave_time']

                    # Get the index of the row in the original dataframe (df)
                    original_index = row3.name

                    # Check if the row is redundant based on its index
                    is_redundant = original_index != k

                    # Set user_type to None for redundant rows
                    stakeholder_type = row3['stakeholder_type'] if not is_redundant else None

                    overlapping_time_seconds = calculate_student_expert_overlapping_time(student_join_time, student_leave_time, expert_times)
                    student_dataframe.at[original_index, 'overlapping_time_seconds'] = round(overlapping_time_seconds, 0)
                    student_dataframe.at[original_index, 'overlapping_time_minutes'] = round(overlapping_time_seconds/60, 2)
                new_df = pd.concat([new_df, student_dataframe])
    column_positioning = ['one_to_one_id', 'user_id', 'stakeholder_name', 'course_user_mapping_id', 'join_time',
                          'leave_time', 'report_type', 'one_to_one_type', 'stakeholder_type',
                          'overlapping_time_seconds', 'overlapping_time_minutes']
    new_df = new_df.reindex(columns=column_positioning)

    ti = kwargs['ti']
    ti.xcom_push(key='preprocessed_data_df', value=new_df)


def insert_preprocessed_data(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres_result_db')
    pg_conn = pg_hook.get_conn()
    pg_cursor = pg_conn.cursor()

    ti = kwargs['ti']
    df_cleaned = ti.xcom_pull(task_ids='fetch_data_and_preprocess', key='preprocessed_data_df')

    for _, row in df_cleaned.iterrows():
        print("Insert Pre Processed Data", row)
    pg_cursor.execute(
        'INSERT INTO video_sessions_course_user_reports (one_to_one_id, user_id, stakeholder_name,'
        'course_user_mapping_id, join_time, leave_time, report_type, one_to_one_type,'
        'stakeholder_type, overlapping_time_seconds, overlapping_time_minutes)'
        'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);',
        (
            row['one_to_one_id'],
            row['user_id'],
            row['stakeholder_name'],
            row['course_user_mapping_id'],
            row['join_time'],
            row['leave_time'],
            row['report_type'],
            row['one_to_one_type'],
            row['stakeholder_type'],
            row['overlapping_time_seconds'],
            row['overlapping_time_minutes']
        )
    )

    pg_conn.commit()


dag = DAG(
    'One_to_One_course_user_report_DAG',
    default_args=default_args,
    concurrency=4,
    max_active_tasks=6,
    max_active_runs=6,
    description='Video sessions one to one Engagement time data with overlapping time calculation',
    schedule_interval='30 0 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql="""
    CREATE TABLE IF NOT EXISTS video_sessions_course_user_reports (
        id serial not null PRIMARY KEY,
        one_to_one_id bigint,
        user_id bigint,
        stakeholder_name text,
        course_user_mapping_id bigint,
        join_time timestamp,
        leave_time timestamp,
        report_type int,
        one_to_one_type int,
        stakeholder_type text,
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

create_table >> fetch_data >> insert_data