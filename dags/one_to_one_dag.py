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
                'INSERT INTO one_to_one (one_to_one_id,student_user_id,course_id,expert_user_id,one_to_one_start_timestamp,one_to_one_end_timestamp,hash,one_to_one_created_at,one_to_one_confirmed_at,one_to_one_cancel_timestamp,one_to_one_status,one_to_one_type,final_call,cancel_reason,rating,reports_pulled,title,video_session_using,one_to_one_token_id,expert_min_join_time,expert_max_leave_time,student_min_join_time,student_max_leave_time,student_duration,expert_duration,overlapping_time,difficulty_level) '
                'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                'on conflict (one_to_one_id) do update set expert_user_id=EXCLUDED.expert_user_id,'
                'one_to_one_start_timestamp = EXCLUDED.one_to_one_start_timestamp,one_to_one_end_timestamp=EXCLUDED.one_to_one_end_timestamp,'
                'one_to_one_confirmed_at= EXCLUDED.one_to_one_confirmed_at,one_to_one_cancel_timestamp=EXCLUDED.one_to_one_cancel_timestamp,'
                'one_to_one_status=EXCLUDED.one_to_one_status,one_to_one_type=EXCLUDED.one_to_one_type,final_call=EXCLUDED.final_call,'
                'cancel_reason=EXCLUDED.cancel_reason,rating=EXCLUDED.rating,reports_pulled=EXCLUDED.reports_pulled,'
                'expert_min_join_time=EXCLUDED.expert_min_join_time,expert_max_leave_time=EXCLUDED.expert_max_leave_time,'
                'student_min_join_time=EXCLUDED.student_min_join_time,student_max_leave_time=EXCLUDED.student_max_leave_time,'
                'student_duration=EXCLUDED.student_duration,expert_duration=EXCLUDED.expert_duration,overlapping_time=EXCLUDED.overlapping_time,'
                'difficulty_level=EXCLUDED.difficulty_level ;',
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
                    transform_row[20],
                    transform_row[21],
                    transform_row[22],
                    transform_row[23],
                    transform_row[24],
                    transform_row[25],
                    transform_row[26],
                 )
        )
    pg_conn.commit()


dag = DAG(
    'One_to_one_dag',
    default_args=default_args,
    description='One to One Table DAG',
    schedule_interval='35 20 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS one_to_one (
            one_to_one_id bigint not null PRIMARY KEY,
            student_user_id bigint,
            course_id bigint,
            expert_user_id bigint,
            one_to_one_start_timestamp timestamp,
            one_to_one_end_timestamp timestamp,
            hash varchar(100),
            one_to_one_created_at timestamp,
            one_to_one_confirmed_at timestamp,
            one_to_one_cancel_timestamp timestamp,
            one_to_one_status int,
            one_to_one_type int,
            final_call int,
            cancel_reason varchar(2048),
            rating int,
            reports_pulled boolean,
            title varchar(256),
            video_session_using int,
            one_to_one_token_id bigint,
            expert_min_join_time timestamp,
            expert_max_leave_time timestamp,
            student_min_join_time timestamp,
            student_max_leave_time timestamp,
            student_duration bigint,
            expert_duration bigint,
            overlapping_time bigint,
            difficulty_level int
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_read_replica',
    sql='''with expert_report as(
            select
            video_sessions_onetoonecourseuserreport.one_to_one_id,
            min(join_time) as expert_min_join_time,
            max(leave_time) as expert_max_leave_time,
            sum(video_sessions_onetoonecourseuserreport.duration) filter (where video_sessions_onetoonecourseuserreport.report_type =4) as expert_duration
            from video_sessions_onetoone
            left join courses_courseusermapping
                on courses_courseusermapping.user_id = video_sessions_onetoone.booked_with_id and video_sessions_onetoone.course_id = courses_courseusermapping.course_id
            left join video_sessions_onetoonecourseuserreport
                on video_sessions_onetoonecourseuserreport.one_to_one_id = video_sessions_onetoone.id and video_sessions_onetoonecourseuserreport.course_user_mapping_id = courses_courseusermapping.id
            group by 1
            ),
            student_report as(
            select
            video_sessions_onetoonecourseuserreport.one_to_one_id,
            min(join_time) as student_min_join_time,
            max(leave_time) as student_max_leave_time,
            sum(video_sessions_onetoonecourseuserreport.duration) filter (where video_sessions_onetoonecourseuserreport.report_type =4) as student_duration,
            sum(video_sessions_onetoonecourseuserreport.duration) filter (where video_sessions_onetoonecourseuserreport.report_type =4 and video_sessions_onetoonecourseuserreport.join_time >= expert_min_join_time and video_sessions_onetoonecourseuserreport.leave_time <= expert_max_leave_time ) as overlapping_time
            from video_sessions_onetoone
            left join courses_courseusermapping
                on courses_courseusermapping.user_id = video_sessions_onetoone.booked_by_id and video_sessions_onetoone.course_id = courses_courseusermapping.course_id
            left join video_sessions_onetoonecourseuserreport
                on video_sessions_onetoonecourseuserreport.one_to_one_id = video_sessions_onetoone.id and video_sessions_onetoonecourseuserreport.course_user_mapping_id = courses_courseusermapping.id
            left join expert_report on expert_report.one_to_one_id = video_sessions_onetoone.id
            group by 1
            )
            select
            distinct video_sessions_onetoone.id as one_to_one_id,
            video_sessions_onetoone.booked_by_id as student_user_id,
            video_sessions_onetoone.course_id,
            video_sessions_onetoone.booked_with_id as expert_user_id,
            cast(video_sessions_onetoone.start_timestamp as varchar) as one_to_one_start_timestamp,
            cast(video_sessions_onetoone.end_timestamp as varchar) as one_to_one_end_timestamp,
            video_sessions_onetoone.hash,
            cast(video_sessions_onetoone.created_at as varchar) as one_to_one_created_at,
            cast(video_sessions_onetoone.confirmed_at as varchar) as one_to_one_confirmed_at,
            cast(video_sessions_onetoone.cancel_timestamp as varchar) as one_to_one_cancel_timestamp,
            video_sessions_onetoone.one_to_one_status,
            video_sessions_onetoone.one_to_one_type,
            video_sessions_onetoone.final_call,
            video_sessions_onetoone.cancel_reason,
            video_sessions_onetoone.rating,
            video_sessions_onetoone.reports_pulled,
            video_sessions_onetoone.title,
            video_sessions_onetoone.video_session_using,
            video_sessions_onetoone.one_to_one_token_id,
            expert_min_join_time,
            expert_max_leave_time,
            student_min_join_time,
            student_max_leave_time,
            student_duration,
            expert_duration,
            overlapping_time,
            video_sessions_onetoonetoken.difficulty_level
            from video_sessions_onetoone
            left join video_sessions_onetoonetoken on video_sessions_onetoonetoken.id = video_sessions_onetoone.one_to_one_token_id
            left join expert_report on expert_report.one_to_one_id = video_sessions_onetoone.id
            left join student_report on student_report.one_to_one_id = video_sessions_onetoone.id
    ;
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