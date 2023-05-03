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
            'INSERT INTO live_lectures_course_user_reports (table_unique_key,lecture_id,course_user_mapping_id,course_user_mapping_status,report_type,'
            'min_created_at,min_join_time,max_leave_time,total_time_spent_in_mins,overlapping_time_in_mins,'
            'lecture_understood_response,lecture_understood_rating,feedback_answer,answer_rating)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (table_unique_key) do update set lecture_id = EXCLUDED.lecture_id,'
            'course_user_mapping_id = EXCLUDED.course_user_mapping_id,'
            'course_user_mapping_status = EXCLUDED.course_user_mapping_status,'
            'report_type = EXCLUDED.report_type,'
            'min_created_at = EXCLUDED.min_created_at,'
            'min_join_time = EXCLUDED.min_join_time,'
            'max_leave_time = EXCLUDED.max_leave_time,'
            'total_time_spent_in_mins = EXCLUDED.total_time_spent_in_mins,'
            'overlapping_time_in_mins = EXCLUDED.overlapping_time_in_mins,'
            'lecture_understood_response = EXCLUDED.lecture_understood_response,'
            'lecture_understood_rating = EXCLUDED.lecture_understood_rating,'
            'feedback_answer = EXCLUDED.feedback_answer,'
            'answer_rating = EXCLUDED.answer_rating;',
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
                transform_row[13]
            )
        )
    pg_conn.commit()


dag = DAG(
    'Live_lecture_course_user_reports_dag',
    default_args=default_args,
    description='Per lecture per user time spent and overlapping time',
    schedule_interval='0 22 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS live_lectures_course_user_reports (
            id serial not null,
            table_unique_key double precision not null PRIMARY KEY,
            lecture_id bigint,
            course_user_mapping_id bigint,
            course_user_mapping_status int,
            report_type int,
            min_created_at timestamp,
            min_join_time timestamp,
            max_leave_time timestamp,
            total_time_spent_in_mins bigint,
            overlapping_time_in_mins bigint,
            lecture_understood_response varchar(128),
            lecture_understood_rating int,
            feedback_answer varchar(64),
            answer_rating int
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
        course_user_mapping_id,
        inst_min_join_time,
        inst_max_leave_time,
        duration_time_in_secs/60 as duration_time_in_mins
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
                min(video_sessions_lecturecourseuserreport.join_time) as inst_min_join_time,
                max(video_sessions_lecturecourseuserreport.leave_time) as inst_max_leave_time,
                sum(duration) filter (where report_type = 4) as duration_time_in_secs
            from
                video_sessions_lecture
            left join video_sessions_lecturecourseuserreport
                on video_sessions_lecturecourseuserreport.lecture_id = video_sessions_lecture.id
            join raw_mapping
                on raw_mapping.cum_id = video_sessions_lecturecourseuserreport.course_user_mapping_id and video_sessions_lecture.course_id = raw_mapping.course_id
            group by 1,2,3)


            select
                lectures.*,
                dense_rank() over (partition by lecture_id order by duration_time_in_secs desc) as d_rank
            from
                lectures
            where duration_time_in_secs is not null
            order by 2) a
    where d_rank = 1
    order by 1),

raw_details as 
        (select
            video_sessions_lecture.id as lecture_id,
            video_sessions_lecturecourseuserreport.course_user_mapping_id,
            courses_courseusermapping.status as course_user_mapping_status,
            report_type,
            min(video_sessions_lecturecourseuserreport.created_at) as min_created_at,
            min(video_sessions_lecturecourseuserreport.join_time) as min_join_time,
            max(video_sessions_lecturecourseuserreport.leave_time) as max_leave_time,
            sum(duration)/60.0  as total_time_spent_in_mins,
            sum(duration) filter (where video_sessions_lecturecourseuserreport.join_time >= inst_time.inst_min_join_time and video_sessions_lecturecourseuserreport.leave_time <= inst_time.inst_max_leave_time)/60.0 as overlapping_time_in_mins
        from
            video_sessions_lecture
        left join inst_time
            on inst_time.lecture_id = video_sessions_lecture.id
        left join video_sessions_lecturecourseuserreport
            on video_sessions_lecturecourseuserreport.lecture_id = video_sessions_lecture.id
        left join courses_courseusermapping
            on courses_courseusermapping.id = video_sessions_lecturecourseuserreport.course_user_mapping_id
        where report_type in (1,2,4)
        group by 1,2,3,4
        order by 1 desc),

understanding_lecture_form_detail as 
    (select 
        video_sessions_lecture.id as lecture_id,
        courses_courseusermapping.id as course_user_mapping_id,
        courses_courseusermapping.user_id,
        feedback_feedbackanswer.text,
        case 
            when feedback_feedbackformuserquestionanswerm2m.feedback_answer_id = 179 then 1
            when feedback_feedbackformuserquestionanswerm2m.feedback_answer_id = 180 then 0
            when feedback_feedbackformuserquestionanswerm2m.feedback_answer_id = 181 then -1
        end as lecture_understood_rating
    from
        feedback_feedbackformusermapping
    left join video_sessions_lecture
        on feedback_feedbackformusermapping.entity_object_id = video_sessions_lecture.id and feedback_feedbackformusermapping.entity_content_type_id = 46 and feedback_feedbackformusermapping.feedback_form_id = 4377
    left join courses_courseusermapping
        on courses_courseusermapping.user_id = feedback_feedbackformusermapping.filled_by_id
    left join feedback_feedbackformuserquestionanswermapping
        on feedback_feedbackformuserquestionanswermapping.feedback_form_user_mapping_id = feedback_feedbackformusermapping.id and feedback_feedbackformuserquestionanswermapping.feedback_question_id = 331
    left join feedback_feedbackformuserquestionanswerm2m
        ON feedback_feedbackformuserquestionanswerm2m.feedback_form_user_question_answer_mapping_id = feedback_feedbackformuserquestionanswermapping.id
    left join feedback_feedbackanswer
        on feedback_feedbackanswer.id = feedback_feedbackformuserquestionanswerm2m.feedback_answer_id
    group by 1,2,3,4,5
    order by 1 desc),

csat_rating_details as
        (select
            feedback_feedbackformusermapping.filled_by_id as user_id,
            feedback_feedbackformusermapping.course_id,
            feedback_feedbackformusermapping.entity_object_id as lecture_id,
            courses_courseusermapping.id as course_user_mapping_id,
            feedback_feedbackanswer.text as feedback_answer,
            case
                when feedback_feedbackanswer.text = 'Awesome' then 5
                when feedback_feedbackanswer.text = 'Good' then 4
                when feedback_feedbackanswer.text = 'Average' then 3
                when feedback_feedbackanswer.text = 'Poor' then 2
                when feedback_feedbackanswer.text = 'Very Poor' then 1
            end as answer_rating

        from
            feedback_feedbackformusermapping
        left join courses_courseusermapping
            on courses_courseusermapping.user_id = feedback_feedbackformusermapping.filled_by_id and feedback_feedbackformusermapping.course_id = courses_courseusermapping.course_id
                and feedback_feedbackformusermapping.feedback_form_id = 4377 and feedback_feedbackformusermapping.entity_content_type_id = 46
        left join feedback_feedbackformuserquestionanswermapping
            on feedback_feedbackformusermapping.id = feedback_feedbackformuserquestionanswermapping.feedback_form_user_mapping_id and feedback_feedbackformuserquestionanswermapping.feedback_question_id = 348
        left join feedback_feedbackformuserquestionanswerm2m 
            on feedback_feedbackformuserquestionanswermapping.id = feedback_feedbackformuserquestionanswerm2m.feedback_form_user_question_answer_mapping_id
        left join feedback_feedbackanswer 
            on feedback_feedbackformuserquestionanswerm2m.feedback_answer_id = feedback_feedbackanswer.id)

select
    cast(concat(raw_details.lecture_id, row_number() over(order by raw_details.lecture_id)) as double precision) as table_unique_key,
    raw_details.*,
    understanding_lecture_form_detail.text as lecture_understood_response,
    understanding_lecture_form_detail.lecture_understood_rating,
    csat_rating_details.feedback_answer,
    csat_rating_details.answer_rating
from
    raw_details
left join understanding_lecture_form_detail
    on understanding_lecture_form_detail.lecture_id = raw_details.lecture_id and raw_details.course_user_mapping_id = understanding_lecture_form_detail.course_user_mapping_id
left join csat_rating_details
    on csat_rating_details.lecture_id = raw_details.lecture_id and raw_details.course_user_mapping_id = csat_rating_details.course_user_mapping_id;
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