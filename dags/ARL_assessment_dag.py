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
            'INSERT INTO arl_assessments (assessment_id,course_id,assessment_release_date,'
            'assessment_type,assessment_sub_type,max_marks,total_questions,'
            'total_mcqs_marked,total_correct_mcqs,students_opened,users_opened_on_time,'
            'users_opened_late,students_submitted,users_submitted_on_time,'
            'users_submitted_late,overall_avg_assessment_percent,students_above_avg_percent,'
            'students_below_avg_percent,median_marks,users_with_zero_marks,'
            'users_with_marks_btw_0_and_25,users_with_marks_btw_25_and_50,'
            'users_with_marks_btw_50_and_75,users_with_marks_btw_75_and_100,users_with_full_marks)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (assessment_id) do update set max_marks=EXCLUDED.max_marks,'
            'total_questions=EXCLUDED.total_questions,total_mcqs_marked=EXCLUDED.total_mcqs_marked,'
            'total_correct_mcqs=EXCLUDED.total_correct_mcqs,students_opened=EXCLUDED.students_opened,'
            'users_opened_on_time=EXCLUDED.users_opened_on_time,users_opened_late=EXCLUDED.users_opened_late,'
            'students_submitted=EXCLUDED.students_submitted,users_submitted_on_time=EXCLUDED.users_submitted_on_time,'
            'users_submitted_late=EXCLUDED.users_submitted_late,'
            'overall_avg_assessment_percent=EXCLUDED.overall_avg_assessment_percent,'
            'students_above_avg_percent=EXCLUDED.students_above_avg_percent,'
            'students_below_avg_percent=EXCLUDED.students_below_avg_percent,'
            'median_marks=EXCLUDED.median_marks,'
            'users_with_zero_marks=EXCLUDED.users_with_zero_marks,'
            'users_with_marks_btw_0_and_25=EXCLUDED.users_with_marks_btw_0_and_25,'
            'users_with_marks_btw_25_and_50=EXCLUDED.users_with_marks_btw_25_and_50,'
            'users_with_marks_btw_50_and_75=EXCLUDED.users_with_marks_btw_50_and_75,'
            'users_with_marks_btw_75_and_100=EXCLUDED.users_with_marks_btw_75_and_100,'
            'users_with_full_marks=EXCLUDED.users_with_full_marks ;',
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
            )
        )
    pg_conn.commit()


dag = DAG(
    'ARL_Assessments',
    default_args=default_args,
    description='An Analytics Reporting Layer DAG for Assessments',
    schedule_interval='35 0 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS arl_assessments (
            assessment_id int not null PRIMARY KEY,
            course_id int,
            assessment_release_date DATE,
            assessment_type varchar(32),
            assessment_sub_type varchar(16),
            max_marks int,
            total_questions int,
            total_mcqs_marked int,
            total_correct_mcqs int,
            students_opened int,
            users_opened_on_time int,
            users_opened_late int,
            students_submitted int,
            users_submitted_on_time int,
            users_submitted_late int,
            overall_avg_assessment_percent real,
            students_above_avg_percent int,
            students_below_avg_percent int,
            median_marks real,
            users_with_zero_marks int,
            users_with_marks_btw_0_and_25 int,
            users_with_marks_btw_25_and_50 int,
            users_with_marks_btw_50_and_75 int,
            users_with_marks_btw_75_and_100 int,
            users_with_full_marks int
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_result_db',
    sql='''with student_all_raw as
            
                (select 
                    cast(concat(assessments.course_id, assessments.assessment_id, course_user_mapping.user_id) as bigint) as table_unique_key,
                    course_user_mapping.user_id,
                    assessments.course_id,
                    assessments.assessment_id,
                    assessments.title as assessment_title,
                    case
                        when assessments.assessment_type = 1 then 'Normal Assessment'
                        when assessments.assessment_type = 2 then 'Filtering Assessment'
                        when assessments.assessment_type = 3 then 'Competitive Assessment'
                        when assessments.assessment_type = 4 then 'Duration Assessment'
                        when assessments.assessment_type = 5 then 'Poll Assessment'
                    end as assessment_type,
                    case 
                        when assessments.sub_type = 1 then 'General'
                        when assessments.sub_type = 2 then 'In-Class'
                        when assessments.sub_type = 3 then 'Post-Class'
                        when assessments.sub_type = 4 then 'Classroom-Quiz'
                    end as assessment_sub_type,
                    
                    date(assessments.start_timestamp) as assessment_release_date,
                    date(assessment_question_user_mapping.assessment_started_at) as assessment_open_date,
                    date(assessment_question_user_mapping.assessment_completed_at) as assessment_submission_date,
                    case 
                        when date(assessment_question_user_mapping.assessment_started_at) <= date(assessments.end_timestamp) then 'Attempted On Time'
                        when date(assessment_question_user_mapping.assessment_started_at) > date(assessments.end_timestamp) then 'Late Attempt'
                        when date(assessment_question_user_mapping.assessment_started_at) is null then 'Not Attempted'
                    end as assessment_attempt_status,
            
                    case 
                        when assessment_late_completed = false then 'On Time Submission'
                        when assessment_late_completed = true then 'Late Submission'
                        when assessment_late_completed is null then 'Not Submitted'
                    end as assessment_submission_status,
                    assessments.question_count,
                    assessments.max_marks,
                    assessment_question_user_mapping.marks_obtained,
                    case
                        when assessments.max_marks <> 0 then assessment_question_user_mapping.marks_obtained * 100 /assessments.max_marks
                        else 0
                    end as assessment_percent,
                    assessment_question_user_mapping.cheated,
                    count(distinct mcq_id) filter (where option_marked_at is not null) as questions_marked,
                    count(distinct mcq_id) filter (where (marked_choice - correct_choice) = 0) as questions_correct
                from
                    assessments
                join courses
                    on courses.course_id = assessments.course_id and course_structure_id in (1,6,8,11,12,14,18,19,20,22,23,26)
                join course_user_mapping
                    on course_user_mapping.course_id = assessments.course_id and status in (5,8,9) and label_id is null and lower(course_user_mapping.unit_type) like 'learning'
                left join assessment_question_user_mapping
                    on assessment_question_user_mapping.assessment_id = assessments.assessment_id and assessment_question_user_mapping.course_user_mapping_id = course_user_mapping.course_user_mapping_id
                group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
                order by 8 desc),
                
            overall_avg_marks as 
                (select 
                    course_id,
                    assessment_id,
                    case 
                        when sum(max_marks) filter (where marks_obtained is not null) <> 0 then sum(marks_obtained) * 100 / sum(max_marks) filter (where marks_obtained is not null) 
                        else 0
                    end as overall_avg_assessment_percent
                from
                    student_all_raw
                group by 1,2)
                
            select 
                distinct student_all_raw.assessment_id,
                student_all_raw.course_id,
                assessment_release_date,
                assessment_type,
                assessment_sub_type,
                max_marks,
                question_count as total_questions,
                sum(questions_marked) as total_mcqs_marked,
                sum(questions_correct) as total_correct_mcqs,
                count(distinct user_id) filter (where assessment_attempt_status not like 'Not Attempted') as students_opened,
                count(distinct user_id) filter (where assessment_attempt_status like 'Attempted On Time') as users_opened_on_time,
                count(distinct user_id) filter (where assessment_attempt_status like 'Late Attempt') as users_opened_late,
                count(distinct user_id) filter (where assessment_submission_status not like 'Not Submitted') as students_submitted,
                count(distinct user_id) filter (where assessment_submission_status like 'On Time Submission') as users_submitted_on_time,
                count(distinct user_id) filter (where assessment_submission_status like 'Late Submission') as users_submitted_late,
                case 
                    when sum(max_marks) filter (where marks_obtained is not null) <> 0 then sum(marks_obtained) * 100 / sum(max_marks) filter (where marks_obtained is not null) 
                    else 0
                end as overall_avg_assessment_percent,
                count(distinct user_id) filter (where assessment_percent >= overall_avg_assessment_percent) as students_above_avg_percent,
                count(distinct user_id) filter (where assessment_percent < overall_avg_assessment_percent) as students_below_avg_percent,
                percentile_cont(0.5) within group (order by marks_obtained) as median_marks,
                count(distinct user_id) filter (where assessment_percent = 0) as users_with_zero_marks,
                count(distinct user_id) filter (where assessment_percent > 0 and assessment_percent < 25) as "users_with_marks_btw_0_and_25",
                count(distinct user_id) filter (where assessment_percent >= 25 and assessment_percent < 50)  as "users_with_marks_btw_25_and_50",
                count(distinct user_id) filter (where assessment_percent >= 50 and assessment_percent < 75) as "users_with_marks_btw_50_and_75",
                count(distinct user_id) filter (where assessment_percent >= 75 and assessment_percent < 100) as "users_with_marks_btw_75_and_100",
                count(distinct user_id) filter (where assessment_percent = 100) as users_with_full_marks
            from
                student_all_raw
            left join overall_avg_marks
                on overall_avg_marks.assessment_id = student_all_raw.assessment_id
            group by 1,2,3,4,5,6,7;
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