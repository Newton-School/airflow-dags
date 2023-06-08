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
            'INSERT INTO arl_assessments_test (assessment_id,course_id,assessment_release_date,'
            'assessment_type,assessment_sub_type,max_marks,total_questions,total_mcqs_marked,'
            'history_based_total_mcqs_marked,total_correct_mcqs,history_based_total_correct_mcqs,'
            'students_opened,history_based_students_opened,users_opened_on_time,'
            'history_based_users_opened_on_time,users_opened_late,'
            'history_based_users_opened_late,students_submitted,'
            'history_based_students_submitted,users_submitted_on_time,'
            'history_based_users_submitted_on_time,users_submitted_late,'
            'history_based_users_submitted_late,overall_avg_assessment_percent,'
            'history_based_overall_avg_assessment_percent,students_above_avg_percent,'
            'history_based_students_above_avg_percent,students_below_avg_percent,'
            'history_based_students_below_avg_percent,median_marks,history_based_median_marks,'
            'users_with_zero_marks,history_based_users_with_zero_marks,users_with_marks_btw_0_and_25,'
            'history_based_users_with_marks_btw_0_and_25,users_with_marks_btw_25_and_50,'
            'history_based_users_with_marks_btw_25_and_50,users_with_marks_btw_50_and_75,'
            'history_based_users_with_marks_btw_50_and_75,users_with_marks_btw_75_and_100,'
            'history_based_users_with_marks_btw_75_and_100,users_with_full_marks,'
            'history_based_users_with_full_marks)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (assessment_id) do update set '
            'assessment_type=EXCLUDED.assessment_type,assessment_sub_type=EXCLUDED.assessment_sub_type,'
            'assessment_release_date=EXCLUDED.assessment_release_date,max_marks=EXCLUDED.max_marks,'
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
            'users_with_full_marks=EXCLUDED.users_with_full_marks,'
            'history_based_total_mcqs_marked=EXCLUDED.history_based_total_mcqs_marked,'
            'history_based_total_correct_mcqs=EXCLUDED.history_based_total_correct_mcqs,'
            'history_based_students_opened=EXCLUDED.history_based_students_opened,'
            'history_based_users_opened_on_time=EXCLUDED.history_based_users_opened_on_time,'
            'history_based_users_opened_late=EXCLUDED.history_based_users_opened_late,'
            'history_based_students_submitted=EXCLUDED.history_based_students_submitted,'
            'history_based_users_submitted_on_time=EXCLUDED.history_based_users_submitted_on_time,'
            'history_based_users_submitted_late=EXCLUDED.history_based_users_submitted_late,'
            'history_based_overall_avg_assessment_percent=EXCLUDED.history_based_overall_avg_assessment_percent,'
            'history_based_students_above_avg_percent=EXCLUDED.history_based_students_above_avg_percent,'
            'history_based_students_below_avg_percent=EXCLUDED.history_based_students_below_avg_percent,'
            'history_based_median_marks=EXCLUDED.history_based_median_marks,'
            'history_based_users_with_zero_marks=EXCLUDED.history_based_users_with_zero_marks,'
            'history_based_users_with_marks_btw_0_and_25=EXCLUDED.history_based_users_with_marks_btw_0_and_25,'
            'history_based_users_with_marks_btw_25_and_50=EXCLUDED.history_based_users_with_marks_btw_25_and_50,'
            'history_based_users_with_marks_btw_50_and_75=EXCLUDED.history_based_users_with_marks_btw_50_and_75,'
            'history_based_users_with_marks_btw_75_and_100=EXCLUDED.history_based_users_with_marks_btw_75_and_100,'
            'history_based_users_with_full_marks=EXCLUDED.history_based_users_with_full_marks;',
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
                transform_row[27],
                transform_row[28],
                transform_row[29],
                transform_row[30],
                transform_row[31],
                transform_row[32],
                transform_row[33],
                transform_row[34],
                transform_row[35],
                transform_row[36],
                transform_row[37],
                transform_row[38],
                transform_row[39],
                transform_row[40],
                transform_row[41],
                transform_row[42],
            )
        )
    pg_conn.commit()


dag = DAG(
    'ARL_Assessments_2',
    default_args=default_args,
    description='An Analytics Reporting Layer DAG for Assessments',
    schedule_interval='35 0 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS arl_assessments_test (
            assessment_id int not null PRIMARY KEY,
            course_id int,
            assessment_release_date DATE,
            assessment_type varchar(64),
            assessment_sub_type varchar(32),
            max_marks int,
            total_questions int,
            total_mcqs_marked int,
            history_based_total_mcqs_marked int,
            total_correct_mcqs int,
            history_based_total_correct_mcqs int,
            students_opened int,
            history_based_students_opened int,
            users_opened_on_time int,
            history_based_users_opened_on_time int,
            users_opened_late int,
            history_based_users_opened_late int,
            students_submitted int,
            history_based_students_submitted int,
            users_submitted_on_time int,
            history_based_users_submitted_on_time int,
            users_submitted_late int,
            history_based_users_submitted_late int,
            overall_avg_assessment_percent int,
            history_based_overall_avg_assessment_percent int,
            students_above_avg_percent int,
            history_based_students_above_avg_percent int,
            students_below_avg_percent int,
            history_based_students_below_avg_percent int,
            median_marks int,
            history_based_median_marks int,
            users_with_zero_marks int,
            history_based_users_with_zero_marks int,
            users_with_marks_btw_0_and_25 int,
            history_based_users_with_marks_btw_0_and_25 int,
            users_with_marks_btw_25_and_50 int,
            history_based_users_with_marks_btw_25_and_50 int,
            users_with_marks_btw_50_and_75 int,
            history_based_users_with_marks_btw_50_and_75 int,
            users_with_marks_btw_75_and_100 int,
            history_based_users_with_marks_btw_75_and_100 int,
            users_with_full_marks int,
            history_based_users_with_full_marks int
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_result_db',
    sql='''with student_all_raw as
            (select 
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
            group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
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
            group by 1,2),
            
        history_based_assessments_data as
        
                (with history_based_assessments_data as 
                
                    (select 
                        course_user_mapping_new.user_id,
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
                    join -- this is the raw dump of enrolled/isa-signed users per week, first week april 17, 2023
                            (select distinct
                                    wud.course_user_mapping_id,
                                    wud.user_id ,
                                    c.course_id,
                                    wud.week_view ,
                                    wud.status
                                from
                                    weekly_user_details wud 
                                join courses c 
                                    on c.course_id = wud.course_id and c.course_structure_id in (1,6,8,11,12,14,18,19,20,22,23,26,32)
                                        and wud.label_mapping_id is null and wud.status in (5,8,9) and wud.unit_type like 'LEARNING') course_user_mapping_new
                        on course_user_mapping_new.course_id = assessments.course_id and date(date_trunc('week', assessments.start_timestamp)) = course_user_mapping_new.week_view    
                    left join assessment_question_user_mapping
                        on assessment_question_user_mapping.assessment_id = assessments.assessment_id and assessment_question_user_mapping.course_user_mapping_id = course_user_mapping_new.course_user_mapping_id
                    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16),

            overall_avg_marks as 
                (select 
                    course_id,
                    assessment_id,
                    case 
                        when sum(max_marks) filter (where marks_obtained is not null) <> 0 then sum(marks_obtained) * 100 / sum(max_marks) filter (where marks_obtained is not null) 
                        else 0
                    end as overall_avg_assessment_percent
                from
                    history_based_assessments_data
                group by 1,2)		
                
            select 
                history_based_assessments_data.assessment_id,
                history_based_assessments_data.course_id,
                assessment_release_date,
                assessment_type,
                assessment_sub_type,
                max_marks,
                question_count as total_questions,
                sum(questions_marked) as history_based_total_mcqs_marked,
                sum(questions_correct) as history_based_total_correct_mcqs,
                count(distinct user_id) filter (where assessment_attempt_status not like 'Not Attempted') as history_based_students_opened,
                count(distinct user_id) filter (where assessment_attempt_status like 'Attempted On Time') as history_based_users_opened_on_time,
                count(distinct user_id) filter (where assessment_attempt_status like 'Late Attempt') as history_based_users_opened_late,
                count(distinct user_id) filter (where assessment_submission_status not like 'Not Submitted') as history_based_students_submitted,
                count(distinct user_id) filter (where assessment_submission_status like 'On Time Submission') as history_based_users_submitted_on_time,
                count(distinct user_id) filter (where assessment_submission_status like 'Late Submission') as history_based_users_submitted_late,
                case 
                    when sum(max_marks) filter (where marks_obtained is not null) <> 0 then sum(marks_obtained) * 100 / sum(max_marks) filter (where marks_obtained is not null) 
                    else 0
                end as history_based_overall_avg_assessment_percent,
                count(distinct user_id) filter (where assessment_percent >= overall_avg_assessment_percent) as history_based_students_above_avg_percent,
                count(distinct user_id) filter (where assessment_percent < overall_avg_assessment_percent) as history_based_students_below_avg_percent,
                percentile_cont(0.5) within group (order by marks_obtained) as history_based_median_marks,
                count(distinct user_id) filter (where assessment_percent = 0) as history_based_users_with_zero_marks,
                count(distinct user_id) filter (where assessment_percent > 0 and assessment_percent < 25) as "history_based_users_with_marks_btw_0_and_25",
                count(distinct user_id) filter (where assessment_percent >= 25 and assessment_percent < 50)  as "history_based_users_with_marks_btw_25_and_50",
                count(distinct user_id) filter (where assessment_percent >= 50 and assessment_percent < 75) as "history_based_users_with_marks_btw_50_and_75",
                count(distinct user_id) filter (where assessment_percent >= 75 and assessment_percent < 100) as "history_based_users_with_marks_btw_75_and_100",
                count(distinct user_id) filter (where assessment_percent = 100) as history_based_users_with_full_marks
            from
                history_based_assessments_data
            left join overall_avg_marks
                on overall_avg_marks.assessment_id = history_based_assessments_data.assessment_id
            group by 1,2,3,4,5,6,7)
           

        select 
            student_all_raw.assessment_id,
            student_all_raw.course_id,
            student_all_raw.assessment_release_date,
            student_all_raw.assessment_type,
            student_all_raw.assessment_sub_type,
            student_all_raw.max_marks,
            student_all_raw.question_count as total_questions,
            sum(questions_marked) as total_mcqs_marked,
            history_based_assessments_data.history_based_total_mcqs_marked,
            sum(questions_correct) as total_correct_mcqs,
            history_based_assessments_data.history_based_total_correct_mcqs,
            count(distinct user_id) filter (where assessment_attempt_status not like 'Not Attempted') as students_opened,
            history_based_assessments_data.history_based_students_opened,
            count(distinct user_id) filter (where assessment_attempt_status like 'Attempted On Time') as users_opened_on_time,
            history_based_assessments_data.history_based_users_opened_on_time,
            count(distinct user_id) filter (where assessment_attempt_status like 'Late Attempt') as users_opened_late,
            history_based_assessments_data.history_based_users_opened_late,
            count(distinct user_id) filter (where assessment_submission_status not like 'Not Submitted') as students_submitted,
            history_based_assessments_data.history_based_students_submitted,
            count(distinct user_id) filter (where assessment_submission_status like 'On Time Submission') as users_submitted_on_time,
            history_based_assessments_data.history_based_users_submitted_on_time,
            count(distinct user_id) filter (where assessment_submission_status like 'Late Submission') as users_submitted_late,
            history_based_assessments_data.history_based_users_submitted_late,
            case 
                when sum(student_all_raw.max_marks) filter (where student_all_raw.marks_obtained is not null) <> 0 then sum(student_all_raw.marks_obtained) * 100 / sum(student_all_raw.max_marks) filter (where student_all_raw.marks_obtained is not null) 
                else 0
            end as overall_avg_assessment_percent,
            history_based_assessments_data.history_based_overall_avg_assessment_percent,
            count(distinct user_id) filter (where assessment_percent >= overall_avg_assessment_percent) as students_above_avg_percent,
            history_based_assessments_data.history_based_students_above_avg_percent,
            count(distinct user_id) filter (where assessment_percent < overall_avg_assessment_percent) as students_below_avg_percent,
            history_based_assessments_data.history_based_students_below_avg_percent,
            percentile_cont(0.5) within group (order by marks_obtained) as median_marks,
            history_based_assessments_data.history_based_median_marks,
            count(distinct user_id) filter (where assessment_percent = 0) as users_with_zero_marks,
            history_based_assessments_data.history_based_users_with_zero_marks,
            count(distinct user_id) filter (where assessment_percent > 0 and assessment_percent < 25) as "users_with_marks_btw_0_and_25",
            history_based_assessments_data."history_based_users_with_marks_btw_0_and_25",
            count(distinct user_id) filter (where assessment_percent >= 25 and assessment_percent < 50)  as "users_with_marks_btw_25_and_50",
            history_based_assessments_data."history_based_users_with_marks_btw_25_and_50",
            count(distinct user_id) filter (where assessment_percent >= 50 and assessment_percent < 75) as "users_with_marks_btw_50_and_75",
            history_based_assessments_data."history_based_users_with_marks_btw_50_and_75",
           count(distinct user_id) filter (where assessment_percent >= 75 and assessment_percent < 100) as "users_with_marks_btw_75_and_100",
            history_based_assessments_data."history_based_users_with_marks_btw_75_and_100",
            count(distinct user_id) filter (where assessment_percent = 100) as users_with_full_marks,
            history_based_assessments_data.history_based_users_with_full_marks
        from
            student_all_raw
        left join overall_avg_marks
            on overall_avg_marks.assessment_id = student_all_raw.assessment_id
        left join history_based_assessments_data
            on history_based_assessments_data.assessment_id = student_all_raw.assessment_id
        group by 1,2,3,4,5,6,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39,41,43;
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