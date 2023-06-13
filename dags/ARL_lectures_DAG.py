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
            'INSERT INTO arl_lectures (table_unique_key,course_id,lecture_id,lecture_title,mandatory,'
            'lecture_date,topic_template_id,lecture_module,total_topic_with_lecture,topics_marked,'
            'inst_total_time_in_mins,avg_overlapping_time,overall_student_count,live_student_count,'
            'one_star,two_star,three_star,four_star,five_star,lecture_csat,lecture_rating,'
            'rating_given_by,understood_feedback_given_by,didnt_understood,partially_understood,'
            'comp_understood,history_based_inst_total_time_in_mins,history_based_avg_overlapping_time,'
            'history_based_overall_student_count,history_based_live_student_count,history_based_one_star,'
            'history_based_two_star,history_based_three_star,history_based_four_star,history_based_five_star,'
            'history_based_lecture_csat,history_based_lecture_rating,history_based_rating_given_by,'
            'history_based_understood_feedback_given_by,history_based_didnt_understood,'
            'history_based_partially_understood,history_based_comp_understood)'
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            'on conflict (table_unique_key) do update set course_id=EXCLUDED.course_id,'
            'lecture_title=EXCLUDED.lecture_title,mandatory=EXCLUDED.mandatory,lecture_date=EXCLUDED.lecture_date,'
            'topic_template_id=EXCLUDED.topic_template_id,lecture_module=EXCLUDED.lecture_module,'
            'total_topic_with_lecture=EXCLUDED.total_topic_with_lecture,topics_marked=EXCLUDED.topics_marked,'
            'inst_total_time_in_mins=EXCLUDED.inst_total_time_in_mins,avg_overlapping_time=EXCLUDED.avg_overlapping_time,'
            'overall_student_count=EXCLUDED.overall_student_count,live_student_count=EXCLUDED.live_student_count,'
            'one_star=EXCLUDED.one_star,two_star=EXCLUDED.two_star,three_star=EXCLUDED.three_star,'
            'four_star=EXCLUDED.four_star,five_star=EXCLUDED.five_star,lecture_csat=EXCLUDED.lecture_csat,'
            'lecture_rating=EXCLUDED.lecture_rating,rating_given_by=EXCLUDED.rating_given_by,'
            'understood_feedback_given_by=EXCLUDED.understood_feedback_given_by,didnt_understood=EXCLUDED.didnt_understood,'
            'partially_understood=EXCLUDED.partially_understood,comp_understood=EXCLUDED.comp_understood,'
            'history_based_inst_total_time_in_mins=EXCLUDED.history_based_inst_total_time_in_mins,'
            'history_based_avg_overlapping_time=EXCLUDED.history_based_avg_overlapping_time,'
            'history_based_overall_student_count=EXCLUDED.history_based_overall_student_count,'
            'history_based_live_student_count=EXCLUDED.history_based_live_student_count,'
            'history_based_one_star=EXCLUDED.history_based_one_star,history_based_two_star=EXCLUDED.history_based_two_star,'
            'history_based_three_star=EXCLUDED.history_based_three_star,'
            'history_based_four_star=EXCLUDED.history_based_four_star,'
            'history_based_five_star=EXCLUDED.history_based_five_star,'
            'history_based_lecture_csat=EXCLUDED.history_based_lecture_csat,'
            'history_based_lecture_rating=EXCLUDED.history_based_lecture_rating,'
            'history_based_rating_given_by=EXCLUDED.history_based_rating_given_by,'
            'history_based_understood_feedback_given_by=EXCLUDED.history_based_understood_feedback_given_by,'
            'history_based_didnt_understood=EXCLUDED.history_based_didnt_understood,'
            'history_based_partially_understood=EXCLUDED.history_based_partially_understood,'
            'history_based_comp_understood=EXCLUDED.history_based_comp_understood;',
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
            )
        )
    pg_conn.commit()


dag = DAG(
    'ARL_Lectures',
    default_args=default_args,
    description='An Analytics Reporting Layer DAG for lectures',
    schedule_interval='35 1 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS arl_lectures (
            id serial,
            table_unique_key double precision not null PRIMARY KEY,
            course_id int,
            lecture_id int,
            lecture_title varchar(300),
            mandatory boolean,
            lecture_date DATE,
            topic_template_id int,
            lecture_module varchar(128),
            total_topic_with_lecture int,
            topics_marked int,
            inst_total_time_in_mins real,
            avg_overlapping_time real,
            overall_student_count int,
            live_student_count int,
            one_star int,
            two_star int,
            three_star int,
            four_star int,
            five_star int,
            lecture_csat real,
            lecture_rating real,
            rating_given_by int,
            understood_feedback_given_by int,
            didnt_understood int,
            partially_understood int,
            comp_understood int,
            history_based_inst_total_time_in_mins real,
            history_based_avg_overlapping_time real,
            history_based_overall_student_count int,
            history_based_live_student_count int,
            history_based_one_star int,
            history_based_two_star int,
            history_based_three_star int,
            history_based_four_star int,
            history_based_five_star int,
            history_based_lecture_csat real,
            history_based_lecture_rating real,
            history_based_rating_given_by int,
            history_based_understood_feedback_given_by int,
            history_based_didnt_understood int,
            history_based_partially_understood int,
            history_based_comp_understood int
        );
    ''',
    dag=dag
)

transform_data = PostgresOperator(
    task_id='transform_data',
    postgres_conn_id='postgres_result_db',
    sql='''with lecture_details as 
            (with basic_details as 
                    (select 
                        l.course_id,		
                        l.lecture_id,		
                        l.lecture_title,
                        l.mandatory,
                        date(l.start_timestamp) as lecture_date,
                        t.topic_template_id,
                        t.template_name as lecture_module
                    from
                        lectures l
                    join courses c 
                        on c.course_id = l.course_id and c.course_structure_id in (1,6,8,11,12,13,14,18,19,20,22,23,26,32)
                    left join lecture_topic_mapping ltm 
                        on ltm.lecture_id = l.lecture_id and ltm.completed = true
                    left join topics t 
                        on t.topic_id = ltm.topic_id and t.topic_template_id in (102, 103, 119, 334, 336, 338, 339, 340, 341, 342, 344, 410)
                    group by 1,2,3,4,5,6,7),
                    
                    marked_topics_details as 
                        (select
                            lecture_id,
                            count(distinct topic_id) as total_topic_with_lecture,
                            count(distinct topic_id) filter(where ltm.completed = true) as topics_marked
                        from
                            lecture_topic_mapping ltm
                        group by 1
                        order by 1 desc)
                        
                select 
                    basic_details.*,
                    marked_topics_details.total_topic_with_lecture,
                    marked_topics_details.topics_marked
                from
                    basic_details
                left join marked_topics_details
                    on marked_topics_details.lecture_id = basic_details.lecture_id),
                    
        attendance_and_time_data as 
            (select distinct
                l.lecture_id,
                lecture_instructor_mapping.duration_time_in_mins as inst_total_time_in_mins,
                avg(llcur.overlapping_time_in_mins) as avg_overlapping_time,
                count(distinct cum.user_id) filter (where (llcur.lecture_id is not null or rlcur.id is not null)) as overall_student_count,
                count(distinct cum.user_id) filter (where llcur.lecture_id is not null) as live_student_count,
                count(distinct cum.user_id) filter (where llcur.answer_rating = 1) as one_star,
                count(distinct cum.user_id) filter (where llcur.answer_rating = 2) as two_star,
                count(distinct cum.user_id) filter (where llcur.answer_rating = 3) as three_star,
                count(distinct cum.user_id) filter (where llcur.answer_rating = 4) as four_star,
                count(distinct cum.user_id) filter (where llcur.answer_rating = 5) as five_star,
                
                case 
                        when count(distinct cum.user_id) filter (where llcur.answer_rating is not null) = 0 then null
                        else count(distinct cum.user_id) filter (where llcur.answer_rating in (4,5)) * 100 / count(distinct cum.user_id) filter (where llcur.answer_rating is not null) 
                end	as lecture_csat,
                
                case 
                        when (count (distinct cum.user_id) filter (where llcur.answer_rating is not null) * 5) = 0 then null
                        else sum(llcur.answer_rating) * 100 / (count (distinct cum.user_id) filter (where llcur.answer_rating is not null) * 5) 
                end	as lecture_rating,
                
                count(distinct cum.user_id) filter (where llcur.answer_rating is not null) as rating_given_by,
                count(distinct cum.user_id) filter (where llcur.lecture_understood_response is not null) as understood_feedback_given_by,
                count(distinct cum.user_id) filter (where llcur.lecture_understood_rating = -1) as didnt_understood,
                count(distinct cum.user_id) filter (where llcur.lecture_understood_rating = 0) as partially_understood,
                count(distinct cum.user_id) filter (where llcur.lecture_understood_rating = 1) as comp_understood
            from
                lectures l
            join courses c
                on c.course_id = l.course_id and c.course_structure_id in (1,6,8,11,12,13,14,18,19,20,22,23,26,32)
            join course_user_mapping cum 
                on cum.course_id = c.course_id and cum.status in (5,8,9) and cum.label_id is null
            left join lecture_instructor_mapping
                on lecture_instructor_mapping.lecture_id = l.lecture_id
            left join live_lectures_course_user_reports llcur 
                on llcur.course_user_mapping_id = cum.course_user_mapping_id and l.lecture_id = llcur.lecture_id
            left join recorded_lectures_course_user_reports rlcur 
                on rlcur.course_user_mapping_id = cum.course_user_mapping_id and rlcur.lecture_id = l.lecture_id
            group by 1,2
            order by 1 desc),
            
        history_attendance_and_time_data as 
            (select distinct
                l.lecture_id,
                lecture_instructor_mapping.duration_time_in_mins as inst_total_time_in_mins,
                avg(llcur.overlapping_time_in_mins) as avg_overlapping_time,
                count(distinct wud.user_id) filter (where (llcur.lecture_id is not null or rlcur.id is not null)) as overall_student_count,
                count(distinct wud.user_id) filter (where llcur.lecture_id is not null) as live_student_count,
                count(distinct wud.user_id) filter (where llcur.answer_rating = 1) as one_star,
                count(distinct wud.user_id) filter (where llcur.answer_rating = 2) as two_star,
                count(distinct wud.user_id) filter (where llcur.answer_rating = 3) as three_star,
                count(distinct wud.user_id) filter (where llcur.answer_rating = 4) as four_star,
                count(distinct wud.user_id) filter (where llcur.answer_rating = 5) as five_star,
                case 
                        when count(distinct wud.user_id) filter (where llcur.answer_rating is not null ) = 0 then null
                        else count(distinct wud.user_id) filter (where llcur.answer_rating in (4,5) ) * 100 / count(distinct wud.user_id) filter (where llcur.answer_rating is not null ) 
                end	as lecture_csat,
                case 
                        when (count (distinct wud.user_id) filter (where llcur.answer_rating is not null) * 5) = 0 then null
                        else sum(llcur.answer_rating) filter (where wud.status in (5,8,9))*100 / (count (distinct wud.user_id) filter (where llcur.answer_rating is not null and wud.status in (5,8,9)) * 5) 
                end	as lecture_rating,
                
                count(distinct wud.user_id) filter (where llcur.answer_rating is not null and wud.status in (5,8,9)) as rating_given_by,
                count(distinct wud.user_id) filter (where llcur.lecture_understood_response is not null and wud.status in (5,8,9)) as understood_feedback_given_by,
                count(distinct wud.user_id) filter (where llcur.lecture_understood_rating = -1 and wud.status in (5,8,9)) as didnt_understood,
                count(distinct wud.user_id) filter (where llcur.lecture_understood_rating = 0 and wud.status in (5,8,9)) as partially_understood,
                count(distinct wud.user_id) filter (where llcur.lecture_understood_rating = 1 and wud.status in (5,8,9)) as comp_understood
            from
                lectures l
            join courses c
                on c.course_id = l.course_id and c.course_structure_id in (1,6,8,11,12,13,14,18,19,20,22,23,26,32)
            join weekly_user_details wud 
                on wud.course_id = c.course_id and wud.status in (5,8,9) and wud.label_mapping_id is null
                    and date(date_trunc('week', l.start_timestamp)) = date(wud.week_view)
            left join lecture_instructor_mapping
                on lecture_instructor_mapping.lecture_id = l.lecture_id
            left join live_lectures_course_user_reports llcur 
                on llcur.course_user_mapping_id = wud.course_user_mapping_id and l.lecture_id = llcur.lecture_id
            left join recorded_lectures_course_user_reports rlcur 
                on rlcur.course_user_mapping_id = wud.course_user_mapping_id and l.lecture_id = rlcur.lecture_id
            group by 1,2
            order by 1 desc)
        select 
            distinct concat(lecture_details.course_id,lecture_details.topic_template_id,lecture_details.lecture_id) as table_unique_key,
            lecture_details.course_id,
            lecture_details.lecture_id,
            lecture_details.lecture_title,
            lecture_details.mandatory,
            lecture_details.lecture_date,
            lecture_details.topic_template_id,
            lecture_details.lecture_module,
            lecture_details.total_topic_with_lecture,
            lecture_details.topics_marked,
            attendance_and_time_data.inst_total_time_in_mins,
            attendance_and_time_data.avg_overlapping_time,
            attendance_and_time_data.overall_student_count,
            attendance_and_time_data.live_student_count,
            attendance_and_time_data.one_star,
            attendance_and_time_data.two_star,
            attendance_and_time_data.three_star,
            attendance_and_time_data.four_star,
            attendance_and_time_data.five_star,
            attendance_and_time_data.lecture_csat,
            attendance_and_time_data.lecture_rating,
            attendance_and_time_data.rating_given_by,
            attendance_and_time_data.understood_feedback_given_by,
            attendance_and_time_data.didnt_understood,
            attendance_and_time_data.partially_understood,
            attendance_and_time_data.comp_understood,
            history_attendance_and_time_data.inst_total_time_in_mins as history_based_inst_total_time_in_mins,
            history_attendance_and_time_data.avg_overlapping_time as history_based_avg_overlapping_time,
            history_attendance_and_time_data.overall_student_count as history_based_overall_student_count,
            history_attendance_and_time_data.live_student_count as history_based_live_student_count,
            history_attendance_and_time_data.one_star as history_based_one_star,
            history_attendance_and_time_data.two_star as history_based_two_star,
            history_attendance_and_time_data.three_star as history_based_three_star,
            history_attendance_and_time_data.four_star as history_based_four_star,
            history_attendance_and_time_data.five_star as history_based_five_star,
            history_attendance_and_time_data.lecture_csat as history_based_lecture_csat,
            history_attendance_and_time_data.lecture_rating as history_based_lecture_rating,
            history_attendance_and_time_data.rating_given_by as history_based_rating_given_by,
            history_attendance_and_time_data.understood_feedback_given_by as history_based_understood_feedback_given_by,
            history_attendance_and_time_data.didnt_understood as history_based_didnt_understood,
            history_attendance_and_time_data.partially_understood as history_based_partially_understood,
            history_attendance_and_time_data.comp_understood as history_based_comp_understood
        from
            lecture_details
        left join attendance_and_time_data
            on attendance_and_time_data.lecture_id = lecture_details.lecture_id
        left join history_attendance_and_time_data
            on history_attendance_and_time_data.lecture_id = lecture_details.lecture_id
        order by 3 desc;
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