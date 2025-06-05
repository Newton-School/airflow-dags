from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'max_active_tasks': 6,
    'max_active_runs': 6,
    'concurrency': 4,
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 5),
}

lecture_per_dags = Variable.get("lecture_per_dag", 4000)

total_number_of_sub_dags = Variable.get("total_number_of_sub_dags", 5)

total_number_of_extraction_cps_dags = Variable.get("total_number_of_extraction_cps_dags", 10)

dag = DAG(
    'lecture_course_user_reports_growth_dag',
    default_args=default_args,
    concurrency=4,
    max_active_tasks=6,
    max_active_runs=6,
    description='Table at user, lecture level for all users who attend masterclass and TnB with cum.status in (1,8,10,11,12,30)',
    schedule_interval='35 3 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS lecture_course_user_reports_growth (
            id bigserial,
            table_unique_key text not null PRIMARY KEY,
            user_id bigint,
            student_name text,
            lead_type text,
            student_category text,
            course_user_mapping_id bigint,
            label_mapping_status text,
            course_id int,
            course_name text,
            course_structure_class text,
            lecture_id bigint,
            lecture_title text,
            lecture_type text,
            mandatory boolean,
            lecture_start_timestamp timestamp,
            inst_min_join_time timestamp,
            inst_max_leave_time timestamp, 
            inst_total_time_in_mins int,
            inst_user_id bigint,
            instructor_name text,
            lecture_date DATE,
            live_attendance int,
            recorded_attendance int,
            overall_attendance int,
            total_overlapping_time_in_mins int,
            total_user_time int,
            user_min_join_time timestamp,
            user_max_leave_time timestamp,
            answer_rating int,
            rating_feedback_answer text,
            lecture_understood_rating int,
            lecture_understanding_feedback_answer text,
            activity_status_7_days text,
            activity_status_14_days text,
            activity_status_30_days text,
            user_placement_status text,
            admin_course_id int,
            admin_unit_name text,
            child_video_session boolean
        );
    ''',
    dag=dag
)


# Leaf Level Abstraction
def extract_data_to_nested(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres_result_db')
    pg_conn = pg_hook.get_conn()
    ti = kwargs['ti']
    current_lecture_sub_dag_id = kwargs['current_lecture_sub_dag_id']
    current_cps_sub_dag_id = kwargs['current_cps_sub_dag_id']
    transform_data_output = ti.xcom_pull(
        task_ids=f'transforming_data_{current_lecture_sub_dag_id}.extract_and_transform_individual_lecture_sub_dag_{current_lecture_sub_dag_id}_cps_sub_dag_{current_cps_sub_dag_id}.transform_data')
    for transform_row in transform_data_output:
        pg_cursor = pg_conn.cursor()
        pg_cursor.execute('INSERT INTO lecture_course_user_reports_growth (table_unique_key, user_id, student_name,'
                          'lead_type, student_category, course_user_mapping_id, label_mapping_status,'
                          'course_id, course_name, course_structure_class, lecture_id, lecture_title,'
                          'lecture_type, '
                          'mandatory, '
                          'lecture_start_timestamp,'
                          'inst_min_join_time, '
                          'inst_max_leave_time,'
                          'inst_total_time_in_mins, inst_user_id, instructor_name,'
                          'lecture_date, live_attendance, recorded_attendance,'
                          'overall_attendance, total_overlapping_time_in_mins, total_user_time, user_min_join_time,'
                          'user_max_leave_time,'
                          'answer_rating,'
                          'rating_feedback_answer,'
                          'lecture_understood_rating,'
                          'lecture_understanding_feedback_answer,'
                          'activity_status_7_days,'
                          'activity_status_14_days,'
                          'activity_status_30_days,'
                          'user_placement_status,'
                          'admin_course_id,'
                          'admin_unit_name,'
                          'child_video_session)'
                          'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                          'on conflict (table_unique_key) do update set student_name = EXCLUDED.student_name,'
                          'lead_type = EXCLUDED.lead_type,'
                          'student_category = EXCLUDED.student_category,'
                          'label_mapping_status = EXCLUDED.label_mapping_status,'
                          'course_name = EXCLUDED.course_name,'
                          'course_structure_class = EXCLUDED.course_structure_class,'
                          'lecture_title = EXCLUDED.lecture_title,'
                          'lecture_type = EXCLUDED.lecture_type,'
                          'mandatory = EXCLUDED.mandatory,'
                          'lecture_start_timestamp = EXCLUDED.lecture_start_timestamp,'
                          'inst_min_join_time = EXCLUDED.inst_min_join_time,'
                          'inst_max_leave_time = EXCLUDED.inst_max_leave_time,'
                          'inst_total_time_in_mins = EXCLUDED.inst_total_time_in_mins,'
                          'inst_user_id = EXCLUDED.inst_user_id,'
                          'instructor_name = EXCLUDED.instructor_name,'
                          'lecture_date = EXCLUDED.lecture_date,'
                          'live_attendance = EXCLUDED.live_attendance,'
                          'recorded_attendance = EXCLUDED.recorded_attendance,'
                          'overall_attendance = EXCLUDED.overall_attendance,'
                          'total_overlapping_time_in_mins = EXCLUDED.total_overlapping_time_in_mins,'
                          'total_user_time = EXCLUDED.total_user_time,'
                          'user_min_join_time = EXCLUDED.user_min_join_time,'
                          'user_max_leave_time = EXCLUDED.user_max_leave_time,'
                          'answer_rating = EXCLUDED.answer_rating,'
                          'rating_feedback_answer = EXCLUDED.rating_feedback_answer,'
                          'lecture_understood_rating = EXCLUDED.lecture_understood_rating,'
                          'lecture_understanding_feedback_answer = EXCLUDED.lecture_understanding_feedback_answer,'
                          'activity_status_7_days = EXCLUDED.activity_status_7_days,'
                          'activity_status_14_days = EXCLUDED.activity_status_14_days,'
                          'activity_status_30_days = EXCLUDED.activity_status_30_days,'
                          'user_placement_status = EXCLUDED.user_placement_status,'
                          'admin_course_id = EXCLUDED.admin_course_id,'
                          'admin_unit_name = EXCLUDED.admin_unit_name,'
                          'child_video_session = EXCLUDED.child_video_session;',
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
                          )
                          )
        pg_conn.commit()
        pg_cursor.close()
    pg_conn.close()


def number_of_rows_per_lecture_sub_dag_func(start_lecture_id, end_lecture_id):
    return PostgresOperator(
        task_id='number_of_rows_per_lecture_sub_dag',
        postgres_conn_id='postgres_result_db',
        dag=dag,
        sql='''select count(table_unique_key) from
            (with user_raw_data as
                (select
                    lecture_id,
                    course_user_mapping_id,
                    join_time,
                    leave_time,
                    extract(epoch from (leave_time - join_time))/60 as time_diff,
                    user_type,
                    overlapping_time_seconds,
                    overlapping_time_minutes
                from
                    lecture_engagement_time let
                where lower(user_type) like 'user'
                group by 1,2,3,4,5,6,7,8),

            inst_raw_data as
                (select 
                    lecture_id,
                    user_id as inst_user_id, 
                    let.course_user_mapping_id as inst_cum_id,
                    join_time,
                    leave_time,
                    extract('epoch' from (leave_time - join_time))/60 as time_diff_in_mins
                from
                    lecture_engagement_time let
                join course_user_mapping cum 
                    on cum.course_user_mapping_id = let.course_user_mapping_id 
                where lower(user_type) like 'instructor'
                group by 1,2,3,4,5,6),

            inst_data as 
                (select 
                    lecture_id,
                    inst_user_id,
                    inst_cum_id,
                    min(join_time) as inst_min_join_time,
                    max(leave_time) as inst_max_leave_time,
                    sum(time_diff_in_mins) as inst_total_time_in_mins
                from
                    inst_raw_data
                group by 1,2,3),

            user_overlapping_time as
                (select 
                    lecture_id,
                    course_user_mapping_id,
                    min(join_time) as min_join_time,
                    max(leave_time) as max_leave_time,
                    sum(overlapping_time_seconds) as total_overlapping_in_seconds,
                    sum(overlapping_time_minutes) as total_overlapping_time_minutes,
                    sum(time_diff) as total_user_time
                from
                    user_raw_data
                group by 1,2),

            lecture_rating as 
                (select
                    user_id,
                    entity_object_id as lecture_id,
                    case
                        when ffar.feedback_answer = 'Awesome' then 5
                        when ffar.feedback_answer = 'Good' then 4
                        when ffar.feedback_answer = 'Average' then 3
                        when ffar.feedback_answer = 'Poor' then 2
                        when ffar.feedback_answer = 'Very Poor' then 1
                    end as answer_rating,
                    feedback_answer as rating_feedback_answer
                from
                    feedback_form_all_responses_new ffar
                where ffar.feedback_form_id = 4377 
                    and ffar.feedback_question_id = 348
                group by 1,2,3,4),

            lecture_understanding as 
                (select
                        user_id,
                        entity_object_id as lecture_id,
                    case 
                        when feedback_answer_id = 179 then 1
                        when feedback_answer_id = 180 then 0
                        when feedback_answer_id = 181 then -1
                    end as lecture_understood_rating,
                        feedback_answer as lecture_understanding_feedback_answer
                    from
                        feedback_form_all_responses_new ffar
                    where ffar.feedback_form_id = 4377 
                        and ffar.feedback_question_id = 331
                    group by 1,2,3,4)    

            select
                concat(cum.user_id, l.lecture_id) as table_unique_key,
                cum.user_id,
                concat(ui.first_name,' ',ui.last_name) as student_name,
                ui.lead_type,
                cucm.student_category,
                cum.course_user_mapping_id,
                case 
                    when cum.label_id is null and cum.status in (8) then 'Enrolled Student'
                    when cum.label_id is null and cum.status in (1) then 'Applied'
                    when cum.label_id is not null and cum.status in (8) then 'Label Marked Student'
                    when c.course_structure_id in (1,18) and cum.status in (11,12) then 'ISA Cancelled Student'
                    when c.course_structure_id not in (1,18) and cum.status in (30) then 'Deferred Student'
                    when c.course_structure_id not in (1,18) and cum.status in (11) then 'Foreclosed Student'
                    when c.course_structure_id not in (1,18) and cum.status in (12) then 'Reject by NS-Ops'
                    else 'Mapping Error'
                end as label_mapping_status,
                c.course_id,
                c.course_name,
                c.course_structure_class,
                l.lecture_id,
                l.lecture_title,
                l.lecture_type,
                l.mandatory,
                l.start_timestamp as lecture_start_timestamp,
                inst_min_join_time,
                inst_max_leave_time,
                cast(inst_total_time_in_mins as int) as inst_total_time_in_mins,
                inst_user_id,
                concat(ui2.first_name,' ' ,ui2.last_name) as instructor_name,
                date(l.start_timestamp) as lecture_date,
                count(distinct let.lecture_id) filter (where let.lecture_id is not null) as live_attendance,
                count(distinct rlcur.lecture_id) filter (where rlcur.id is not null) as recorded_attendance,
                case
                    when (count(distinct let.lecture_id) filter (where let.lecture_id is not null) = 1 or 
                        count(distinct rlcur.lecture_id) filter (where rlcur.id is not null) = 1) then 1
                    else 0 
                end as overall_attendance,
                cast(let.total_overlapping_time_minutes as int) as total_overlapping_time_in_mins,
                cast(let.total_user_time as int) as total_user_time ,
                let.min_join_time as user_min_join_time,
                let.max_leave_time as user_max_leave_time,
                answer_rating,
                rating_feedback_answer,
                lecture_understood_rating,
                lecture_understanding_feedback_answer,
                uasm.activity_status_7_days,
                uasm.activity_status_14_days,
                uasm.activity_status_30_days,
                cum.user_placement_status,
                cum.admin_course_id,
                cum.admin_unit_name,
                l.child_video_session 
            from
                courses c
            join course_user_mapping cum
                on cum.course_id = c.course_id and c.course_structure_id in (82,83,130,134,135,168,169,170,171)
                    and cum.status in (1,8,10,11,12,30)
            join lectures l
                on l.course_id = c.course_id and l.start_timestamp >= '2025-03-01'
                    and (l.lecture_id between %d and %d)
            left join user_overlapping_time let
                on let.lecture_id = l.lecture_id and let.course_user_mapping_id = cum.course_user_mapping_id
            left join recorded_lectures_course_user_reports rlcur
                on rlcur.lecture_id = l.lecture_id and rlcur.course_user_mapping_id = cum.course_user_mapping_id
            left join inst_data
                on inst_data.lecture_id = l.lecture_id
            left join users_info ui
                on ui.user_id = cum.user_id
            left join course_user_category_mapping cucm
                on cucm.user_id = cum.user_id and cucm.course_id = cum.course_id
            left join lecture_understanding
                on lecture_understanding.lecture_id = l.lecture_id 
                    and lecture_understanding.user_id = cum.user_id 
            left join lecture_rating
                on lecture_rating.lecture_id = l.lecture_id 
                    and lecture_rating.user_id = cum.user_id
            left join user_activity_status_mapping uasm 
                on uasm.user_id = cum.user_id 
            left join users_info ui2 
                on ui2.user_id = inst_data.inst_user_id
            group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,27,28,29,30,31,32,33,34,35,36,37,38,39) count_query;
        ''' % (start_lecture_id, end_lecture_id),
    )


# Python Limit Offset generator
def limit_offset_generator_func(**kwargs):
    ti = kwargs['ti']
    current_lecture_sub_dag_id = kwargs['current_lecture_sub_dag_id']
    current_cps_sub_dag_id = kwargs['current_cps_sub_dag_id']
    count_cps_rows = ti.xcom_pull(
        task_ids=f'transforming_data_{current_lecture_sub_dag_id}.number_of_rows_per_lecture_sub_dag')
    print(count_cps_rows)
    total_count_rows = count_cps_rows[0][0]
    return {
        "limit": total_count_rows // total_number_of_extraction_cps_dags,
        "offset": current_cps_sub_dag_id * (total_count_rows // total_number_of_extraction_cps_dags) + 1,
    }


def transform_data_per_query(start_lecture_id, end_lecture_id, cps_sub_dag_id, current_lecture_sub_dag_id):
    return PostgresOperator(
        task_id='transform_data',
        postgres_conn_id='postgres_result_db',
        dag=dag,
        params={
            'current_cps_sub_dag_id': cps_sub_dag_id,
            'current_lecture_sub_dag_id': current_lecture_sub_dag_id,
            'task_key': f'transforming_data_{current_lecture_sub_dag_id}.extract_and_transform_individual_lecture_sub_dag_{current_lecture_sub_dag_id}_cps_sub_dag_{cps_sub_dag_id}.limit_offset_generator'
        },
        sql=''' with user_raw_data as
            (select
                lecture_id,
                course_user_mapping_id,
                join_time,
                leave_time,
                extract(epoch from (leave_time - join_time))/60 as time_diff,
                user_type,
                overlapping_time_seconds,
                overlapping_time_minutes
            from
                lecture_engagement_time let
            where lower(user_type) like 'user'
            group by 1,2,3,4,5,6,7,8),

        inst_raw_data as
            (select 
                lecture_id,
                user_id as inst_user_id, 
                let.course_user_mapping_id as inst_cum_id,
                join_time,
                leave_time,
                extract('epoch' from (leave_time - join_time))/60 as time_diff_in_mins
            from
                lecture_engagement_time let
            join course_user_mapping cum 
                on cum.course_user_mapping_id = let.course_user_mapping_id 
            where lower(user_type) like 'instructor'
            group by 1,2,3,4,5,6),

        inst_data as 
            (select 
                lecture_id,
                inst_user_id,
                inst_cum_id,
                min(join_time) as inst_min_join_time,
                max(leave_time) as inst_max_leave_time,
                sum(time_diff_in_mins) as inst_total_time_in_mins
            from
                inst_raw_data
            group by 1,2,3),

        user_overlapping_time as
            (select 
                lecture_id,
                course_user_mapping_id,
                min(join_time) as min_join_time,
                max(leave_time) as max_leave_time,
                sum(overlapping_time_seconds) as total_overlapping_in_seconds,
                sum(overlapping_time_minutes) as total_overlapping_time_minutes,
                sum(time_diff) as total_user_time
            from
                user_raw_data
            group by 1,2),

        lecture_rating as 
            (select
                user_id,
                entity_object_id as lecture_id,
                case
                    when ffar.feedback_answer = 'Awesome' then 5
                    when ffar.feedback_answer = 'Good' then 4
                    when ffar.feedback_answer = 'Average' then 3
                    when ffar.feedback_answer = 'Poor' then 2
                    when ffar.feedback_answer = 'Very Poor' then 1
                end as answer_rating,
                feedback_answer as rating_feedback_answer
            from
                feedback_form_all_responses_new ffar
            where ffar.feedback_form_id = 4377 
                and ffar.feedback_question_id = 348
            group by 1,2,3,4),

        lecture_understanding as 
            (select
                    user_id,
                    entity_object_id as lecture_id,
                case 
                    when feedback_answer_id = 179 then 1
                    when feedback_answer_id = 180 then 0
                    when feedback_answer_id = 181 then -1
                end as lecture_understood_rating,
                    feedback_answer as lecture_understanding_feedback_answer
                from
                    feedback_form_all_responses_new ffar
                where ffar.feedback_form_id = 4377 
                    and ffar.feedback_question_id = 331
                group by 1,2,3,4)    

        select
            concat(cum.user_id, l.lecture_id) as table_unique_key,
            cum.user_id,
            concat(ui.first_name,' ',ui.last_name) as student_name,
            ui.lead_type,
            cucm.student_category,
            cum.course_user_mapping_id,
            case 
                when cum.label_id is null and cum.status in (8) then 'Enrolled Student'
                when cum.label_id is null and cum.status in (1) then 'Applied'
                when cum.label_id is not null and cum.status in (8) then 'Label Marked Student'
                when c.course_structure_id in (1,18) and cum.status in (11,12) then 'ISA Cancelled Student'
                when c.course_structure_id not in (1,18) and cum.status in (30) then 'Deferred Student'
                when c.course_structure_id not in (1,18) and cum.status in (11) then 'Foreclosed Student'
                when c.course_structure_id not in (1,18) and cum.status in (12) then 'Reject by NS-Ops'
                else 'Mapping Error'
            end as label_mapping_status,
            c.course_id,
            c.course_name,
            c.course_structure_class,
            l.lecture_id,
            l.lecture_title,
            l.lecture_type,
            l.mandatory,
            l.start_timestamp as lecture_start_timestamp,
            inst_min_join_time,
            inst_max_leave_time,
            cast(inst_total_time_in_mins as int) as inst_total_time_in_mins,
            inst_user_id,
            concat(ui2.first_name,' ' ,ui2.last_name) as instructor_name,
            date(l.start_timestamp) as lecture_date,
            count(distinct let.lecture_id) filter (where let.lecture_id is not null) as live_attendance,
            count(distinct rlcur.lecture_id) filter (where rlcur.id is not null) as recorded_attendance,
            case
                when (count(distinct let.lecture_id) filter (where let.lecture_id is not null) = 1 or 
                    count(distinct rlcur.lecture_id) filter (where rlcur.id is not null) = 1) then 1
                else 0 
            end as overall_attendance,
            cast(let.total_overlapping_time_minutes as int) as total_overlapping_time_in_mins,
            cast(let.total_user_time as int) as total_user_time ,
            let.min_join_time as user_min_join_time,
            let.max_leave_time as user_max_leave_time,
            answer_rating,
            rating_feedback_answer,
            lecture_understood_rating,
            lecture_understanding_feedback_answer,
            uasm.activity_status_7_days,
            uasm.activity_status_14_days,
            uasm.activity_status_30_days,
            cum.user_placement_status,
            cum.admin_course_id,
            cum.admin_unit_name,
            l.child_video_session 
        from
            courses c
        join course_user_mapping cum
            on cum.course_id = c.course_id and c.course_structure_id in (82,83,130,134,135,168,169,170,171)
                    and cum.status in (1,8,10,11,12,30)
        join lectures l
            on l.course_id = c.course_id and l.start_timestamp >= '2025-03-01'
                and (l.lecture_id between %d and %d)
        left join user_overlapping_time let
            on let.lecture_id = l.lecture_id and let.course_user_mapping_id = cum.course_user_mapping_id
        left join recorded_lectures_course_user_reports rlcur
            on rlcur.lecture_id = l.lecture_id and rlcur.course_user_mapping_id = cum.course_user_mapping_id
        left join inst_data
            on inst_data.lecture_id = l.lecture_id
        left join users_info ui
            on ui.user_id = cum.user_id
        left join course_user_category_mapping cucm
            on cucm.user_id = cum.user_id and cucm.course_id = cum.course_id
        left join lecture_understanding
            on lecture_understanding.lecture_id = l.lecture_id 
                and lecture_understanding.user_id = cum.user_id 
        left join lecture_rating
            on lecture_rating.lecture_id = l.lecture_id 
                and lecture_rating.user_id = cum.user_id
        left join user_activity_status_mapping uasm 
            on uasm.user_id = cum.user_id 
        left join users_info ui2 
            on ui2.user_id = inst_data.inst_user_id
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,27,28,29,30,31,32,33,34,35,36,37,38,39;
            ''' % (start_lecture_id, end_lecture_id),
    )


for lecture_sub_dag_id in range(int(total_number_of_sub_dags)):
    with TaskGroup(group_id=f"transforming_data_{lecture_sub_dag_id}", dag=dag) as lecture_sub_dag_task_group:
        lecture_start_id = lecture_sub_dag_id * int(lecture_per_dags) + 1
        lecture_end_id = (lecture_sub_dag_id + 1) * int(lecture_per_dags)
        number_of_rows_per_lecture_sub_dag = number_of_rows_per_lecture_sub_dag_func(lecture_start_id,
                                                                                     lecture_end_id)

        for cps_sub_dag_id in range(int(total_number_of_extraction_cps_dags)):
            with TaskGroup(
                    group_id=f"extract_and_transform_individual_lecture_sub_dag_{lecture_sub_dag_id}_cps_sub_dag_{cps_sub_dag_id}",
                    dag=dag) as cps_sub_dag:
                limit_offset_generator = PythonOperator(
                    task_id='limit_offset_generator',
                    python_callable=limit_offset_generator_func,
                    provide_context=True,
                    op_kwargs={
                        'current_lecture_sub_dag_id': lecture_sub_dag_id,
                        'current_cps_sub_dag_id': cps_sub_dag_id,
                    },
                    dag=dag,
                )

                transform_data = transform_data_per_query(lecture_start_id, lecture_end_id, cps_sub_dag_id,
                                                          lecture_sub_dag_id)

                extract_python_data = PythonOperator(
                    task_id='extract_python_data',
                    python_callable=extract_data_to_nested,
                    provide_context=True,
                    op_kwargs={
                        'current_lecture_sub_dag_id': lecture_sub_dag_id,
                        'current_cps_sub_dag_id': cps_sub_dag_id
                    },
                    dag=dag,
                )

                limit_offset_generator >> transform_data >> extract_python_data

            number_of_rows_per_lecture_sub_dag >> cps_sub_dag

    create_table >> lecture_sub_dag_task_group