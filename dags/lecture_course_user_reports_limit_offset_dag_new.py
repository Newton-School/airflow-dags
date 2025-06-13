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
    'start_date': datetime(2023, 3, 16),
}

lecture_per_dags = Variable.get("lecture_per_dag", 4000)

total_number_of_sub_dags = Variable.get("total_number_of_sub_dags", 5)

total_number_of_extraction_cps_dags = Variable.get("total_number_of_extraction_cps_dags", 10)

dag = DAG(
    'lecture_course_user_reports_bigserial_limit_offset_dag',
    default_args=default_args,
    concurrency=4,
    max_active_tasks=6,
    max_active_runs=6,
    description='Table at user, lecture level for all users with cum.status in (8,9,11,12,30)',
    schedule_interval='45 5 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS lecture_course_user_reports_bigserial (
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
            topic_template_id int,
            template_name text,
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
    import logging
    import traceback

    logger = logging.getLogger(__name__)
    logger.info("Starting extract_data_to_nested function")

    pg_hook = None
    pg_conn = None

    try:
        # Get task instance and parameters
        ti = kwargs['ti']
        current_lecture_sub_dag_id = kwargs['current_lecture_sub_dag_id']
        current_cps_sub_dag_id = kwargs['current_cps_sub_dag_id']

        logger.info(
            f"Processing lecture_sub_dag_id: {current_lecture_sub_dag_id}, cps_sub_dag_id: {current_cps_sub_dag_id}")

        # Pull data from XCom
        task_id = f'transforming_data_{current_lecture_sub_dag_id}.extract_and_transform_individual_lecture_sub_dag_{current_lecture_sub_dag_id}_cps_sub_dag_{current_cps_sub_dag_id}.transform_data'
        logger.info(f"Pulling XCom data from task_id: {task_id}")

        transform_data_output = ti.xcom_pull(task_ids=task_id)

        # Validate XCom data
        if transform_data_output is None:
            logger.warning("No data received from XCom - transform_data_output is None")
            return "No data to process"

        logger.info(
            f"Received {len(transform_data_output) if hasattr(transform_data_output, '__len__') else 'unknown'} rows from transform task")

        # Initialize database connection
        logger.info("Establishing database connection")
        pg_hook = PostgresHook(postgres_conn_id='postgres_result_db')
        pg_conn = pg_hook.get_conn()

        # Process data in batches for better performance
        batch_size = 1000
        processed_rows = 0

        for i, transform_row in enumerate(transform_data_output):
            try:
                # Validate row data
                if transform_row is None:
                    logger.warning(f"Skipping row {i}: row is None")
                    continue

                if len(transform_row) != 41:
                    logger.warning(f"Skipping row {i}: expected 41 columns, got {len(transform_row)}")
                    continue

                pg_cursor = pg_conn.cursor()

                # Execute INSERT with error handling
                pg_cursor.execute('''
                    INSERT INTO lecture_course_user_reports_bigserial (
                        table_unique_key, user_id, student_name, lead_type, student_category, 
                        course_user_mapping_id, label_mapping_status, course_id, course_name, 
                        course_structure_class, lecture_id, lecture_title, lecture_type, mandatory, 
                        lecture_start_timestamp, topic_template_id, template_name, inst_min_join_time, 
                        inst_max_leave_time, inst_total_time_in_mins, inst_user_id, instructor_name,
                        lecture_date, live_attendance, recorded_attendance, overall_attendance, 
                        total_overlapping_time_in_mins, total_user_time, user_min_join_time,
                        user_max_leave_time, answer_rating, rating_feedback_answer,
                        lecture_understood_rating, lecture_understanding_feedback_answer,
                        activity_status_7_days, activity_status_14_days, activity_status_30_days,
                        user_placement_status, admin_course_id, admin_unit_name, child_video_session
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (table_unique_key) DO UPDATE SET 
                        student_name = EXCLUDED.student_name,
                        lead_type = EXCLUDED.lead_type,
                        student_category = EXCLUDED.student_category,
                        label_mapping_status = EXCLUDED.label_mapping_status,
                        course_name = EXCLUDED.course_name,
                        course_structure_class = EXCLUDED.course_structure_class,
                        lecture_title = EXCLUDED.lecture_title,
                        lecture_type = EXCLUDED.lecture_type,
                        mandatory = EXCLUDED.mandatory,
                        lecture_start_timestamp = EXCLUDED.lecture_start_timestamp,
                        topic_template_id = EXCLUDED.topic_template_id,
                        template_name = EXCLUDED.template_name,
                        inst_min_join_time = EXCLUDED.inst_min_join_time,
                        inst_max_leave_time = EXCLUDED.inst_max_leave_time,
                        inst_total_time_in_mins = EXCLUDED.inst_total_time_in_mins,
                        inst_user_id = EXCLUDED.inst_user_id,
                        instructor_name = EXCLUDED.instructor_name,
                        lecture_date = EXCLUDED.lecture_date,
                        live_attendance = EXCLUDED.live_attendance,
                        recorded_attendance = EXCLUDED.recorded_attendance,
                        overall_attendance = EXCLUDED.overall_attendance,
                        total_overlapping_time_in_mins = EXCLUDED.total_overlapping_time_in_mins,
                        total_user_time = EXCLUDED.total_user_time,
                        user_min_join_time = EXCLUDED.user_min_join_time,
                        user_max_leave_time = EXCLUDED.user_max_leave_time,
                        answer_rating = EXCLUDED.answer_rating,
                        rating_feedback_answer = EXCLUDED.rating_feedback_answer,
                        lecture_understood_rating = EXCLUDED.lecture_understood_rating,
                        lecture_understanding_feedback_answer = EXCLUDED.lecture_understanding_feedback_answer,
                        activity_status_7_days = EXCLUDED.activity_status_7_days,
                        activity_status_14_days = EXCLUDED.activity_status_14_days,
                        activity_status_30_days = EXCLUDED.activity_status_30_days,
                        user_placement_status = EXCLUDED.user_placement_status,
                        admin_course_id = EXCLUDED.admin_course_id,
                        admin_unit_name = EXCLUDED.admin_unit_name,
                        child_video_session = EXCLUDED.child_video_session
                ''', tuple(transform_row))

                processed_rows += 1

                # Commit in batches
                if processed_rows % batch_size == 0:
                    pg_conn.commit()
                    logger.info(f"Committed batch: {processed_rows} rows processed")

                pg_cursor.close()

            except Exception as row_error:
                logger.error(f"Error processing row {i}: {str(row_error)}")
                logger.error(f"Row data: {transform_row}")
                if 'pg_cursor' in locals():
                    pg_cursor.close()
                # Continue processing other rows
                continue

        # Final commit
        pg_conn.commit()
        logger.info(f"Successfully processed {processed_rows} rows")

        return f"Successfully processed {processed_rows} rows"

    except Exception as e:
        logger.error(f"Error in extract_data_to_nested: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        if pg_conn:
            pg_conn.rollback()
        raise e

    finally:
        # Clean up connections
        if pg_conn:
            pg_conn.close()
            logger.info("Database connection closed")


def number_of_rows_per_lecture_sub_dag_func(start_lecture_id, end_lecture_id):
    return PostgresOperator(
        task_id='number_of_rows_per_lecture_sub_dag',
        postgres_conn_id='postgres_result_db',
        dag=dag,
        sql='''select count(table_unique_key) from
            (with user_raw_data as (
            select
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
            group by 1,2,3,4,5,6,7,8
        ),

        inst_raw_data as (
            select 
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
            group by 1,2,3,4,5,6
        ),

        inst_data as (
            select 
                lecture_id,
                inst_user_id,
                inst_cum_id,
                min(join_time) as inst_min_join_time,
                max(leave_time) as inst_max_leave_time,
                sum(time_diff_in_mins) as inst_total_time_in_mins
            from
                inst_raw_data
            group by 1,2,3
        ),

        user_overlapping_time as (
            select 
                lecture_id,
                course_user_mapping_id,
                min(join_time) as min_join_time,
                max(leave_time) as max_leave_time,
                sum(overlapping_time_seconds) as total_overlapping_in_seconds,
                sum(overlapping_time_minutes) as total_overlapping_time_minutes,
                sum(time_diff) as total_user_time
            from
                user_raw_data
            group by 1,2
        ),

        lecture_rating as (
            select
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
            group by 1,2,3,4
        ),

        lecture_understanding as (
            select
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
            group by 1,2,3,4
        ),

        lecture_topics_aggregated as (
            select 
                ltm.lecture_id,
                array_agg(distinct t.topic_template_id) filter (where t.topic_template_id is not null) as topic_template_ids,
                array_agg(distinct t.template_name) filter (where t.template_name is not null) as template_names,
                count(distinct t.topic_template_id) filter (where t.topic_template_id is not null) as topic_count
            from lecture_topic_mapping ltm
            left join topics t 
                on t.topic_id = ltm.topic_id 
                and t.topic_template_id in (102,103,119,334,336,338,339,340,341,342,344,410,208,209,367,447,489,544,555,577,1232,1247,1490)
            -- Removed the completed = true filter as you commented it out
            group by ltm.lecture_id
        )

        select
            concat(cum.user_id, l.lecture_id, coalesce(array_to_string(lta.topic_template_ids, ','), '')) as table_unique_key,
            cum.user_id,
            concat(ui.first_name,' ',ui.last_name) as student_name,
            ui.lead_type,
            cucm.student_category,
            cum.course_user_mapping_id,
            case 
                when cum.label_id is null and cum.status in (8) then 'Enrolled Student'
                when cum.label_id is null and cum.status in (1) then 'Applied Student'
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
            lta.topic_template_ids,
            lta.template_names,
            coalesce(lta.topic_count, 0) as topic_count,

            inst_data.inst_min_join_time,
            inst_data.inst_max_leave_time,
            cast(inst_data.inst_total_time_in_mins as int) as inst_total_time_in_mins,
            inst_data.inst_user_id,
            concat(ui2.first_name,' ' ,ui2.last_name) as instructor_name,
            date(l.start_timestamp) as lecture_date,

            case when let.lecture_id is not null then 1 else 0 end as live_attendance,
            case when rlcur.lecture_id is not null then 1 else 0 end as recorded_attendance,
            case when (let.lecture_id is not null or rlcur.lecture_id is not null) then 1 else 0 end as overall_attendance,

            cast(let.total_overlapping_time_minutes as int) as total_overlapping_time_in_mins,
            cast(let.total_user_time as int) as total_user_time,
            let.min_join_time as user_min_join_time,
            let.max_leave_time as user_max_leave_time,
            lecture_rating.answer_rating,
            lecture_rating.rating_feedback_answer,
            lecture_understanding.lecture_understood_rating,
            lecture_understanding.lecture_understanding_feedback_answer,
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
            on cum.course_id = c.course_id 
            and c.course_structure_id in (1,6,7,8,11,12,14,18,19,20,22,23,26,32,34,44,47,50,51,52,53,54,55,56,57,58,59,60,72,127,118,119,122,121,94,95,131,132,82,83,130,134,135,168,169,170,171)
            and cum.status in (1,8,9,11,12,30) 
        join lectures l
            on l.course_id = c.course_id 
            and l.lecture_id BETWEEN %s AND %s
            and (l.lecture_id in (select lecture_id from recorded_lectures_course_user_reports where lecture_watch_date BETWEEN '2025-01-01' AND '2025-01-31')
                 or l.start_timestamp  BETWEEN '2025-01-01' AND '2025-01-31')
        left join user_overlapping_time let
            on let.lecture_id = l.lecture_id and let.course_user_mapping_id = cum.course_user_mapping_id
        left join recorded_lectures_course_user_reports rlcur
            on rlcur.lecture_id = l.lecture_id and rlcur.course_user_mapping_id = cum.course_user_mapping_id
        left join inst_data
            on inst_data.lecture_id = l.lecture_id
        left join lecture_topics_aggregated lta
            on lta.lecture_id = l.lecture_id
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
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41) count_query;
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
        sql=''' with user_raw_data as (
                select
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
                group by 1,2,3,4,5,6,7,8
            ),

            inst_raw_data as (
                select 
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
                group by 1,2,3,4,5,6
            ),

            inst_data as (
                select 
                    lecture_id,
                    inst_user_id,
                    inst_cum_id,
                    min(join_time) as inst_min_join_time,
                    max(leave_time) as inst_max_leave_time,
                    sum(time_diff_in_mins) as inst_total_time_in_mins
                from
                    inst_raw_data
                group by 1,2,3
            ),

            user_overlapping_time as (
                select 
                    lecture_id,
                    course_user_mapping_id,
                    min(join_time) as min_join_time,
                    max(leave_time) as max_leave_time,
                    sum(overlapping_time_seconds) as total_overlapping_in_seconds,
                    sum(overlapping_time_minutes) as total_overlapping_time_minutes,
                    sum(time_diff) as total_user_time
                from
                    user_raw_data
                group by 1,2
            ),

            lecture_rating as (
                select
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
                group by 1,2,3,4
            ),

            lecture_understanding as (
                select
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
                group by 1,2,3,4
            ),

            lecture_topics_aggregated as (
                select 
                    ltm.lecture_id,
                    array_agg(distinct t.topic_template_id) filter (where t.topic_template_id is not null) as topic_template_ids,
                    array_agg(distinct t.template_name) filter (where t.template_name is not null) as template_names,
                    count(distinct t.topic_template_id) filter (where t.topic_template_id is not null) as topic_count
                from lecture_topic_mapping ltm
                left join topics t 
                    on t.topic_id = ltm.topic_id 
                    and t.topic_template_id in (102,103,119,334,336,338,339,340,341,342,344,410,208,209,367,447,489,544,555,577,1232,1247,1490)
                -- Removed the completed = true filter as you commented it out
                group by ltm.lecture_id
            )

            select
                concat(cum.user_id, l.lecture_id, coalesce(array_to_string(lta.topic_template_ids, ','), '')) as table_unique_key,
                cum.user_id,
                concat(ui.first_name,' ',ui.last_name) as student_name,
                ui.lead_type,
                cucm.student_category,
                cum.course_user_mapping_id,
                case 
                    when cum.label_id is null and cum.status in (8) then 'Enrolled Student'
                    when cum.label_id is null and cum.status in (1) then 'Applied Student'
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
                lta.topic_template_ids,
                lta.template_names,
                coalesce(lta.topic_count, 0) as topic_count,

                inst_data.inst_min_join_time,
                inst_data.inst_max_leave_time,
                cast(inst_data.inst_total_time_in_mins as int) as inst_total_time_in_mins,
                inst_data.inst_user_id,
                concat(ui2.first_name,' ' ,ui2.last_name) as instructor_name,
                date(l.start_timestamp) as lecture_date,

                case when let.lecture_id is not null then 1 else 0 end as live_attendance,
                case when rlcur.lecture_id is not null then 1 else 0 end as recorded_attendance,
                case when (let.lecture_id is not null or rlcur.lecture_id is not null) then 1 else 0 end as overall_attendance,

                cast(let.total_overlapping_time_minutes as int) as total_overlapping_time_in_mins,
                cast(let.total_user_time as int) as total_user_time,
                let.min_join_time as user_min_join_time,
                let.max_leave_time as user_max_leave_time,
                lecture_rating.answer_rating,
                lecture_rating.rating_feedback_answer,
                lecture_understanding.lecture_understood_rating,
                lecture_understanding.lecture_understanding_feedback_answer,
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
                on cum.course_id = c.course_id 
                and c.course_structure_id in (1,6,7,8,11,12,14,18,19,20,22,23,26,32,34,44,47,50,51,52,53,54,55,56,57,58,59,60,72,127,118,119,122,121,94,95,131,132,82,83,130,134,135,168,169,170,171)
                and cum.status in (1,8,9,11,12,30) 
            join lectures l
                on l.course_id = c.course_id 
                and l.lecture_id BETWEEN %s AND %s
                and (l.lecture_id in (select lecture_id from recorded_lectures_course_user_reports where lecture_watch_date BETWEEN '2025-01-01' AND '2025-01-31')
                     or l.start_timestamp  BETWEEN '2025-01-01' AND '2025-01-31')
            left join user_overlapping_time let
                on let.lecture_id = l.lecture_id and let.course_user_mapping_id = cum.course_user_mapping_id
            left join recorded_lectures_course_user_reports rlcur
                on rlcur.lecture_id = l.lecture_id and rlcur.course_user_mapping_id = cum.course_user_mapping_id
            left join inst_data
                on inst_data.lecture_id = l.lecture_id
            left join lecture_topics_aggregated lta
                on lta.lecture_id = l.lecture_id
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
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41;
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