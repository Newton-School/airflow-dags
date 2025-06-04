from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import logging

default_args = {
    'owner': 'airflow',
    'max_active_tasks': 6,
    'max_active_runs': 6,
    'concurrency': 4,
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 3),
    'retries': 1,
}

# Get variables with proper defaults and type conversion
lecture_per_dags = int(Variable.get("lecture_per_dag", default_var=4000))
total_number_of_sub_dags = int(Variable.get("total_number_of_sub_dags", default_var=5))
total_number_of_extraction_cps_dags = int(Variable.get("total_number_of_extraction_cps_dags", default_var=10))

dag = DAG(
    'lecture_cur_growth_v2',
    default_args=default_args,
    concurrency=4,
    max_active_tasks=6,
    max_active_runs=6,
    description='Table at user, lecture level for all users with cum.status in (8,9,11,12,30)',
    schedule_interval='35 3 * * *',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_result_db',
    sql='''CREATE TABLE IF NOT EXISTS lecture_cur_growth_v2 (
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
            course_structure_id int,
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


def extract_data_to_nested(**kwargs):
    """Extract and insert transformed data into the target table"""
    pg_hook = PostgresHook(postgres_conn_id='postgres_result_db')
    pg_conn = pg_hook.get_conn()

    try:
        ti = kwargs['ti']
        current_lecture_sub_dag_id = kwargs['current_lecture_sub_dag_id']
        current_cps_sub_dag_id = kwargs['current_cps_sub_dag_id']

        # Pull transformed data from XCom
        transform_data_output = ti.xcom_pull(
            task_ids=f'transforming_data_{current_lecture_sub_dag_id}.extract_and_transform_individual_lecture_sub_dag_{current_lecture_sub_dag_id}_cps_sub_dag_{current_cps_sub_dag_id}.transform_data'
        )

        if not transform_data_output:
            logging.warning(
                f"No data received from transform task for lecture_sub_dag_{current_lecture_sub_dag_id}, cps_sub_dag_{current_cps_sub_dag_id}")
            return

        logging.info(f"Processing {len(transform_data_output)} rows")

        pg_cursor = pg_conn.cursor()

        # Use executemany for better performance
        insert_query = '''INSERT INTO lecture_cur_growth_v2 (
            table_unique_key, user_id, student_name, lead_type, student_category, 
            course_user_mapping_id, label_mapping_status, course_id, course_name,
            course_structure_id, course_structure_class, lecture_id, lecture_title,
            lecture_type, mandatory, lecture_start_timestamp, inst_min_join_time, 
            inst_max_leave_time, inst_total_time_in_mins, inst_user_id, instructor_name,
            lecture_date, live_attendance, recorded_attendance, overall_attendance, 
            total_overlapping_time_in_mins, total_user_time, user_min_join_time,
            user_max_leave_time, answer_rating, rating_feedback_answer,
            lecture_understood_rating, lecture_understanding_feedback_answer,
            activity_status_7_days, activity_status_14_days, activity_status_30_days,
            user_placement_status, admin_course_id, admin_unit_name, child_video_session
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (table_unique_key) DO UPDATE SET 
            student_name = EXCLUDED.student_name,
            lead_type = EXCLUDED.lead_type,
            student_category = EXCLUDED.student_category,
            label_mapping_status = EXCLUDED.label_mapping_status,
            course_name = EXCLUDED.course_name,
            course_structure_id = EXCLUDED.course_structure_id,
            course_structure_class = EXCLUDED.course_structure_class,
            lecture_title = EXCLUDED.lecture_title,
            lecture_type = EXCLUDED.lecture_type,
            mandatory = EXCLUDED.mandatory,
            lecture_start_timestamp = EXCLUDED.lecture_start_timestamp,
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
            child_video_session = EXCLUDED.child_video_session;'''

        # Convert data to list of tuples for executemany
        data_tuples = [tuple(row) for row in transform_data_output]

        pg_cursor.executemany(insert_query, data_tuples)
        pg_conn.commit()

        logging.info(f"Successfully inserted/updated {len(data_tuples)} rows")

    except Exception as e:
        logging.error(f"Error in extract_data_to_nested: {str(e)}")
        pg_conn.rollback()
        raise
    finally:
        if 'pg_cursor' in locals():
            pg_cursor.close()
        pg_conn.close()


def number_of_rows_per_lecture_sub_dag_func(start_lecture_id, end_lecture_id):
    """Get count of rows for a specific lecture range"""
    return PostgresOperator(
        task_id='number_of_rows_per_lecture_sub_dag',
        postgres_conn_id='postgres_result_db',
        dag=dag,
        sql=f'''SELECT COUNT(*) as row_count FROM (
            WITH user_raw_data AS (
                SELECT
                    lecture_id,
                    course_user_mapping_id,
                    join_time,
                    leave_time,
                    EXTRACT(epoch FROM (leave_time - join_time))/60 AS time_diff,
                    user_type,
                    overlapping_time_seconds,
                    overlapping_time_minutes
                FROM lecture_engagement_time let
                WHERE LOWER(user_type) LIKE 'user'
                GROUP BY 1,2,3,4,5,6,7,8
            ),

            inst_raw_data AS (
                SELECT 
                    lecture_id,
                    user_id AS inst_user_id, 
                    let.course_user_mapping_id AS inst_cum_id,
                    join_time,
                    leave_time,
                    EXTRACT('epoch' FROM (leave_time - join_time))/60 AS time_diff_in_mins
                FROM lecture_engagement_time let
                JOIN course_user_mapping cum 
                    ON cum.course_user_mapping_id = let.course_user_mapping_id 
                WHERE LOWER(user_type) LIKE 'instructor'
                GROUP BY 1,2,3,4,5,6
            ),

            inst_data AS (
                SELECT 
                    lecture_id,
                    inst_user_id,
                    inst_cum_id,
                    MIN(join_time) AS inst_min_join_time,
                    MAX(leave_time) AS inst_max_leave_time,
                    SUM(time_diff_in_mins) AS inst_total_time_in_mins
                FROM inst_raw_data
                GROUP BY 1,2,3
            ),

            user_overlapping_time AS (
                SELECT 
                    lecture_id,
                    course_user_mapping_id,
                    MIN(join_time) AS min_join_time,
                    MAX(leave_time) AS max_leave_time,
                    SUM(overlapping_time_seconds) AS total_overlapping_in_seconds,
                    SUM(overlapping_time_minutes) AS total_overlapping_time_minutes,
                    SUM(time_diff) AS total_user_time
                FROM user_raw_data
                GROUP BY 1,2
            )

            SELECT DISTINCT
                CONCAT(cum.user_id, l.lecture_id) AS table_unique_key
            FROM courses c
            JOIN course_user_mapping cum
                ON cum.course_id = c.course_id 
                AND c.course_structure_id IN (82,83,130,134,135,168,169,170,171)
                AND cum.status IN (1,8,10,11,12,30) 
            JOIN lectures l
                ON l.course_id = c.course_id 
                AND l.start_timestamp >= '2025-01-01'
                AND l.lecture_id BETWEEN {start_lecture_id} AND {end_lecture_id}
        ) count_query;''',
    )


def limit_offset_generator_func(**kwargs):
    """Generate limit and offset for pagination"""
    try:
        ti = kwargs['ti']
        current_lecture_sub_dag_id = kwargs['current_lecture_sub_dag_id']
        current_cps_sub_dag_id = kwargs['current_cps_sub_dag_id']

        count_cps_rows = ti.xcom_pull(
            task_ids=f'transforming_data_{current_lecture_sub_dag_id}.number_of_rows_per_lecture_sub_dag'
        )

        if not count_cps_rows or len(count_cps_rows) == 0:
            logging.warning("No count data received, using default values")
            total_count_rows = 1000  # Default fallback
        else:
            total_count_rows = count_cps_rows[0][0]

        logging.info(f"Total rows: {total_count_rows}, Sub DAGs: {total_number_of_extraction_cps_dags}")

        if total_count_rows == 0:
            return {"limit": 0, "offset": 0}

        rows_per_dag = max(1, total_count_rows // total_number_of_extraction_cps_dags)
        offset = current_cps_sub_dag_id * rows_per_dag

        return {
            "limit": rows_per_dag,
            "offset": offset,
        }

    except Exception as e:
        logging.error(f"Error in limit_offset_generator_func: {str(e)}")
        return {"limit": 1000, "offset": 0}  # Fallback values


def transform_data_per_query(start_lecture_id, end_lecture_id, cps_sub_dag_id, current_lecture_sub_dag_id):
    """Transform data for a specific lecture range with pagination"""
    return PostgresOperator(
        task_id='transform_data',
        postgres_conn_id='postgres_result_db',
        dag=dag,
        sql=f'''WITH user_raw_data AS (
            SELECT
                lecture_id,
                course_user_mapping_id,
                join_time,
                leave_time,
                EXTRACT(epoch FROM (leave_time - join_time))/60 AS time_diff,
                user_type,
                overlapping_time_seconds,
                overlapping_time_minutes
            FROM lecture_engagement_time let
            WHERE LOWER(user_type) LIKE 'user'
            GROUP BY 1,2,3,4,5,6,7,8
        ),

        inst_raw_data AS (
            SELECT 
                lecture_id,
                user_id AS inst_user_id, 
                let.course_user_mapping_id AS inst_cum_id,
                join_time,
                leave_time,
                EXTRACT('epoch' FROM (leave_time - join_time))/60 AS time_diff_in_mins
            FROM lecture_engagement_time let
            JOIN course_user_mapping cum 
                ON cum.course_user_mapping_id = let.course_user_mapping_id 
            WHERE LOWER(user_type) LIKE 'instructor'
            GROUP BY 1,2,3,4,5,6
        ),

        inst_data AS (
            SELECT 
                lecture_id,
                inst_user_id,
                inst_cum_id,
                MIN(join_time) AS inst_min_join_time,
                MAX(leave_time) AS inst_max_leave_time,
                SUM(time_diff_in_mins) AS inst_total_time_in_mins
            FROM inst_raw_data
            GROUP BY 1,2,3
        ),

        user_overlapping_time AS (
            SELECT 
                lecture_id,
                course_user_mapping_id,
                MIN(join_time) AS min_join_time,
                MAX(leave_time) AS max_leave_time,
                SUM(overlapping_time_seconds) AS total_overlapping_in_seconds,
                SUM(overlapping_time_minutes) AS total_overlapping_time_minutes,
                SUM(time_diff) AS total_user_time
            FROM user_raw_data
            GROUP BY 1,2
        ),

        lecture_rating AS (
            SELECT
                user_id,
                entity_object_id AS lecture_id,
                CASE
                    WHEN ffar.feedback_answer = 'Awesome' THEN 5
                    WHEN ffar.feedback_answer = 'Good' THEN 4
                    WHEN ffar.feedback_answer = 'Average' THEN 3
                    WHEN ffar.feedback_answer = 'Poor' THEN 2
                    WHEN ffar.feedback_answer = 'Very Poor' THEN 1
                END AS answer_rating,
                feedback_answer AS rating_feedback_answer
            FROM feedback_form_all_responses_new ffar
            WHERE ffar.feedback_form_id = 4377 
                AND ffar.feedback_question_id = 348
            GROUP BY 1,2,3,4
        ),

        lecture_understanding AS (
            SELECT
                user_id,
                entity_object_id AS lecture_id,
                CASE 
                    WHEN feedback_answer_id = 179 THEN 1
                    WHEN feedback_answer_id = 180 THEN 0
                    WHEN feedback_answer_id = 181 THEN -1
                END AS lecture_understood_rating,
                feedback_answer AS lecture_understanding_feedback_answer
            FROM feedback_form_all_responses_new ffar
            WHERE ffar.feedback_form_id = 4377 
                AND ffar.feedback_question_id = 331
            GROUP BY 1,2,3,4
        )    

        SELECT
            CONCAT(cum.user_id, l.lecture_id) AS table_unique_key,
            cum.user_id,
            CONCAT(ui.first_name,' ',ui.last_name) AS student_name,
            ui.lead_type,
            cucm.student_category,
            cum.course_user_mapping_id,
            CASE 
                WHEN cum.label_id IS NULL AND cum.status IN (8,10) THEN 'Enrolled Student'
                WHEN cum.label_id IS NOT NULL AND cum.status IN (8,10) THEN 'Label Marked Student'
                WHEN cum.label_id IS NULL AND cum.status IN (1) THEN 'Applied'
                WHEN cum.status IN (30) THEN 'Deferred Student'
                WHEN cum.status IN (11) THEN 'Foreclosed Student'
                WHEN cum.status IN (12) THEN 'Reject by NS-Ops'
                ELSE 'Mapping Error'
            END AS label_mapping_status,
            c.course_id,
            c.course_name,
            c.course_structure_id,
            c.course_structure_class,
            l.lecture_id,
            l.lecture_title,
            l.lecture_type,
            l.mandatory,
            l.start_timestamp AS lecture_start_timestamp,
            inst_min_join_time,
            inst_max_leave_time,
            CAST(inst_total_time_in_mins AS int) AS inst_total_time_in_mins,
            inst_user_id,
            CONCAT(ui2.first_name,' ' ,ui2.last_name) AS instructor_name,
            DATE(l.start_timestamp) AS lecture_date,
            COUNT(DISTINCT let.lecture_id) FILTER (WHERE let.lecture_id IS NOT NULL) AS live_attendance,
            COUNT(DISTINCT rlcur.lecture_id) FILTER (WHERE rlcur.id IS NOT NULL) AS recorded_attendance,
            CASE
                WHEN (COUNT(DISTINCT let.lecture_id) FILTER (WHERE let.lecture_id IS NOT NULL) = 1 OR 
                    COUNT(DISTINCT rlcur.lecture_id) FILTER (WHERE rlcur.id IS NOT NULL) = 1) THEN 1
                ELSE 0 
            END AS overall_attendance,
            CAST(let.total_overlapping_time_minutes AS int) AS total_overlapping_time_in_mins,
            CAST(let.total_user_time AS int) AS total_user_time,
            let.min_join_time AS user_min_join_time,
            let.max_leave_time AS user_max_leave_time,
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
        FROM courses c
        JOIN course_user_mapping cum
            ON cum.course_id = c.course_id 
            AND c.course_structure_id IN (82,83,130,134,135,168,169,170,171)
            AND cum.status IN (1,8,10,11,12,30) 
        JOIN lectures l
            ON l.course_id = c.course_id 
            AND l.start_timestamp >= '2025-01-01'
            AND l.lecture_id BETWEEN {start_lecture_id} AND {end_lecture_id}
        LEFT JOIN user_overlapping_time let
            ON let.lecture_id = l.lecture_id 
            AND let.course_user_mapping_id = cum.course_user_mapping_id
        LEFT JOIN recorded_lectures_course_user_reports rlcur
            ON rlcur.lecture_id = l.lecture_id 
            AND rlcur.course_user_mapping_id = cum.course_user_mapping_id
        LEFT JOIN inst_data
            ON inst_data.lecture_id = l.lecture_id
        LEFT JOIN users_info ui
            ON ui.user_id = cum.user_id
        LEFT JOIN course_user_category_mapping cucm
            ON cucm.user_id = cum.user_id 
            AND cucm.course_id = cum.course_id
        LEFT JOIN lecture_understanding
            ON lecture_understanding.lecture_id = l.lecture_id 
            AND lecture_understanding.user_id = cum.user_id 
        LEFT JOIN lecture_rating
            ON lecture_rating.lecture_id = l.lecture_id 
            AND lecture_rating.user_id = cum.user_id
        LEFT JOIN user_activity_status_mapping uasm 
            ON uasm.user_id = cum.user_id 
        LEFT JOIN users_info ui2 
            ON ui2.user_id = inst_data.inst_user_id
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,27,28,29,30,31,32,33,34,35,36,37,38,39,40
        ORDER BY table_unique_key
        LIMIT {{{{ ti.xcom_pull(task_ids='transforming_data_{current_lecture_sub_dag_id}.extract_and_transform_individual_lecture_sub_dag_{current_lecture_sub_dag_id}_cps_sub_dag_{cps_sub_dag_id}.limit_offset_generator')['limit'] }}}}
        OFFSET {{{{ ti.xcom_pull(task_ids='transforming_data_{current_lecture_sub_dag_id}.extract_and_transform_individual_lecture_sub_dag_{current_lecture_sub_dag_id}_cps_sub_dag_{cps_sub_dag_id}.limit_offset_generator')['offset'] }}}};''',
    )


# Create task groups for processing different lecture ranges
for lecture_sub_dag_id in range(total_number_of_sub_dags):
    with TaskGroup(group_id=f"transforming_data_{lecture_sub_dag_id}", dag=dag) as lecture_sub_dag_task_group:
        lecture_start_id = lecture_sub_dag_id * lecture_per_dags + 1
        lecture_end_id = (lecture_sub_dag_id + 1) * lecture_per_dags

        # Get row count for this lecture range
        number_of_rows_per_lecture_sub_dag = number_of_rows_per_lecture_sub_dag_func(
            lecture_start_id, lecture_end_id
        )

        # Create sub-DAGs for parallel processing within each lecture range
        for cps_sub_dag_id in range(total_number_of_extraction_cps_dags):
            with TaskGroup(
                    group_id=f"extract_and_transform_individual_lecture_sub_dag_{lecture_sub_dag_id}_cps_sub_dag_{cps_sub_dag_id}",
                    dag=dag
            ) as cps_sub_dag:
                # Generate pagination parameters
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

                # Transform data with pagination
                transform_data = transform_data_per_query(
                    lecture_start_id, lecture_end_id, cps_sub_dag_id, lecture_sub_dag_id
                )

                # Extract and load data
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

                # Set task dependencies within the sub-DAG
                number_of_rows_per_lecture_sub_dag >> limit_offset_generator >> transform_data >> extract_python_data

    # Set dependency: table creation must complete before any data processing
    create_table >> lecture_sub_dag_task_group