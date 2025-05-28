"""
Optimized DAG that fetches user information from newton_api, combines it with data from contact_alias,
and stores it in the 'results' database with improved performance and resource management.
"""

import logging
import pendulum
from typing import List, Dict, Optional, Any
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException
from psycopg2.extras import Json, execute_values

# Configuration constants
RESULT_DATABASE_CONNECTION_ID = "postgres_result_db"
SOURCE_DATABASE_CONNECTION_ID = "postgres_read_replica"
BATCH_SIZE = 1000
FETCH_BATCH_SIZE = 5000  # Batch size for fetching from source
logger = logging.getLogger(__name__)

# Table creation queries remain the same
CREATE_COURSE_STRUCTURE_X_USER_INFO_TABLE_QUERY = """
        CREATE TABLE IF NOT EXISTS course_structure_x_user_info (
            id BIGSERIAL PRIMARY KEY,
            course_structure_id BIGINT NOT NULL,
            unified_user_id INTEGER REFERENCES unified_user(id) ON DELETE CASCADE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            counter INTEGER DEFAULT 1,
            latest_counter_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            -- Apply form and generic form responses
            current_work TEXT,
            yearly_salary TEXT,
            bachelor_qualification TEXT,
            date_of_birth TEXT,
            twelfth_passing_marks TEXT,
            graduation_year TEXT,
            data_science_joining_reason TEXT,
            current_city TEXT,
            surety_on_learning_ds TEXT,
            how_soon_you_can_join TEXT,
            where_you_get_to_know_about_ns TEXT,
            work_experience TEXT,
            given_any_of_following_exam TEXT,
            department_worked_on TEXT,
            
            -- Unique constraint for course_structure_id and unified_user_id combination
            CONSTRAINT unique_course_structure_user UNIQUE (course_structure_id, unified_user_id)
        );
"""

CREATE_COURSE_X_USER_INFO_TABLE_QUERY = """
        CREATE TABLE IF NOT EXISTS course_x_user_info (
            id BIGSERIAL PRIMARY KEY,
            course_user_apply_form_mapping_id BIGINT,
            course_user_apply_form_mapping_created_at TIMESTAMP,
            course_user_mapping_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            course_user_mapping_utm_param_json JSONB,
            course_id BIGINT NOT NULL,
            coursestructure_slug VARCHAR(255),
            coursestructure_id BIGINT NOT NULL,
            course_user_timeline_flowmapping_id BIGINT,
            course_user_timeline_flow_mapping_course_timeline_flow INTEGER,
            courseusertimelineflow_mapping_apply_form_question_set INTEGER,
            course_user_timeline_flow_mapping_apply_form_version INTEGER,
            email VARCHAR(255),
            phone VARCHAR(20),
            
            -- Apply form question responses
            current_work TEXT,
            yearly_salary TEXT,
            bachelor_qualification TEXT,
            date_of_birth TEXT,
            twelfth_passing_marks TEXT,
            graduation_year TEXT,
            data_science_joining_reason TEXT,
            current_city TEXT,
            surety_on_learning_ds TEXT,
            how_soon_you_can_join TEXT,
            where_you_get_to_know_about_ns TEXT,
            work_experience TEXT,
            given_any_of_following_exam TEXT,
            department_worked_on TEXT,
            
            -- Aggregated statistics
            max_all_test_cases_passed INTEGER,
            max_assessment_marks DECIMAL(10,2),
            
            -- Metadata
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            unified_user_id INTEGER REFERENCES unified_user(id) ON DELETE SET NULL,
            course_structure_x_user_info_id BIGINT REFERENCES course_structure_x_user_info(id) ON DELETE SET NULL,
            
            -- Unique constraint for course_user_mapping_id to enable upsert
            CONSTRAINT unique_course_user_mapping_id UNIQUE (course_user_mapping_id)
        );
"""

CREATE_INDEXES_QUERY = """
        -- Index for course_structure_x_user_info lookups
        CREATE INDEX IF NOT EXISTS idx_course_structure_x_user_info_course_structure 
        ON course_structure_x_user_info(course_structure_id);
        
        CREATE INDEX IF NOT EXISTS idx_course_structure_x_user_info_unified_user 
        ON course_structure_x_user_info(unified_user_id);
        
        -- Index for course_x_user_info lookups
        CREATE INDEX IF NOT EXISTS idx_course_x_user_info_user_id 
        ON course_x_user_info(user_id);
        
        CREATE INDEX IF NOT EXISTS idx_course_x_user_info_course_id 
        ON course_x_user_info(course_id);
        
        CREATE INDEX IF NOT EXISTS idx_course_x_user_info_coursestructure_id 
        ON course_x_user_info(coursestructure_id);
        
        CREATE INDEX IF NOT EXISTS idx_course_x_user_info_unified_user_id 
        ON course_x_user_info(unified_user_id);
        
        CREATE INDEX IF NOT EXISTS idx_course_x_user_info_course_user_mapping_id
        ON course_x_user_info(course_user_mapping_id);
        
        -- Indexes for unified_user lookups (if not already created)
        CREATE INDEX IF NOT EXISTS idx_unified_user_user_id 
        ON unified_user(user_id) WHERE user_id IS NOT NULL;
        
        CREATE INDEX IF NOT EXISTS idx_unified_user_phone 
        ON unified_user(phone) WHERE phone IS NOT NULL;
        
        CREATE INDEX IF NOT EXISTS idx_unified_user_email 
        ON unified_user(email) WHERE email IS NOT NULL;
"""

# Optimized query using JSON aggregation instead of multiple subqueries
FETCH_COURSE_USER_DATA_QUERY = """
WITH form_responses AS (
    SELECT 
        cuafqm.course_user_apply_form_mapping_id,
        jsonb_object_agg(
            afqm.apply_form_question_id::text,
            cuafqm.response
        ) as responses
    FROM apply_forms_courseuserapplyformquestionmapping cuafqm
    JOIN apply_forms_applyformquestionmapping afqm 
        ON cuafqm.apply_form_question_mapping_id = afqm.id
    WHERE afqm.apply_form_question_id IN (53, 100, 102, 62, 110, 3, 107, 17, 101, 95, 104, 97, 109, 103)
    GROUP BY cuafqm.course_user_apply_form_mapping_id
)
SELECT
    courseuserapplyformmapping.id as course_user_apply_form_mapping_id,
    courseuserapplyformmapping.created_at as course_user_apply_form_mapping_created_at,
    courseusermapping.id as course_user_mapping_id,
    courseusermapping.user_id as user_id,
    courseusermapping.utm_param_json as course_user_mapping_utm_param_json,
    course.id as course_id,
    coursestructure.slug as coursestructure_slug,
    coursestructure.id as coursestructure_id,
    courseusertimelineflowmapping.id as course_user_timeline_flowmapping_id,
    courseusertimelineflowmapping.course_timeline_flow as course_user_timeline_flow_mapping_course_timeline_flow,
    courseusertimelineflowmapping.apply_form_question_set as courseusertimelineflow_mapping_apply_form_question_set,
    courseusertimelineflowmapping.apply_form_version as course_user_timeline_flow_mapping_apply_form_version,
    authuser.email as user_email,
    userprofile.phone as user_profile_phone,
    -- Extract form responses from JSON
    fr.responses->>'53' as current_work,
    fr.responses->>'100' as yearly_salary,
    fr.responses->>'102' as bachelor_qualification,
    fr.responses->>'62' as date_of_birth,
    fr.responses->>'110' as twelfth_passing_marks,
    fr.responses->>'3' as graduation_year,
    fr.responses->>'107' as data_science_joining_reason,
    fr.responses->>'17' as current_city,
    fr.responses->>'101' as surety_on_learning_ds,
    fr.responses->>'95' as how_soon_you_can_join,
    fr.responses->>'104' as where_you_get_to_know_about_ns,
    fr.responses->>'97' as work_experience,
    fr.responses->>'109' as given_any_of_following_exam,
    fr.responses->>'103' as department_worked_on,
    -- Max values
    max_assignment_stats.max_all_test_cases_passed as max_all_test_cases_passed,
    max_assessment_stats.max_marks as max_assessment_marks
FROM
    apply_forms_courseuserapplyformmapping courseuserapplyformmapping
    LEFT JOIN courses_courseusermapping courseusermapping 
        ON courseuserapplyformmapping.course_user_mapping_id = courseusermapping.id
    LEFT JOIN courses_course course 
        ON courseusermapping.course_id = course.id
    LEFT JOIN courses_coursestructure coursestructure 
        ON course.course_structure_id = coursestructure.id
    LEFT JOIN form_responses fr 
        ON courseuserapplyformmapping.id = fr.course_user_apply_form_mapping_id
    LEFT JOIN (
        SELECT 
            acum.course_user_mapping_id,
            MAX(acum.all_test_cases_passed_question_total_count) as max_all_test_cases_passed
        FROM assignments_assignmentcourseusermapping acum
        INNER JOIN assignments_assignment a ON acum.assignment_id = a.id
        WHERE a.assignment_type = 2
        GROUP BY acum.course_user_mapping_id
    ) max_assignment_stats ON courseusermapping.id = max_assignment_stats.course_user_mapping_id
    LEFT JOIN (
        SELECT 
            cuam.course_user_mapping_id,
            MAX(cuam.marks) as max_marks
        FROM assessments_courseuserassessmentmapping cuam
        INNER JOIN assessments_assessment a ON cuam.assessment_id = a.id
        WHERE a.assessment_type = 2
        GROUP BY cuam.course_user_mapping_id
    ) max_assessment_stats ON courseusermapping.id = max_assessment_stats.course_user_mapping_id
    LEFT JOIN courses_courseusertimelineflowmapping courseusertimelineflowmapping 
        ON courseusermapping.id = courseusertimelineflowmapping.course_user_mapping_id
    LEFT JOIN auth_user authuser ON courseusermapping.user_id = authuser.id
    LEFT JOIN users_userprofile userprofile ON authuser.id = userprofile.user_id
WHERE
    courseuserapplyformmapping.created_at >= CAST((NOW() + INTERVAL '-7 day') AS date)
    AND courseuserapplyformmapping.created_at < CAST(NOW() AS date)
ORDER BY courseuserapplyformmapping.id
LIMIT %s OFFSET %s;
"""

# Count query for pagination
COUNT_QUERY = """
SELECT COUNT(*)
FROM apply_forms_courseuserapplyformmapping courseuserapplyformmapping
WHERE courseuserapplyformmapping.created_at >= CAST((NOW() + INTERVAL '-7 day') AS date)
    AND courseuserapplyformmapping.created_at < CAST(NOW() AS date);
"""


@dag(
    dag_id="course_x_user_info",
    schedule=None,
    start_date=pendulum.datetime(2025, 5, 27, tz="UTC"),
    catchup=False,
    default_args={
        "owner": "data_team",
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=1),
    }
)
def course_x_user_info():
    """Optimized DAG to fetch user information from newton_api and store in results database."""

    @task(task_id="create_tables_if_not_exists")
    def create_tables_if_not_exists():
        """Create both results tables if they do not exist."""
        postgres_hook = PostgresHook(RESULT_DATABASE_CONNECTION_ID)

        try:
            with postgres_hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(CREATE_COURSE_STRUCTURE_X_USER_INFO_TABLE_QUERY)
                    cursor.execute(CREATE_COURSE_X_USER_INFO_TABLE_QUERY)
                    cursor.execute(CREATE_INDEXES_QUERY)
                    conn.commit()
            logger.info("Tables and indexes created successfully or already exist.")
        except Exception as e:
            logger.error(f"Error creating tables: {str(e)}")
            raise AirflowException(f"Failed to create tables: {str(e)}")

    @task(task_id="fetch_and_process_course_user_data")
    def fetch_and_process_course_user_data():
        """Fetch course user data in batches and process it efficiently."""
        source_hook = PostgresHook(SOURCE_DATABASE_CONNECTION_ID)
        result_hook = PostgresHook(RESULT_DATABASE_CONNECTION_ID)

        try:
            # Get total count for progress tracking
            total_count = source_hook.get_first(COUNT_QUERY)[0]
            logger.info(f"Total records to process: {total_count}")

            if total_count == 0:
                logger.info("No data to process.")
                return

            offset = 0
            total_processed = 0
            total_inserted = 0
            total_updated = 0

            while offset < total_count:
                # Fetch batch of data
                logger.info(f"Fetching batch: offset={offset}, limit={FETCH_BATCH_SIZE}")
                batch_data = source_hook.get_records(
                    FETCH_COURSE_USER_DATA_QUERY,
                    parameters=[FETCH_BATCH_SIZE, offset]
                )

                if not batch_data:
                    break

                # Process batch
                stats = process_batch(result_hook, batch_data)

                total_processed += len(batch_data)
                total_inserted += stats['inserted']
                total_updated += stats['updated']

                logger.info(
                    f"Batch processed: {len(batch_data)} records, "
                    f"{stats['inserted']} inserted, {stats['updated']} updated, "
                    f"{stats['skipped']} skipped. Total progress: {total_processed}/{total_count}"
                )

                offset += FETCH_BATCH_SIZE

            logger.info(
                f"Processing complete. Total: {total_processed} records, "
                f"Inserted: {total_inserted}, Updated: {total_updated}"
            )

        except Exception as e:
            logger.error(f"Error in fetch_and_process_course_user_data: {str(e)}")
            raise AirflowException(f"Failed to process data: {str(e)}")

    def get_unified_user_ids_batch(hook, batch_data: List) -> Dict[str, Optional[int]]:
        """
        Efficiently lookup unified_user_ids for a batch of records.
        Returns a mapping of 'user_id|email|phone' -> unified_user_id
        """
        # Collect unique identifiers
        user_ids = set()
        emails = set()
        phones = set()

        for record in batch_data:
            if record[3]:  # user_id
                user_ids.add(record[3])
            if record[12]:  # email
                emails.add(record[12])
            if record[14]:  # phone
                phones.add(record[14])

        # Build the query with three separate lookups
        lookup_map = {}

        # Lookup by user_id (highest priority)
        if user_ids:
            user_ids_list = list(user_ids)
            placeholders = ','.join(['%s'] * len(user_ids_list))
            query = f"""
                SELECT user_id, id 
                FROM unified_user 
                WHERE user_id IN ({placeholders})
            """
            results = hook.get_records(query, parameters=user_ids_list)
            for user_id, unified_id in results:
                lookup_map[f"user_{user_id}"] = unified_id

        # Lookup by phone (second priority)
        if phones:
            phones_list = list(phones)
            placeholders = ','.join(['%s'] * len(phones_list))
            query = f"""
                SELECT phone, id 
                FROM unified_user 
                WHERE phone IN ({placeholders})
                  AND phone NOT IN (
                    SELECT phone FROM unified_user 
                    WHERE user_id IN (
                        SELECT user_id FROM unified_user 
                        WHERE user_id IS NOT NULL
                    )
                  )
            """
            results = hook.get_records(query, parameters=phones_list)
            for phone, unified_id in results:
                lookup_map[f"phone_{phone}"] = unified_id

        # Lookup by email (third priority)
        if emails:
            emails_list = list(emails)
            placeholders = ','.join(['%s'] * len(emails_list))
            query = f"""
                SELECT email, id 
                FROM unified_user 
                WHERE email IN ({placeholders})
                  AND email NOT IN (
                    SELECT email FROM unified_user 
                    WHERE user_id IN (
                        SELECT user_id FROM unified_user 
                        WHERE user_id IS NOT NULL
                    )
                    OR phone IN (
                        SELECT phone FROM unified_user 
                        WHERE phone IS NOT NULL
                    )
                  )
            """
            results = hook.get_records(query, parameters=emails_list)
            for email, unified_id in results:
                lookup_map[f"email_{email}"] = unified_id

        # Build final mapping for each record
        result_map = {}
        for i, record in enumerate(batch_data):
            user_id, email, phone = record[3], record[12], record[14]
            key = f"{i}_{user_id}_{email}_{phone}"

            # Check in priority order
            if user_id and f"user_{user_id}" in lookup_map:
                result_map[key] = lookup_map[f"user_{user_id}"]
            elif phone and f"phone_{phone}" in lookup_map:
                result_map[key] = lookup_map[f"phone_{phone}"]
            elif email and f"email_{email}" in lookup_map:
                result_map[key] = lookup_map[f"email_{email}"]
            else:
                result_map[key] = None

        return result_map

    def process_batch(hook, batch_data: List) -> Dict[str, int]:
        """Process a batch of records efficiently."""
        stats = {'inserted': 0, 'updated': 0, 'skipped': 0}

        # Get batch lookup of unified_user_ids
        unified_user_lookup = get_unified_user_ids_batch(hook, batch_data)

        # Batch check for existing records
        course_user_mapping_ids = [record[2] for record in batch_data]
        existing_records = get_existing_records(hook, course_user_mapping_ids)
        existing_mapping = {r['course_user_mapping_id']: r for r in existing_records}

        # Batch load course_structure_x_user_info data
        structure_user_data = load_course_structure_user_data(hook, batch_data, unified_user_lookup)

        # Prepare batch insert/update data
        insert_records = []
        update_records = []

        for i, record in enumerate(batch_data):
            user_id = record[3]
            email = record[12]
            phone = record[14]
            course_user_mapping_id = record[2]
            coursestructure_id = record[7]

            # Get unified_user_id from batch lookup
            lookup_key = f"{i}_{user_id}_{email}_{phone}"
            unified_user_id = unified_user_lookup.get(lookup_key)

            if unified_user_id is None:
                stats['skipped'] += 1
                continue

            # Get course_structure_x_user_info_id
            structure_key = f"{coursestructure_id}_{unified_user_id}"
            structure_info = structure_user_data.get(structure_key)

            if not structure_info:
                stats['skipped'] += 1
                continue

            # Prepare record data
            prepared_record = list(record) + [unified_user_id, structure_info['id']]

            if course_user_mapping_id in existing_mapping:
                update_records.append(prepared_record)
            else:
                insert_records.append(prepared_record)

        # Perform batch operations
        if insert_records:
            batch_insert_course_user_data(hook, insert_records)
            stats['inserted'] = len(insert_records)

        if update_records:
            batch_update_course_user_data(hook, update_records)
            stats['updated'] = len(update_records)

        update_course_structure_counters(hook, insert_records, update_records)

        return stats

    def get_existing_records(hook, course_user_mapping_ids: List[int]) -> List[Dict]:
        """Batch check for existing course_x_user_info records."""
        if not course_user_mapping_ids:
            return []

        placeholders = ','.join(['%s'] * len(course_user_mapping_ids))
        query = f"""
            SELECT course_user_mapping_id, id, course_structure_x_user_info_id
            FROM course_x_user_info 
            WHERE course_user_mapping_id IN ({placeholders})
        """

        records = hook.get_records(query, parameters=course_user_mapping_ids)
        return [
            {
                'course_user_mapping_id': r[0],
                'id': r[1],
                'course_structure_x_user_info_id': r[2]
            }
            for r in records
        ]

    def load_course_structure_user_data(hook, batch_data: List, unified_user_lookup: Dict) -> Dict:
        """Load or create course_structure_x_user_info data for the batch."""
        # Collect unique course_structure_id and unified_user_id pairs
        structure_user_pairs = set()
        for i, record in enumerate(batch_data):
            user_id = record[3]
            email = record[12]
            phone = record[14]
            coursestructure_id = record[7]

            lookup_key = f"{i}_{user_id}_{email}_{phone}"
            unified_user_id = unified_user_lookup.get(lookup_key)
            if unified_user_id:
                structure_user_pairs.add((coursestructure_id, unified_user_id))

        if not structure_user_pairs:
            return {}

        # Batch insert missing course_structure_x_user_info records
        values_list = []
        for coursestructure_id, unified_user_id in structure_user_pairs:
            values_list.append(f"({coursestructure_id}, {unified_user_id})")

        # Use INSERT ON CONFLICT to handle concurrent inserts
        insert_query = f"""
            INSERT INTO course_structure_x_user_info (course_structure_id, unified_user_id)
            VALUES {','.join(values_list)}
            ON CONFLICT (course_structure_id, unified_user_id) DO NOTHING;
        """

        hook.run(insert_query)

        # Load all course_structure_x_user_info data
        pairs_conditions = ' OR '.join([
            f"(course_structure_id = {cs_id} AND unified_user_id = {u_id})"
            for cs_id, u_id in structure_user_pairs
        ])

        select_query = f"""
            SELECT id, course_structure_id, unified_user_id, 
                   counter, latest_counter_updated_at
            FROM course_structure_x_user_info
            WHERE {pairs_conditions}
        """

        records = hook.get_records(select_query)

        result = {}
        for record in records:
            key = f"{record[1]}_{record[2]}"  # coursestructure_id_unified_user_id
            result[key] = {
                'id': record[0],
                'counter': record[3],
                'latest_counter_updated_at': record[4]
            }

        return result

    def batch_insert_course_user_data(hook, records):
        """Efficiently insert multiple records."""
        if not records:
            return

        # Process records to handle JSON fields
        processed_records = []
        for record in records:
            processed_record = []
            for i, value in enumerate(record):
                # Index 4 is course_user_mapping_utm_param_json (JSONB field)
                if i == 4 and isinstance(value, dict):
                    processed_record.append(Json(value))
                else:
                    processed_record.append(value)
            processed_records.append(tuple(processed_record))

        insert_query = """
           INSERT INTO course_x_user_info (
               course_user_apply_form_mapping_id,
               course_user_apply_form_mapping_created_at,
               course_user_mapping_id,
               user_id,
               course_user_mapping_utm_param_json,
               course_id,
               coursestructure_slug,
               coursestructure_id,
               course_user_timeline_flowmapping_id,
               course_user_timeline_flow_mapping_course_timeline_flow,
               courseusertimelineflow_mapping_apply_form_question_set,
               course_user_timeline_flow_mapping_apply_form_version,
               email,
               phone,
               current_work,
               yearly_salary,
               bachelor_qualification,
               date_of_birth,
               twelfth_passing_marks,
               graduation_year,
               data_science_joining_reason,
               current_city,
               surety_on_learning_ds,
               how_soon_you_can_join,
               where_you_get_to_know_about_ns,
               work_experience,
               given_any_of_following_exam,
               department_worked_on,
               max_all_test_cases_passed,
               max_assessment_marks,
               unified_user_id,
               course_structure_x_user_info_id
           )
           VALUES %s
        """

        # Execute in smaller batches
        for i in range(0, len(processed_records), 100):
            batch = processed_records[i:i + 100]
            with hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    execute_values(cursor, insert_query, batch)
                    conn.commit()

    def batch_update_course_user_data(hook, records: List):
        """Efficiently update multiple records using CASE statements."""
        if not records:
            return

        # Build update query with CASE statements
        course_user_mapping_ids = [r[2] for r in records]

        # Create CASE statements for each field
        case_statements = []
        fields_to_update = [
            (0, 'course_user_apply_form_mapping_id'),
            (1, 'course_user_apply_form_mapping_created_at'),
            (3, 'user_id'),
            (4, 'course_user_mapping_utm_param_json'),
            (5, 'course_id'),
            (6, 'coursestructure_slug'),
            (7, 'coursestructure_id'),
            (8, 'course_user_timeline_flowmapping_id'),
            (9, 'course_user_timeline_flow_mapping_course_timeline_flow'),
            (10, 'courseusertimelineflow_mapping_apply_form_question_set'),
            (11, 'course_user_timeline_flow_mapping_apply_form_version'),
            (12, 'email'),
            (13, 'phone'),
            (15, 'current_work'),
            (16, 'yearly_salary'),
            (17, 'bachelor_qualification'),
            (18, 'date_of_birth'),
            (19, 'twelfth_passing_marks'),
            (20, 'graduation_year'),
            (21, 'data_science_joining_reason'),
            (22, 'current_city'),
            (23, 'surety_on_learning_ds'),
            (24, 'how_soon_you_can_join'),
            (25, 'where_you_get_to_know_about_ns'),
            (26, 'work_experience'),
            (27, 'given_any_of_following_exam'),
            (28, 'department_worked_on'),
            (29, 'max_all_test_cases_passed'),
            (30, 'max_assessment_marks'),
            (31, 'unified_user_id'),
            (32, 'course_structure_x_user_info_id')
        ]

        # Execute updates in batches
        for i in range(0, len(records), 50):
            batch = records[i:i+50]
            batch_ids = [r[2] for r in batch]

            # Build UPDATE query
            set_clauses = []
            for idx, field_name in fields_to_update:
                case_parts = []
                for record in batch:
                    value = record[idx]
                    if value is None:
                        case_parts.append(f"WHEN course_user_mapping_id = {record[2]} THEN NULL")
                    elif isinstance(value, (dict, list)):
                        # Handle JSON fields
                        import json
                        case_parts.append(f"WHEN course_user_mapping_id = {record[2]} THEN '{json.dumps(value)}'::jsonb")
                    elif isinstance(value, str):
                        # Escape single quotes
                        escaped_value = value.replace("'", "''")
                        case_parts.append(f"WHEN course_user_mapping_id = {record[2]} THEN '{escaped_value}'")
                    else:
                        case_parts.append(f"WHEN course_user_mapping_id = {record[2]} THEN {value}")

                case_statement = f"{field_name} = CASE {' '.join(case_parts)} END"
                set_clauses.append(case_statement)

            placeholders = ','.join(['%s'] * len(batch_ids))
            update_query = f"""
                UPDATE course_x_user_info
                SET {', '.join(set_clauses)},
                    updated_at = CURRENT_TIMESTAMP
                WHERE course_user_mapping_id IN ({placeholders})
            """

            hook.run(update_query, parameters=batch_ids)

    def update_course_structure_counters(
            hook,
            created_records: List[List[Any]],
            updated_records: List[List[Any]]
    ):
        """Update course_structure_x_user_info for new and updated mappings.

        - `created_records`: records newly inserted; increment counters and possibly update response fields.
        - `updated_records`: existing mapping changes; update response fields and timestamp without changing counter.
        """

        # Helper to aggregate by structure_id
        def aggregate(records, inc_flag: bool):
            aggregates = {}
            for rec in records:
                created_at = rec[1]
                struct_id = rec[32]
                resp = {
                        'current_work': rec[15], 'yearly_salary': rec[16],
                        'bachelor_qualification': rec[17], 'date_of_birth': rec[18],
                        'twelfth_passing_marks': rec[19], 'graduation_year': rec[20],
                        'data_science_joining_reason': rec[21], 'current_city': rec[22],
                        'surety_on_learning_ds': rec[23], 'how_soon_you_can_join': rec[24],
                        'where_you_get_to_know_about_ns': rec[25], 'work_experience': rec[26],
                        'given_any_of_following_exam': rec[27], 'department_worked_on': rec[28],
                }
                entry = aggregates.setdefault(
                    struct_id, {
                                'inc': 0,
                                'latest': created_at,
                                'responses': resp
                        }
                    )
                if inc_flag:
                    entry['inc'] += 1
                # Always refresh if this record is newer
                if created_at > entry['latest']:
                    entry['latest'] = created_at
                    entry['responses'] = resp
            return aggregates

        # Build aggregates
        new_aggs = aggregate(created_records, inc_flag=True)
        upd_aggs = aggregate(updated_records, inc_flag=False)

        # Merge updated-only into new if same id
        for sid, u in upd_aggs.items():
            if sid in new_aggs:
                # merge: keep inc from new, but possibly refresh latency and responses
                if u['latest'] > new_aggs[sid]['latest']:
                    new_aggs[sid]['latest'] = u['latest']
                    new_aggs[sid]['responses'] = u['responses']
            else:
                new_aggs[sid] = u

        if not new_aggs:
            return

        # Prepare batch VALUES
        values, params = [], []
        fields = [
                'current_work', 'yearly_salary', 'bachelor_qualification', 'date_of_birth',
                'twelfth_passing_marks', 'graduation_year', 'data_science_joining_reason',
                'current_city', 'surety_on_learning_ds', 'how_soon_you_can_join',
                'where_you_get_to_know_about_ns', 'work_experience',
                'given_any_of_following_exam', 'department_worked_on'
        ]
        for sid, agg in new_aggs.items():
            row = [sid, agg['inc'], agg['latest']]
            for f in fields:
                row.append(agg['responses'][f])
            values.append("(" + ",".join(["%s"] * len(row)) + ")")
            params.extend(row)

        values_clause = ",".join(values)
        sql = f"""
        WITH updates (id, inc, new_ts, {', '.join(fields)}) AS (
            VALUES {values_clause}
        )
        UPDATE course_structure_x_user_info cs
        SET
            counter = cs.counter + updates.inc,
            latest_counter_updated_at = GREATEST(cs.latest_counter_updated_at, updates.new_ts),
            updated_at = CURRENT_TIMESTAMP,
            {', '.join(
                [
                        f"{f} = CASE WHEN updates.new_ts > cs.latest_counter_updated_at THEN updates.{f} ELSE cs.{f} END" for f in fields
                ]
        )}
        FROM updates
        WHERE cs.id = updates.id;
        """
        hook.run(sql, parameters=params)

    # Define task dependencies
    create_tables_task = create_tables_if_not_exists()
    fetch_data_task = fetch_and_process_course_user_data()

    create_tables_task >> fetch_data_task

# Instantiate the DAG
course_x_user_info_dag = course_x_user_info()
