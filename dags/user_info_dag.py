"""
Airflow DAG for managing user information across multiple sources.

This DAG consolidates user data from auth_user and marketing_genericformresponse tables,
manages identity using contact_aliases, and maintains a comprehensive unified_user table
with UTM tracking, profile data, and form response data.
"""
import logging
import uuid
from datetime import timezone
from typing import List, Optional, Tuple, Dict, Any

import pendulum
from airflow.decorators import dag, task
from airflow.models import Param
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Configuration constants
RESULT_DATABASE_CONNECTION_ID = "postgres_result_db"
SOURCE_DATABASE_CONNECTION_ID = "postgres_read_replica"
BATCH_SIZE = 1000  # Process data in batches to avoid memory issues
logger = logging.getLogger(__name__)

# SQL Queries
# Table definition queries
TABLE_QUERIES = {
    "CREATE_USER_INFO_TABLE": """
        CREATE TABLE IF NOT EXISTS unified_user (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            identity_group_id UUID NOT NULL,
            email VARCHAR(512),
            phone VARCHAR(15),
            first_name VARCHAR(255),
            last_name VARCHAR(255),
            date_joined TIMESTAMP,
            last_login TIMESTAMP,
            username VARCHAR(255),
            current_location_city VARCHAR(255),
            current_location_state VARCHAR(255),
            gender VARCHAR(10),
            date_of_birth DATE,
            
            -- UTM information
            first_utm_source VARCHAR(255),
            first_utm_medium VARCHAR(255),
            first_utm_campaign VARCHAR(255),
            first_utm_timestamp TIMESTAMP,
            utm_referer VARCHAR(255),
            first_utm_hash VARCHAR(255),
            
            latest_utm_source VARCHAR(255),
            latest_utm_medium VARCHAR(255),
            latest_utm_campaign VARCHAR(255),
            latest_utm_timestamp TIMESTAMP,
            latest_utm_hash VARCHAR(255),
            
            signup_utm_source VARCHAR(255),
            signup_utm_medium VARCHAR(255),
            signup_utm_campaign VARCHAR(255),
            signup_utm_hash VARCHAR(255),
            
            -- Education information
            tenth_marks VARCHAR(50),
            twelfth_marks VARCHAR(50),
            bachelors_marks VARCHAR(50),
            bachelors_grad_year INTEGER,
            bachelors_degree VARCHAR(255),
            bachelors_field_of_study VARCHAR(255),
            masters_marks VARCHAR(50),
            masters_grad_year INTEGER,
            masters_degree VARCHAR(255),
            masters_field_of_study VARCHAR(255),
            graduation_year INTEGER,
            
            -- Form response data
            "current_role" VARCHAR(255),
            "current_status" VARCHAR(255),
            years_of_experience VARCHAR(50),
            course_type_interested_in VARCHAR(255),
            highest_qualification VARCHAR(255),
            
            -- Additional info
            lead_type VARCHAR(50),
            data_source VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            CONSTRAINT ck_unified_user_presence CHECK (
                (email IS NOT NULL) OR (phone IS NOT NULL)
            )
        );
        
        -- Create indexes for better performance
        CREATE INDEX IF NOT EXISTS idx_unified_user_user_id ON unified_user(user_id) WHERE user_id IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_unified_user_identity_group_id ON unified_user(identity_group_id);
        CREATE INDEX IF NOT EXISTS idx_unified_user_email ON unified_user(email) WHERE email IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_unified_user_phone ON unified_user(phone) WHERE phone IS NOT NULL;
    """
}

# Auth user related queries
AUTH_USER_QUERIES = {
    "FETCH_RECENT_USER_DATA": """
        with lead_type_table as(
            select distinct 
                user_id,
                case when other_status is null then 'Fresh' else 'Deferred' end as lead_type
            from(
                with raw_data as (
                    select
                        courses_course.id as course_id,
                        courses_course.title as batch_name,
                        courses_course.start_timestamp,
                        courses_course.end_timestamp,
                        user_id,
                        courses_courseusermapping.status,
                        email
                    from courses_courseusermapping
                    join courses_course
                        on courses_course.id = courses_courseusermapping.course_id 
                        and courses_courseusermapping.status not in (13,14,18,27) 
                        and courses_course.course_type in (1,6)
                    join auth_user
                        on auth_user.id = courses_courseusermapping.user_id
                    where 
                        date(courses_course.start_timestamp) < date(current_date)
                        and courses_course.course_structure_id in (1,6,8,11,12,13,14,18,19,20,21,22,23,26,50,51,52,53,54,55,56,57,58,59,60,72,127,118,119,122,121)
                        and unit_type like 'LEARNING'
                ),
                
                non_studying as (
                    select * 
                    from raw_data
                    where status in (11,30)
                ),
                    
                studying as (
                    select *
                    from raw_data
                    where status in (5,8,9)
                )
                    
                select
                    studying.*,
                    non_studying.status as other_status,
                    non_studying.course_id as other_course_id,
                    non_studying.start_timestamp as other_st,
                    non_studying.end_timestamp as other_et
                from studying
                left join non_studying 
                    on non_studying.user_id = studying.user_id
                group by 1,2,3,4,5,6,7,8,9,10,11
            ) raw
        ),
        
        t1 as(
            select 
                distinct auth_user.id as user_id,
                auth_user.first_name,
                auth_user.last_name,
                auth_user.date_joined as date_joined,
                auth_user.last_login as last_login,
                auth_user.username,
                auth_user.email,
                users_userprofile.phone,
                internationalization_city.name as current_location_city,
                internationalization_state.name as current_location_state,
                case when users_userprofile.gender = 1 then 'Male' 
                     when users_userprofile.gender = 2 then 'Female' 
                     when users_userprofile.gender = 3 then 'Other' end as gender,
                users_userprofile.date_of_birth as date_of_birth,
                (users_userprofile.utm_param_json -> 'utm_source') #>> '{}' AS utm_source, 
                (users_userprofile.utm_param_json -> 'utm_medium') #>> '{}' AS utm_medium, 
                (users_userprofile.utm_param_json -> 'utm_campaign') #>> '{}' AS utm_campaign,
                (users_userprofile.utm_param_json -> 'utm_referer') #>> '{}' AS utm_referer,
                (users_userprofile.utm_param_json -> 'utm_hash') #>> '{}' AS utm_hash,
                (courses_courseusermapping.utm_param_json -> 'utm_source') #>> '{}' as latest_utm_source,
                (courses_courseusermapping.utm_param_json -> 'utm_medium') #>> '{}' as latest_utm_medium,
                (courses_courseusermapping.utm_param_json -> 'utm_campaign') #>> '{}' as latest_utm_campaign,
                (courses_courseusermapping.utm_param_json -> 'utm_hash') #>> '{}' as latest_utm_hash,
                courses_courseusermapping.created_at as latest_utm_timestamp,
                A.grade as tenth_marks,
                B.grade as twelfth_marks,
                C.grade as bachelors_marks,
                EXTRACT(YEAR FROM C.end_date) as bachelors_grad_year,
                E.name as bachelors_degree,
                F.name as bachelors_field_of_study,
                D.grade as masters_marks,
                EXTRACT(YEAR FROM D.end_date) as masters_grad_year,
                M.name as masters_degree,
                MF.name as masters_field_of_study,
                lead_type_table.lead_type,
                users_extendeduserprofile.graduation_year as graduation_year,
                row_number() over(partition by auth_user.id order by date_joined) as rank
            from auth_user 
            left join (
                select 
                    user_id,
                    utm_param_json,
                    created_at
                from (
                    select 
                        user_id,
                        utm_param_json,
                        created_at,
                        row_number() over(partition by user_id order by created_at desc) as rn
                    from courses_courseusermapping 
                ) a
                where rn = 1
            ) courses_courseusermapping ON (courses_courseusermapping.user_id = auth_user.id) 
            left join users_userprofile on users_userprofile.user_id = auth_user.id 
            left join users_extendeduserprofile on users_extendeduserprofile.user_id = auth_user.id
            left join internationalization_city on users_userprofile.city_id = internationalization_city.id 
            left join internationalization_state on internationalization_state.id = internationalization_city.state_id
            full JOIN users_education A ON (A.user_id = auth_user.id AND A.education_type = 1) 
            full JOIN users_education B ON (B.user_id = auth_user.id AND B.education_type = 2 ) 
            full JOIN users_education C ON (C.user_id = auth_user.id AND C.education_type = 3) 
            full JOIN users_education D ON (D.user_id = auth_user.id AND D.education_type = 4) 
            left join education_degree E on C.degree_id = E.id  
            left join education_fieldofstudy F on C.field_of_study_id = F.id 
            left join education_degree M on D.degree_id = M.id  
            left join education_fieldofstudy MF on D.field_of_study_id = MF.id
            left join lead_type_table on lead_type_table.user_id = auth_user.id
            where 
                -- Only process users updated in the last day
                auth_user.date_joined >= CURRENT_DATE - INTERVAL '1' DAY
                OR auth_user.last_login >= CURRENT_DATE - INTERVAL '1' DAY
        )
        
        select 
            user_id,
            first_name,
            last_name,
            date_joined,
            last_login,
            username,
            email,
            phone,
            current_location_city,
            current_location_state,
            gender,
            date_of_birth,
            utm_source,
            utm_medium,
            utm_campaign,
            utm_referer,
            utm_hash,
            latest_utm_source,
            latest_utm_medium,
            latest_utm_campaign,
            latest_utm_hash,
            latest_utm_timestamp,
            tenth_marks,
            twelfth_marks,
            bachelors_marks,
            bachelors_grad_year,
            bachelors_degree,
            bachelors_field_of_study,
            masters_marks,
            masters_grad_year,
            masters_degree,
            masters_field_of_study,
            lead_type,
            graduation_year
        from t1
        where 
            rank = 1 
            and user_id is not null
    """,
    "FETCH_USER_DATA_BACKFILL": """
        with lead_type_table as(
            select distinct 
                user_id,
                case when other_status is null then 'Fresh' else 'Deferred' end as lead_type
            from(
                with raw_data as (
                    select
                        courses_course.id as course_id,
                        courses_course.title as batch_name,
                        courses_course.start_timestamp,
                        courses_course.end_timestamp,
                        user_id,
                        courses_courseusermapping.status,
                        email
                    from courses_courseusermapping
                    join courses_course
                        on courses_course.id = courses_courseusermapping.course_id 
                        and courses_courseusermapping.status not in (13,14,18,27) 
                        and courses_course.course_type in (1,6)
                    join auth_user
                        on auth_user.id = courses_courseusermapping.user_id
                    where 
                        date(courses_course.start_timestamp) < date(current_date)
                        and courses_course.course_structure_id in (1,6,8,11,12,13,14,18,19,20,21,22,23,26,50,51,52,53,54,55,56,57,58,59,60,72,127,118,119,122,121)
                        and unit_type like 'LEARNING'
                ),
                
                non_studying as (
                    select * 
                    from raw_data
                    where status in (11,30)
                ),
                    
                studying as (
                    select *
                    from raw_data
                    where status in (5,8,9)
                )
                    
                select
                    studying.*,
                    non_studying.status as other_status,
                    non_studying.course_id as other_course_id,
                    non_studying.start_timestamp as other_st,
                    non_studying.end_timestamp as other_et
                from studying
                left join non_studying 
                    on non_studying.user_id = studying.user_id
                group by 1,2,3,4,5,6,7,8,9,10,11
            ) raw
        ),
        
        t1 as(
            select 
                distinct auth_user.id as user_id,
                auth_user.first_name,
                auth_user.last_name,
                auth_user.date_joined as date_joined,
                auth_user.last_login as last_login,
                auth_user.username,
                auth_user.email,
                users_userprofile.phone,
                internationalization_city.name as current_location_city,
                internationalization_state.name as current_location_state,
                case when users_userprofile.gender = 1 then 'Male' 
                     when users_userprofile.gender = 2 then 'Female' 
                     when users_userprofile.gender = 3 then 'Other' end as gender,
                users_userprofile.date_of_birth as date_of_birth,
                (users_userprofile.utm_param_json -> 'utm_source') #>> '{}' AS utm_source, 
                (users_userprofile.utm_param_json -> 'utm_medium') #>> '{}' AS utm_medium, 
                (users_userprofile.utm_param_json -> 'utm_campaign') #>> '{}' AS utm_campaign,
                (users_userprofile.utm_param_json -> 'utm_referer') #>> '{}' AS utm_referer,
                (users_userprofile.utm_param_json -> 'utm_hash') #>> '{}' AS utm_hash,
                (courses_courseusermapping.utm_param_json -> 'utm_source') #>> '{}' as latest_utm_source,
                (courses_courseusermapping.utm_param_json -> 'utm_medium') #>> '{}' as latest_utm_medium,
                (courses_courseusermapping.utm_param_json -> 'utm_campaign') #>> '{}' as latest_utm_campaign,
                (courses_courseusermapping.utm_param_json -> 'utm_hash') #>> '{}' as latest_utm_hash,
                courses_courseusermapping.created_at as latest_utm_timestamp,
                A.grade as tenth_marks,
                B.grade as twelfth_marks,
                C.grade as bachelors_marks,
                EXTRACT(YEAR FROM C.end_date) as bachelors_grad_year,
                E.name as bachelors_degree,
                F.name as bachelors_field_of_study,
                D.grade as masters_marks,
                EXTRACT(YEAR FROM D.end_date) as masters_grad_year,
                M.name as masters_degree,
                MF.name as masters_field_of_study,
                lead_type_table.lead_type,
                users_extendeduserprofile.graduation_year as graduation_year,
                row_number() over(partition by auth_user.id order by date_joined) as rank
            from auth_user 
            left join (
                select 
                    user_id,
                    utm_param_json,
                    created_at
                from (
                    select 
                        user_id,
                        utm_param_json,
                        created_at,
                        row_number() over(partition by user_id order by created_at desc) as rn
                    from courses_courseusermapping 
                ) a
                where rn = 1
            ) courses_courseusermapping ON (courses_courseusermapping.user_id = auth_user.id) 
            left join users_userprofile on users_userprofile.user_id = auth_user.id 
            left join users_extendeduserprofile on users_extendeduserprofile.user_id = auth_user.id
            left join internationalization_city on users_userprofile.city_id = internationalization_city.id 
            left join internationalization_state on internationalization_state.id = internationalization_city.state_id
            full JOIN users_education A ON (A.user_id = auth_user.id AND A.education_type = 1) 
            full JOIN users_education B ON (B.user_id = auth_user.id AND B.education_type = 2 ) 
            full JOIN users_education C ON (C.user_id = auth_user.id AND C.education_type = 3) 
            full JOIN users_education D ON (D.user_id = auth_user.id AND D.education_type = 4) 
            left join education_degree E on C.degree_id = E.id  
            left join education_fieldofstudy F on C.field_of_study_id = F.id 
            left join education_degree M on D.degree_id = M.id  
            left join education_fieldofstudy MF on D.field_of_study_id = MF.id
            left join lead_type_table on lead_type_table.user_id = auth_user.id
            where 
                -- Only process users updated in the last day
                auth_user.id BETWEEN %s AND %s
        )
        
        select 
            user_id,
            first_name,
            last_name,
            date_joined,
            last_login,
            username,
            email,
            phone,
            current_location_city,
            current_location_state,
            gender,
            date_of_birth,
            utm_source,
            utm_medium,
            utm_campaign,
            utm_referer,
            utm_hash,
            latest_utm_source,
            latest_utm_medium,
            latest_utm_campaign,
            latest_utm_hash,
            latest_utm_timestamp,
            tenth_marks,
            twelfth_marks,
            bachelors_marks,
            bachelors_grad_year,
            bachelors_degree,
            bachelors_field_of_study,
            masters_marks,
            masters_grad_year,
            masters_degree,
            masters_field_of_study,
            lead_type,
            graduation_year
        from t1
        where 
            rank = 1 
            and user_id is not null
    """
}

# Form response related queries
FORM_RESPONSE_QUERIES = {
    "FETCH_RECENT_FORM_RESPONSES": """
        SELECT
            id,
            created_at,
            response_json ->> 'email' AS email,
            COALESCE(response_json ->> 'phone', response_json ->> 'phone_number') AS phone,
            response_json ->> 'first_name' AS first_name,
            response_json ->> 'utm_source' AS utm_source,
            response_json ->> 'utm_medium' AS utm_medium,
            response_json ->> 'utm_campaign' AS utm_campaign,
            response_json ->> 'utm_referer' AS utm_referer,
            response_json ->> 'utm_hash' AS utm_hash,
            response_json ->> 'graduation_year' AS graduation_year,
            response_json ->> 'current_role' AS current_role,
            response_json ->> 'current_status' AS current_status,
            response_json ->> 'years_of_experience' AS years_of_experience,
            response_json ->> 'course_type_interested_in' AS course_type_interested_in,
            response_json ->> 'highest_qualification' AS highest_qualification,
            response_json ->> '_degree' AS degree,
            response_json ->> 'activity_event' AS activity_event
        FROM
            marketing_genericformresponse
        WHERE
            created_at::date = (CURRENT_DATE - INTERVAL '1 day')::date
            AND (
                (response_json ? 'email' AND NULLIF(TRIM(response_json ->> 'email'), '') IS NOT NULL)
                OR
                ((response_json ? 'phone' AND NULLIF(TRIM(response_json ->> 'phone'), '') IS NOT NULL)
                 OR
                 (response_json ? 'phone_number' AND NULLIF(TRIM(response_json ->> 'phone_number'), '') IS NOT NULL))
            )
        ORDER BY 
            created_at ASC
    """,
    "FETCH_FORM_RESPONSES_BACKFILL": """
        SELECT
            id,
            created_at,
            response_json ->> 'email' AS email,
            COALESCE(response_json ->> 'phone', response_json ->> 'phone_number') AS phone,
            response_json ->> 'first_name' AS first_name,
            response_json ->> 'utm_source' AS utm_source,
            response_json ->> 'utm_medium' AS utm_medium,
            response_json ->> 'utm_campaign' AS utm_campaign,
            response_json ->> 'utm_referer' AS utm_referer,
            response_json ->> 'utm_hash' AS utm_hash,
            response_json ->> 'graduation_year' AS graduation_year,
            response_json ->> 'current_role' AS current_role,
            response_json ->> 'current_status' AS current_status,
            response_json ->> 'years_of_experience' AS years_of_experience,
            response_json ->> 'course_type_interested_in' AS course_type_interested_in,
            response_json ->> 'highest_qualification' AS highest_qualification,
            response_json ->> '_degree' AS degree,
            response_json ->> 'activity_event' AS activity_event
        FROM
            marketing_genericformresponse
        WHERE
            id BETWEEN %s AND %s
            AND (
                (response_json ? 'email' AND NULLIF(TRIM(response_json ->> 'email'), '') IS NOT NULL)
                OR
                ((response_json ? 'phone' AND NULLIF(TRIM(response_json ->> 'phone'), '') IS NOT NULL)
                 OR
                 (response_json ? 'phone_number' AND NULLIF(TRIM(response_json ->> 'phone_number'), '') IS NOT NULL))
            )
        ORDER BY 
            created_at ASC
    """
}

# Contact alias related queries
CONTACT_ALIAS_QUERIES = {
    "FETCH_IDENTITY_BY_EMAIL_OR_PHONE": """
        SELECT identity_group_id, user_id 
        FROM contact_aliases 
        WHERE email = %s OR phone = %s
        ORDER BY created_at ASC
        LIMIT 1;
    """
}

# User info related queries
USER_INFO_QUERIES = {
    "CHECK_EXISTING_RECORD": """
        SELECT id, user_id, identity_group_id, 
               first_utm_source, first_utm_medium, first_utm_campaign, first_utm_timestamp, utm_referer, first_utm_hash,
               latest_utm_source, latest_utm_medium, latest_utm_campaign, latest_utm_timestamp, latest_utm_hash,
               data_source
        FROM unified_user 
        WHERE user_id = %s OR identity_group_id = %s
        LIMIT 1
    """,

    "CHECK_IDENTITY_GROUP_RECORD": """
        SELECT id, latest_utm_timestamp, first_utm_timestamp
        FROM unified_user 
        WHERE identity_group_id = %s
        LIMIT 1
    """,

    "UPDATE_LATEST_UTM_INFO": """
        UPDATE unified_user
        SET 
            latest_utm_source = %s,
            latest_utm_medium = %s,
            latest_utm_campaign = %s,
            latest_utm_timestamp = %s,
            utm_referer = %s,
            latest_utm_hash = %s,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
    """,

    "UPDATE_FIRST_UTM_INFO": """
        UPDATE unified_user
        SET 
            first_utm_source = %s,
            first_utm_medium = %s,
            first_utm_campaign = %s,
            first_utm_timestamp = %s,
            utm_referer = %s,
            first_utm_hash = %s,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
    """
}


def normalize_timestamp(ts):
    if ts is None:
        return None
    # If timestamp is naive (no timezone info), make it timezone-aware
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts


class UserInfoManager:
    """Manager class for user information operations."""

    def __init__(
        self,
        result_db_hook: PostgresHook,
        source_db_hook: PostgresHook,
    ):
        """Initialize with database connections.

        Args:
            result_db_hook: Hook for the results database
            source_db_hook: Hook for the source database
        """
        self.result_db_hook = result_db_hook
        self.source_db_hook = source_db_hook

        # Backfill related fields
        self.start_id = None
        self.end_id = None

        logger.info("Initializing UserInfoManager")

    def create_tables(self) -> None:
        """Create necessary tables if they don't exist."""
        with self.result_db_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                # Create unified_user table
                cursor.execute(TABLE_QUERIES["CREATE_USER_INFO_TABLE"])
                conn.commit()
        logger.info("Tables created or verified")

    def get_identity_from_contact_aliases(self, email: Optional[str], phone: Optional[str]) -> Tuple[Optional[str], Optional[int]]:
        """Get identity_group_id and user_id from contact_aliases.

        Args:
            email: Email address
            phone: Phone number

        Returns:
            Tuple of (identity_group_id, user_id) or (None, None) if not found
        """
        if email is None and phone is None:
            return None, None

        with self.result_db_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    CONTACT_ALIAS_QUERIES["FETCH_IDENTITY_BY_EMAIL_OR_PHONE"],
                    (email, phone)
                )
                result = cursor.fetchone()

        if result:
            return result[0], result[1]
        return None, None

    def process_auth_user_data(self, backfill: bool = False) -> None:
        """Process user data from auth_user and related tables."""
        with self.source_db_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                if not backfill:
                    cursor.execute(AUTH_USER_QUERIES["FETCH_RECENT_USER_DATA"])
                else:
                    cursor.execute(AUTH_USER_QUERIES["FETCH_USER_DATA_BACKFILL"], (self.start_id, self.end_id))

                # Process in batches
                batch = cursor.fetchmany(BATCH_SIZE)
                batch_count = 0
                total_records = 0

                while batch:
                    batch_count += 1
                    total_records += len(batch)
                    logger.info(f"Processing batch {batch_count} with {len(batch)} users")

                    for user in batch:
                        (user_id, first_name, last_name, date_joined, last_login, username,
                         email, phone, current_location_city, current_location_state,
                         gender, date_of_birth, utm_source, utm_medium, utm_campaign,
                         utm_referer, utm_hash, latest_utm_source, latest_utm_medium,
                         latest_utm_campaign, latest_utm_hash,
                         latest_utm_timestamp, tenth_marks, twelfth_marks,
                         bachelors_marks, bachelors_grad_year, bachelors_degree,
                         bachelors_field_of_study, masters_marks, masters_grad_year,
                         masters_degree, masters_field_of_study, lead_type, graduation_year) = user

                        # Get identity_group_id from contact_aliases or create a new one
                        identity_group_id, existing_user_id = self.get_identity_from_contact_aliases(email, phone)
                        if not identity_group_id:
                            identity_group_id = str(uuid.uuid4())

                        # Process user data
                        self._upsert_unified_user(
                            user_id=user_id,
                            identity_group_id=identity_group_id,
                            email=email,
                            phone=phone,
                            first_name=first_name,
                            last_name=last_name,
                            date_joined=date_joined,
                            last_login=last_login,
                            username=username,
                            current_location_city=current_location_city,
                            current_location_state=current_location_state,
                            gender=gender,
                            date_of_birth=date_of_birth,
                            utm_referer=utm_referer,

                            # UTM information
                            signup_utm_source=utm_source,
                            signup_utm_medium=utm_medium,
                            signup_utm_campaign=utm_campaign,
                            signup_utm_hash=utm_hash,

                            # Set first UTM to signup UTM if this is a new record
                            first_utm_source=utm_source,
                            first_utm_medium=utm_medium,
                            first_utm_campaign=utm_campaign,
                            first_utm_timestamp=date_joined,
                            first_utm_hash=utm_hash,

                            # Latest UTM info
                            latest_utm_source=latest_utm_source,
                            latest_utm_medium=latest_utm_medium,
                            latest_utm_campaign=latest_utm_campaign,
                            latest_utm_timestamp=latest_utm_timestamp,
                            latest_utm_hash=latest_utm_hash,

                            # Education information
                            tenth_marks=tenth_marks,
                            twelfth_marks=twelfth_marks,
                            bachelors_marks=bachelors_marks,
                            bachelors_grad_year=bachelors_grad_year,
                            bachelors_degree=bachelors_degree,
                            bachelors_field_of_study=bachelors_field_of_study,
                            masters_marks=masters_marks,
                            masters_grad_year=masters_grad_year,
                            masters_degree=masters_degree,
                            masters_field_of_study=masters_field_of_study,
                            graduation_year=graduation_year,

                            # Additional info
                            lead_type=lead_type,
                            data_source="auth_user"
                        )

                    # Fetch next batch
                    batch = cursor.fetchmany(BATCH_SIZE)

        logger.info(f"Completed processing auth user data - {total_records} users in {batch_count} batches")

    def process_form_response_data(self, backfill: bool = False) -> None:
        """Process data from marketing_genericformresponse."""
        with self.source_db_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                if not backfill:
                    cursor.execute(FORM_RESPONSE_QUERIES["FETCH_RECENT_FORM_RESPONSES"])
                else:
                    cursor.execute(FORM_RESPONSE_QUERIES["FETCH_FORM_RESPONSES_BACKFILL"], (self.start_id, self.end_id))

                # Process in batches
                form_responses = []
                batch = cursor.fetchmany(BATCH_SIZE)
                batch_count = 0

                while batch:
                    batch_count += 1
                    logger.info(f"Fetching batch {batch_count} with {len(batch)} form responses")
                    form_responses.extend(batch)
                    batch = cursor.fetchmany(BATCH_SIZE)

                logger.info(f"Total form responses to process: {len(form_responses)}")

                # Group form responses by email/phone
                response_groups = self._group_form_responses_by_identity(form_responses)

                logger.info(f"Processing {len(response_groups)} unique identity groups")

                # Process each group
                processed_count = 0
                for group_id, group_data in response_groups.items():
                    self._process_form_response_group(group_id, group_data)
                    processed_count += 1

                    # Log progress periodically
                    if processed_count % 100 == 0:
                        logger.info(f"Processed {processed_count}/{len(response_groups)} identity groups")

        logger.info(f"Completed processing form response data - {len(response_groups)} identity groups")

    def _group_form_responses_by_identity(self, form_responses: List[Tuple]) -> Dict[str, Dict[str, Any]]:
        """Group form responses by identity group ID.

        Args:
            form_responses: List of form response records

        Returns:
            Dictionary mapping identity group IDs to response data
        """
        response_groups = {}
        skipped_count = 0

        for response in form_responses:
            (id, created_at, email, phone, first_name,
             utm_source, utm_medium, utm_campaign, utm_referer, utm_hash,
             graduation_year, current_role, current_status, years_of_experience,
             course_type_interested_in, highest_qualification, degree, activity_event) = response

            identity_group_id, user_id = self.get_identity_from_contact_aliases(email, phone)
            if not identity_group_id:
                # Skip if we can't find a match in contact_aliases
                skipped_count += 1
                continue

            # Use identity_group_id as key for grouping
            if identity_group_id not in response_groups:
                response_groups[identity_group_id] = {
                    'user_id': user_id,
                    'identity_group_id': identity_group_id,
                    'responses': []
                }

            response_groups[identity_group_id]['responses'].append({
                'id': id,
                'created_at': created_at,
                'email': email,
                'phone': phone,
                'first_name': first_name,
                'utm_source': utm_source,
                'utm_medium': utm_medium,
                'utm_campaign': utm_campaign,
                'utm_referer': utm_referer,
                'utm_hash': utm_hash,
                'graduation_year': graduation_year,
                'current_role': current_role,
                'current_status': current_status,
                'years_of_experience': years_of_experience,
                'course_type_interested_in': course_type_interested_in,
                'highest_qualification': highest_qualification,
                'degree': degree,
                'activity_event': activity_event
            })

        logger.info(f"Grouped {len(form_responses) - skipped_count} form responses into {len(response_groups)} identity groups. Skipped {skipped_count} responses with no matching identity.")
        return response_groups

    def _process_form_response_group(self, group_id: str, group_data: Dict[str, Any]) -> None:
        """Process a group of form responses for the same identity.

        Args:
            group_id: Identity group ID
            group_data: Group data including user_id and responses
        """
        responses = group_data['responses']
        if not responses:
            return

        # Sort by created_at
        responses.sort(key=lambda x: x['created_at'])

        first_response = responses[0]
        last_response = responses[-1]

        # Find best values for certain fields by looking at all responses
        best_values = self._extract_best_values_from_responses(responses)

        # Update unified_user table with form response data
        self._update_unified_user_from_form_response(
            user_id=group_data['user_id'],
            identity_group_id=group_id,
            email=first_response['email'],
            phone=first_response['phone'],
            first_name=best_values.get('first_name'),
            utm_referer=first_response['utm_referer'],

            # UTM info from first and latest responses
            first_utm_source=first_response['utm_source'],
            first_utm_medium=first_response['utm_medium'],
            first_utm_campaign=first_response['utm_campaign'],
            first_utm_timestamp=first_response['created_at'],
            first_utm_hash=first_response['utm_hash'],

            latest_utm_source=last_response['utm_source'],
            latest_utm_medium=last_response['utm_medium'],
            latest_utm_campaign=last_response['utm_campaign'],
            latest_utm_timestamp=last_response['created_at'],
            latest_utm_hash=last_response['utm_hash'],

            # Additional MGFR fields
            graduation_year=best_values.get('graduation_year'),
            current_role=best_values.get('current_role'),
            current_status=best_values.get('current_status'),
            years_of_experience=best_values.get('years_of_experience'),
            course_type_interested_in=best_values.get('course_type_interested_in'),
            highest_qualification=best_values.get('highest_qualification'),

            # Set data source
            data_source="form_response"
        )

    def _extract_best_values_from_responses(self, responses: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Extract the best non-null values from a list of form responses.

        Args:
            responses: List of form response data

        Returns:
            Dictionary with best values for each field
        """
        # Start with empty values
        best_values = {
            'first_name': None,
            'graduation_year': None,
            'current_role': None,
            'current_status': None,
            'years_of_experience': None,
            'course_type_interested_in': None,
            'highest_qualification': None
        }

        # Iterate through responses to find non-null values
        for response in responses:
            for field in best_values.keys():
                # If we don't have a value yet and this response has one, use it
                if best_values[field] is None and response.get(field) is not None:
                    best_values[field] = response[field]

        return best_values

    def _upsert_unified_user(self, **kwargs) -> None:
        """Insert or update record in unified_user table.

        Args:
            **kwargs: Key-value pairs for unified_user fields
        """
        user_id = kwargs.get('user_id')
        identity_group_id = kwargs.get('identity_group_id')

        with self.result_db_hook.get_conn() as conn:
            try:
                # Start a transaction
                conn.autocommit = False
                with conn.cursor() as cursor:
                    # Check if record exists
                    cursor.execute(
                        USER_INFO_QUERIES["CHECK_EXISTING_RECORD"],
                        (user_id, identity_group_id)
                    )
                    existing_record = cursor.fetchone()

                    if existing_record:
                        # Record exists - handle UTM updates carefully
                        record_id = existing_record[0]
                        existing_data_source = existing_record[16] if len(existing_record) > 16 else None

                        # Extract UTM data from existing record
                        existing_first_utm = {
                            'source': existing_record[3],
                            'medium': existing_record[4],
                            'campaign': existing_record[5],
                            'timestamp': existing_record[6],
                            'referer': existing_record[7],
                            'hash': existing_record[8]
                        }

                        existing_latest_utm = {
                            'source': existing_record[9],
                            'medium': existing_record[10],
                            'campaign': existing_record[11],
                            'timestamp': existing_record[12],
                            'hash': existing_record[13]
                        }

                        # Prepare update fields
                        fields = []
                        values = []

                        # Special handling for auth_user source
                        if kwargs.get('data_source') == 'auth_user':
                            # For auth_user, always update certain fields
                            for key in [
                                'first_name', 'last_name', 'username', 'date_joined', 'last_login',
                                'current_location_city', 'current_location_state', 'gender', 'date_of_birth',
                                'tenth_marks', 'twelfth_marks', 'bachelors_marks', 'bachelors_grad_year',
                                'bachelors_degree', 'bachelors_field_of_study', 'masters_marks',
                                'masters_grad_year', 'masters_degree', 'masters_field_of_study',
                                'lead_type', 'user_id'
                            ]:
                                if kwargs.get(key) is not None:
                                    fields.append(f"{key} = %s")
                                    values.append(kwargs.get(key))

                            # Always update signup UTM for auth_user source
                            if 'signup_utm_source' in kwargs:
                                fields.append("signup_utm_source = %s")
                                values.append(kwargs.get('signup_utm_source'))
                                fields.append("signup_utm_medium = %s")
                                values.append(kwargs.get('signup_utm_medium'))
                                fields.append("signup_utm_campaign = %s")
                                values.append(kwargs.get('signup_utm_campaign'))
                                fields.append("signup_utm_hash = %s")
                                values.append(kwargs.get('signup_utm_hash'))
                        else:
                            # For form_response, only update fields that aren't already set
                            for key in [
                                'first_name', 'current_role', 'current_status', 'years_of_experience',
                                'course_type_interested_in', 'highest_qualification'
                            ]:
                                value = kwargs.get(key)
                                if value is not None:
                                    # Check if we should update this field
                                    cursor.execute(f"SELECT {key} FROM unified_user WHERE id = %s", (record_id,))
                                    current_value = cursor.fetchone()[0]
                                    if current_value is None:
                                        fields.append(f"{key} = %s")
                                        values.append(value)

                            # For graduation_year, prefer auth_user data over form_response
                            if kwargs.get('graduation_year') is not None and existing_data_source != 'auth_user':
                                cursor.execute("SELECT graduation_year FROM unified_user WHERE id = %s", (record_id,))
                                current_value = cursor.fetchone()[0]
                                if current_value is None:
                                    fields.append("graduation_year = %s")
                                    values.append(kwargs.get('graduation_year'))

                        # Handle first UTM update - only update if the new timestamp is earlier than existing
                        # or if existing is null
                        new_first_utm_timestamp = kwargs.get('first_utm_timestamp')
                        if new_first_utm_timestamp is not None:
                            new_first_utm_timestamp = normalize_timestamp(new_first_utm_timestamp)
                        if (new_first_utm_timestamp is not None and
                            new_first_utm_timestamp is not None and
                            (existing_first_utm['timestamp'] is None or
                             new_first_utm_timestamp < normalize_timestamp(existing_first_utm['timestamp']))):

                            # Update first UTM with new earlier values
                            cursor.execute(
                                USER_INFO_QUERIES["UPDATE_FIRST_UTM_INFO"],
                                (
                                    kwargs.get('first_utm_source'),
                                    kwargs.get('first_utm_medium'),
                                    kwargs.get('first_utm_campaign'),
                                    new_first_utm_timestamp,
                                    kwargs.get('utm_referer'),
                                    kwargs.get('first_utm_hash'),
                                    record_id
                                )
                            )

                        # Handle latest UTM update - only update if the new timestamp is later than existing
                        # or if existing is null
                        new_latest_utm_timestamp = kwargs.get('latest_utm_timestamp')
                        if new_latest_utm_timestamp is not None:
                            new_latest_utm_timestamp = normalize_timestamp(new_latest_utm_timestamp)
                        if (new_latest_utm_timestamp is not None and
                            (existing_latest_utm['timestamp'] is None or
                             new_latest_utm_timestamp > normalize_timestamp(existing_latest_utm['timestamp']))):

                            # Update latest UTM with new later values
                            cursor.execute(
                                USER_INFO_QUERIES["UPDATE_LATEST_UTM_INFO"],
                                (
                                    kwargs.get('latest_utm_source'),
                                    kwargs.get('latest_utm_medium'),
                                    kwargs.get('latest_utm_campaign'),
                                    new_latest_utm_timestamp,
                                    kwargs.get('utm_referer'),
                                    kwargs.get('latest_utm_hash'),
                                    record_id
                                )
                            )

                        # Always update data_source if moving from form_response to auth_user
                        if kwargs.get('data_source') == 'auth_user' and existing_data_source != 'auth_user':
                            fields.append("data_source = %s")
                            values.append('auth_user')

                        # Always update timestamp
                        fields.append("updated_at = CURRENT_TIMESTAMP")

                        if fields:
                            update_query = f"""
                                UPDATE unified_user SET {', '.join(fields)}
                                WHERE id = %s
                            """
                            values.append(record_id)
                            cursor.execute(update_query, values)
                    else:
                        # Insert new record
                        # Prepare field names and placeholders
                        fields = []
                        placeholders = []
                        values = []

                        for key, value in kwargs.items():
                            if value is not None:
                                fields.append(key)
                                placeholders.append("%s")
                                values.append(value)

                        insert_query = f"""
                            INSERT INTO unified_user ({', '.join(fields)})
                            VALUES ({', '.join(placeholders)})
                        """
                        cursor.execute(insert_query, values)

                    conn.commit()
            except Exception as e:
                conn.rollback()
                logger.error(f"Error processing user_id={user_id}, identity_group_id={identity_group_id}: {str(e)}")
                # Re-raise for critical errors
                if "violates not-null constraint" in str(e):
                    raise
            finally:
                conn.autocommit = True

    def _update_unified_user_from_form_response(self, **kwargs) -> None:
        """Update unified_user with data from form responses.

        Args:
            **kwargs: Key-value pairs for unified_user fields
        """
        user_id = kwargs.get('user_id')
        identity_group_id = kwargs.get('identity_group_id')
        first_utm_timestamp = normalize_timestamp(kwargs.get('first_utm_timestamp'))
        latest_utm_timestamp = normalize_timestamp(kwargs.get('latest_utm_timestamp'))

        if not identity_group_id:
            return

        with self.result_db_hook.get_conn() as conn:
            try:
                # Start a transaction
                conn.autocommit = False
                with conn.cursor() as cursor:
                    # Check if record exists and get current UTM timestamps
                    cursor.execute(
                        USER_INFO_QUERIES["CHECK_IDENTITY_GROUP_RECORD"],
                        (identity_group_id,)
                    )
                    existing_record = cursor.fetchone()

                    if existing_record:
                        # Only update UTMs if timestamps are appropriate
                        record_id = existing_record[0]
                        existing_latest_utm_timestamp = existing_record[1]
                        if existing_latest_utm_timestamp:
                            existing_latest_utm_timestamp = normalize_timestamp(existing_latest_utm_timestamp)
                        existing_first_utm_timestamp = existing_record[2]
                        if existing_first_utm_timestamp:
                            existing_first_utm_timestamp = normalize_timestamp(existing_first_utm_timestamp)

                        # Update first UTM if it's earlier than existing or no existing timestamp
                        if first_utm_timestamp is not None and (
                            existing_first_utm_timestamp is None or
                            first_utm_timestamp < existing_first_utm_timestamp
                        ):
                            cursor.execute(
                                USER_INFO_QUERIES["UPDATE_FIRST_UTM_INFO"],
                                (
                                    kwargs.get('first_utm_source'),
                                    kwargs.get('first_utm_medium'),
                                    kwargs.get('first_utm_campaign'),
                                    first_utm_timestamp,
                                    kwargs.get('utm_referer'),
                                    kwargs.get('first_utm_hash'),
                                    record_id
                                )
                            )

                        # Update latest UTM if it's later than existing or no existing timestamp
                        if latest_utm_timestamp is not None and (
                            existing_latest_utm_timestamp is None or
                            latest_utm_timestamp > existing_latest_utm_timestamp
                        ):
                            cursor.execute(
                                USER_INFO_QUERIES["UPDATE_LATEST_UTM_INFO"],
                                (
                                    kwargs.get('latest_utm_source'),
                                    kwargs.get('latest_utm_medium'),
                                    kwargs.get('latest_utm_campaign'),
                                    latest_utm_timestamp,
                                    kwargs.get('utm_referer'),
                                    kwargs.get('latest_utm_hash'),
                                    record_id
                                )
                            )

                        # Update other fields
                        update_fields = []
                        update_values = []

                        for key in [
                            'first_name', 'current_role', 'current_status', 'years_of_experience',
                            'course_type_interested_in', 'highest_qualification'
                        ]:
                            if kwargs.get(key) is not None:
                                # Check if current value is NULL before updating
                                cursor.execute(f"SELECT {key} FROM unified_user WHERE id = %s", (record_id,))
                                current_value = cursor.fetchone()[0]
                                if current_value is None:
                                    update_fields.append(f"{key} = %s")
                                    update_values.append(kwargs.get(key))

                        # Update graduation_year only if not set from auth_user
                        if kwargs.get('graduation_year') is not None:
                            cursor.execute("SELECT data_source, graduation_year FROM unified_user WHERE id = %s", (record_id,))
                            row = cursor.fetchone()
                            data_source = row[0]
                            current_graduation_year = row[1]

                            if current_graduation_year is None or data_source != 'auth_user':
                                update_fields.append("graduation_year = %s")
                                update_values.append(kwargs.get('graduation_year'))

                        if update_fields:
                            update_fields.append("updated_at = CURRENT_TIMESTAMP")
                            update_query = f"""
                                UPDATE unified_user SET {', '.join(update_fields)}
                                WHERE id = %s
                            """
                            update_values.append(record_id)
                            cursor.execute(update_query, update_values)
                    else:
                        # Create new record if it doesn't exist
                        self._upsert_unified_user(**kwargs)

                    conn.commit()
            except Exception as e:
                conn.rollback()
                logger.error(f"Error updating info for identity_group_id={identity_group_id}: {str(e)}")
            finally:
                conn.autocommit = True


@dag(
    dag_id="unified_user_dag",
    schedule="0 1 * * *",  # Run at 1:00 AM UTC every day
    start_date=pendulum.datetime(2025, 4, 23, tz="UTC"),
    catchup=False,
    tags=["unified_user", "data_processing"],
    default_args={
        "owner": "data_team",
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=5),
    },
    doc_md="""
    # User Information Processing DAG

    This DAG consolidates user information from multiple data sources:

    1. Creates/verifies required tables in the result database
    2. Processes user data from auth_user and related tables
    3. Processes form responses data from marketing_genericformresponse
    4. Consolidates user identity across sources using contact_aliases

    The pipeline runs daily and processes data from the previous day.
    """,
)
def unified_user_dag():
    """DAG for processing and consolidating user information."""

    @task(task_id="create_tables")
    def create_tables() -> bool:
        """Create necessary tables if they don't exist."""
        result_db_hook = PostgresHook(postgres_conn_id=RESULT_DATABASE_CONNECTION_ID)
        source_db_hook = PostgresHook(postgres_conn_id=SOURCE_DATABASE_CONNECTION_ID)
        manager = UserInfoManager(result_db_hook, source_db_hook)
        manager.create_tables()
        return True

    @task(task_id="process_auth_user_data")
    def process_auth_user_data(tables_created: bool) -> bool:
        """Process data from auth_user and related tables."""
        if not tables_created:
            raise ValueError("Table creation task failed")

        result_db_hook = PostgresHook(postgres_conn_id=RESULT_DATABASE_CONNECTION_ID)
        source_db_hook = PostgresHook(postgres_conn_id=SOURCE_DATABASE_CONNECTION_ID)
        manager = UserInfoManager(result_db_hook, source_db_hook)
        manager.process_auth_user_data()
        return True

    @task(task_id="process_form_responses")
    def process_form_responses(auth_user_processed: bool) -> bool:
        """Process data from marketing_genericformresponse."""
        if not auth_user_processed:
            raise ValueError("Auth user processing task failed")

        result_db_hook = PostgresHook(postgres_conn_id=RESULT_DATABASE_CONNECTION_ID)
        source_db_hook = PostgresHook(postgres_conn_id=SOURCE_DATABASE_CONNECTION_ID)
        manager = UserInfoManager(result_db_hook, source_db_hook)
        manager.process_form_response_data()
        return True

    # Define the task dependencies
    tables_created = create_tables()
    auth_user_processed = process_auth_user_data(tables_created)
    form_responses_processed = process_form_responses(auth_user_processed)

    # Return final task for potential downstream dependencies
    return form_responses_processed


@dag(
    dag_id="unified_user_backfill_dag",
    schedule=None,  # Backfill only
    start_date=pendulum.datetime(2025, 5, 19, tz="UTC"),
        catchup=False,
    tags=["unified_user", "data_processing", "backfill"],
    default_args={
        "owner": "data_team",
        "retries":1,
        "retry_delay": pendulum.duration(minutes=5),
    },
        params={
                "start_id": Param(0, type="integer", minimum=0),
                "end_id": Param(1000, type="integer", minimum=0),
                "source_type": Param("AUTH_USER", enum=["AUTH_USER", "FORM_RESPONSE"])
        }
)
def unified_user_backfill_dag():
    """DAG for backfilling user information."""

    @task(task_id="create_tables")
    def create_tables() -> bool:
        """Create necessary tables if they don't exist."""
        result_db_hook = PostgresHook(postgres_conn_id=RESULT_DATABASE_CONNECTION_ID)
        source_db_hook = PostgresHook(postgres_conn_id=SOURCE_DATABASE_CONNECTION_ID)
        manager = UserInfoManager(result_db_hook, source_db_hook)
        manager.create_tables()
        return True

    @task(task_id="process_auth_user_data")
    def process_auth_user_data(tables_created: bool, **context) -> bool:
        """Process data from auth_user and related tables."""
        if not tables_created:
            raise ValueError("Table creation task failed")

        params = context["params"]
        start_id = params["start_id"]
        end_id = params["end_id"]
        source_type = params["source_type"]

        if source_type != "AUTH_USER":
            logger.info("Skipping auth_user processing as source_type is not AUTH_USER")
            return True

        result_db_hook = PostgresHook(postgres_conn_id=RESULT_DATABASE_CONNECTION_ID)
        source_db_hook = PostgresHook(postgres_conn_id=SOURCE_DATABASE_CONNECTION_ID)
        manager = UserInfoManager(result_db_hook, source_db_hook)
        manager.start_id = start_id
        manager.end_id = end_id
        manager.process_auth_user_data(backfill=True)

        return True

    @task(task_id="process_form_responses")
    def process_form_responses(auth_user_processed: bool, **context) -> bool:
        """Process data from marketing_genericformresponse."""
        if not auth_user_processed:
            raise ValueError("Auth user processing task failed")

        params = context["params"]
        start_id = params["start_id"]
        end_id = params["end_id"]
        source_type = params["source_type"]

        if source_type != "FORM_RESPONSE":
            logger.info("Skipping form response processing as source_type is not FORM_RESPONSE")
            return True

        result_db_hook = PostgresHook(postgres_conn_id=RESULT_DATABASE_CONNECTION_ID)
        source_db_hook = PostgresHook(postgres_conn_id=SOURCE_DATABASE_CONNECTION_ID)
        manager = UserInfoManager(result_db_hook, source_db_hook)
        manager.start_id = start_id
        manager.end_id = end_id
        manager.process_form_response_data(backfill=True)

        return True

    # Define the task dependencies
    tables_created = create_tables()
    auth_user_processed = process_auth_user_data(tables_created)
    form_responses_processed = process_form_responses(auth_user_processed)

    # Return final task for potential downstream dependencies
    return form_responses_processed


# Instantiate the DAG
unified_user_dag_instance = unified_user_dag()
unified_user_backfill_dag_instance = unified_user_backfill_dag()
