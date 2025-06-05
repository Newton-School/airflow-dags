import logging
from typing import List, Dict, Any, Tuple

from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2._json import Json
from psycopg2.extras import execute_values

log = logging.getLogger(__name__)

FETCH_SQL = """
WITH form_responses AS (
  SELECT
    id AS form_id,
    user_id,
    created_at,
    response_type,
    response_json,
    COALESCE(
      response_json #>> '{email,value}',
      response_json #>> '{email}'
    ) AS email,
    COALESCE(
      response_json #>> '{phone,value}',
      response_json #>> '{phone}'
    ) AS phone,
    response_json ->> 'course_type_interested_in' AS course_type_interested_in,
    -- Calculate business_line
    COALESCE(
      -- First check response_type mapping
      CASE response_type
        WHEN 'DS_TIMELINE_REQUEST_CALLBACK_FORM' THEN 'DS'
        WHEN 'MASTER_CLASS_REQUEST_CALLBACK_FORM' THEN 'DS'
        WHEN 'PUBLIC_FULLSTACK_WEBSITE_HOME_REQUEST_CALLBACK_FORM' THEN 'FSD'
        WHEN 'PUBLIC_WEBSITE_DS_HOME_REQUEST_CALLBACK_FORM' THEN 'DS'
        WHEN 'SAT_REQUEST_CALLBACK_FORM' THEN 'DS'
        ELSE NULL
      END,
      -- If null, check course_type_interested_in mapping
      CASE response_json ->> 'course_type_interested_in'
        WHEN 'btech_in_cs_ai_from_nst' THEN 'NST'
        WHEN 'certification_full_stack_development' THEN 'FSD'
        WHEN 'professional_certification_in_ds_ai' THEN 'DS'
        ELSE NULL
      END
    ) AS business_line,
    jsonb_strip_nulls(
      jsonb_build_object(
        'current_status',
        COALESCE(
          response_json #>> '{current_status,value}',
          response_json #>> '{current_status}'
        ),
        'graduation_year',
        COALESCE(
          response_json #>> '{graduation_year,value}',
          response_json #>> '{graduation_year}'
        ),
        'highest_qualification',
        COALESCE(
          response_json #>> '{highest_qualification,value}',
          response_json #>> '{highest_qualification}'
        ),
        'degree',
        COALESCE(
          response_json #>> '{degree,value}',
          response_json #>> '{degree}'
        ),
        'current_role',
        COALESCE(
          response_json #>> '{current_role,value}',
          response_json #>> '{current_role}'
        )
      )
    ) AS processed_response_json
  FROM
    marketing_genericformresponse
  WHERE
    created_at >= NOW() - INTERVAL '1 day'
)
SELECT * FROM form_responses
WHERE business_line IS NOT NULL
LIMIT %(limit)s OFFSET %(offset)s;
"""

COUNT_SQL = """
SELECT COUNT(*) FROM marketing_genericformresponse
WHERE created_at >= NOW() - INTERVAL '1 day';
"""

TARGET_COLS = (
    'form_id',
    'user_id',
    'created_at',
    'response_type',
    'response_json',
    'email',
    'phone',
    'course_type_interested_in',
    'business_line',
    'processed_response_json'
)

def load_data(fetch_batch: int = 2000, insert_batch: int = 1000):
    src = PostgresHook(postgres_conn_id='postgres_read_replica')
    dst = PostgresHook(postgres_conn_id='postgres_result_db')

    total = src.get_first(COUNT_SQL)[0]
    if total == 0:
        log.info("No new data to process.")
        return

    log.info(f"Total records to process: {total}")
    offset = 0
    while offset < total:
        raw = src.get_records(
                FETCH_SQL, parameters={'limit': fetch_batch, 'offset': offset}
        )
        rows = [dict(zip(TARGET_COLS, row)) for row in raw]

        uid_map = _resolve_unified_ids(rows, dst)

        prepared: List[Tuple] = []
        for row in rows:
            uid = (
                    uid_map.get(("user_id", row["user_id"]))
                    or uid_map.get(("phone", row["phone"]))
                    or uid_map.get(("email", row["email"]))
            )
            if not uid:
                log.warning(f"Skipping row with no unified user ID: {row}")
                continue
            prepared.append(_row_tuple(row, uid))

        _bulk_upsert(dst, prepared, insert_batch)

        offset += fetch_batch
        log.info(" → %s / %s rows processed", min(offset, total), total)


def _resolve_unified_ids(rows: List[Dict[str, Any]], hook: PostgresHook) -> Dict[tuple, int]:
    """Return {('user_id', val)|('phone', val)|('email', val) : unified_user.id}."""
    user_ids = {r["user_id"] for r in rows if r["user_id"]}
    phones   = {r["phone"]   for r in rows if r["phone"]}
    emails   = {r["email"]   for r in rows if r["email"]}

    mapping: Dict[tuple, int] = {}
    with hook.get_conn() as conn, conn.cursor() as cur:
        if user_ids:
            cur.execute(
                "SELECT user_id, id FROM unified_user WHERE user_id = ANY(%s);",
                (list(user_ids),),
            )
            mapping.update({("user_id", u): i for u, i in cur.fetchall()})

        if phones:
            cur.execute(
                """
                SELECT phone, id
                FROM unified_user
                WHERE phone = ANY(%s)
                  AND user_id IS NULL;
                """,
                (list(phones),),
            )
            mapping.update({("phone", p): i for p, i in cur.fetchall()})

        if emails:
            cur.execute(
                """
                SELECT email, id
                FROM unified_user
                WHERE email = ANY(%s)
                  AND user_id IS NULL
                  AND phone IS NULL;
                """,
                (list(emails),),
            )
            mapping.update({("email", e): i for e, i in cur.fetchall()})
    return mapping

def _row_tuple(r: Dict[str, Any], unified_id: int) -> tuple:
    answers = r["processed_response_json"] or {}
    return (
        r["form_id"],
        r["created_at"],
        unified_id,
        r["business_line"],
        r["response_type"],
        r["email"],
        r["phone"],
        Json({k: v for k, v in answers.items() if v is not None}),
    )


def _bulk_upsert(hook: PostgresHook, rows: List[tuple], chunk: int):
    if not rows:
        return
    sql = """
    INSERT INTO generic_form_response_x_user_info (
        form_id,
        form_created_at,
        unified_user_id,
        business_line,
        response_type,
        email,
        phone,
        response
    )
    VALUES %s
    ON CONFLICT (form_id)
    DO UPDATE SET
        unified_user_id           = EXCLUDED.unified_user_id,
        email                     = EXCLUDED.email,
        phone                     = EXCLUDED.phone,
        response                  = generic_form_response_x_user_info.response
                                    || jsonb_strip_nulls(EXCLUDED.response),
        updated_at                = CURRENT_TIMESTAMP;
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        for i in range(0, len(rows), chunk):
            execute_values(cur, sql, rows[i : i + chunk])
        conn.commit()
