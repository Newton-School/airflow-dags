"""
etl.py  ·  Loads the last-7-days slice from the **source** DB,
resolves `unified_user_id` in the **result** DB, and upserts into
`course_x_user_info`.

Performance tweaks
------------------
* **ID-first sub-query**: fetch just the primary-key IDs for the batch,
  then join detail tables on those 5 000 (or N) rows only.
* **LEFT JOIN LATERAL** replaces the expensive CTE so we aggregate JSON
  form-responses one row at a time, hitting the PK index instead of
  scanning the whole 7-day window.
* No changes to the source schema or extra indexes required.
* Business-line counters & JSON merge are still handled by the trigger
  defined in `prepare_schema.sql`.
"""

import logging
from typing import List, Dict, Any

from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values, Json

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 1.  Batched fetch from the read-replica
# ---------------------------------------------------------------------------

FETCH_SQL = """
WITH
/* ---------------------------------------------------------------------------
 * STEP 0  – limit every downstream join to at most %(limit)s rows
 * ------------------------------------------------------------------------ */
ids AS (
    SELECT id, course_user_mapping_id, created_at
    FROM   apply_forms_courseuserapplyformmapping
    WHERE  created_at >= (CURRENT_DATE - INTERVAL '7 day')
       AND created_at <  CURRENT_DATE
    ORDER  BY id
    LIMIT  %(limit)s OFFSET %(offset)s
),

/* ---------------------------------------------------------------------------
 * STEP 1  – tiny in-memory table that maps question-IDs → descriptive keys
 * ------------------------------------------------------------------------ */
qmap(id, key) AS (
    VALUES
      (53 , 'current_work'),
      (100, 'yearly_salary'),
      (102, 'bachelor_qualification'),
      (62 , 'date_of_birth'),
      (110, '12th_passing_marks'),
      (3  , 'graduation_year'),
      (107, 'Data_science_joining_reason'),
      (17 , 'current_city'),
      (101, 'surety_on_learning_ds'),
      (95 , 'how_soon_you_can_join'),
      (104, 'where_you_get_to_know_about_NS'),
      (97 , 'work_experience'),
      (109, 'given_any_of_following_exam'),
      (103, 'department_worked_on')
)

/* ---------------------------------------------------------------------------
 * STEP 2  – full row payload
 * ------------------------------------------------------------------------ */
SELECT
    cafm.id        AS course_user_apply_form_mapping_id,
    cafm.created_at,
    cum.id         AS course_user_mapping_id,
    cum.user_id,
    au.email,
    up.phone,
    cum.course_id,
    cs.slug        AS coursestructure_slug,

    mas.max_all_test_cases_passed,
    asm.max_marks  AS max_assessment_marks,

    /* per-row JSON aggregation – string keys, one row at a time */
    fr.responses   AS form_responses

FROM  ids
JOIN  apply_forms_courseuserapplyformmapping cafm USING (id)

LEFT JOIN courses_courseusermapping  cum ON cum.id      = cafm.course_user_mapping_id
LEFT JOIN auth_user                  au  ON au.id       = cum.user_id
LEFT JOIN users_userprofile          up  ON up.user_id  = au.id
LEFT JOIN courses_course             c   ON c.id        = cum.course_id
LEFT JOIN courses_coursestructure    cs  ON cs.id       = c.course_structure_id

/* assignment MAX, type 2 */
LEFT JOIN (
    SELECT course_user_mapping_id,
           MAX(all_test_cases_passed_question_total_count)
               AS max_all_test_cases_passed
    FROM   assignments_assignmentcourseusermapping a
    JOIN   assignments_assignment b
           ON b.id = a.assignment_id
          AND b.assignment_type = 2
    GROUP  BY course_user_mapping_id
) mas ON mas.course_user_mapping_id = cum.id

/* assessment MAX, type 2 */
LEFT JOIN (
    SELECT course_user_mapping_id,
           MAX(marks) AS max_marks
    FROM   assessments_courseuserassessmentmapping a
    JOIN   assessments_assessment b
           ON b.id = a.assessment_id
          AND b.assessment_type = 2
    GROUP  BY course_user_mapping_id
) asm ON asm.course_user_mapping_id = cum.id

/* on-the-fly JSON aggregation for THIS row only */
LEFT JOIN LATERAL (
    SELECT jsonb_object_agg(
               qmap.key,               -- ← human-readable key
               cuafqm.response
           ) AS responses
    FROM   apply_forms_courseuserapplyformquestionmapping cuafqm
    JOIN   apply_forms_applyformquestionmapping afqm
           ON cuafqm.apply_form_question_mapping_id = afqm.id
    JOIN   qmap                                   -- filters & maps in one step
           ON qmap.id = afqm.apply_form_question_id
    WHERE  cuafqm.course_user_apply_form_mapping_id = cafm.id
) fr ON TRUE

ORDER BY cafm.id;
"""

COUNT_SQL = """
SELECT COUNT(*)
FROM apply_forms_courseuserapplyformmapping
WHERE created_at >= (CURRENT_DATE - INTERVAL '7 day')
  AND created_at <  CURRENT_DATE;
"""

TARGET_COLS = (
    "course_user_apply_form_mapping_id",
    "created_at",
    "course_user_mapping_id",
    "user_id",
    "email",
    "phone",
    "course_id",
    "coursestructure_slug",
    "max_all_test_cases_passed",
    "max_assessment_marks",
    "form_responses",
)

# ---------------------------------------------------------------------------
# 2.  Public entry point – called from the DAG
# ---------------------------------------------------------------------------

def load_last_7_days(fetch_batch: int = 5_000, insert_batch: int = 1_000):
    src = PostgresHook("postgres_read_replica")
    dst = PostgresHook("postgres_result_db")

    total = src.get_first(COUNT_SQL)[0]
    if total == 0:
        log.info("No new data for the last 7 days.")
        return

    log.info("Starting sync of %s rows …", total)
    offset = 0
    while offset < total:
        raw = src.get_records(
            FETCH_SQL, parameters=dict(limit=fetch_batch, offset=offset)
        )
        rows = [dict(zip(TARGET_COLS, r)) for r in raw]

        uid_map = _resolve_unified_ids(rows, dst)

        prepared: List[tuple] = []
        for r in rows:
            uid = (
                uid_map.get(("user_id", r["user_id"]))
                or uid_map.get(("phone", r["phone"]))
                or uid_map.get(("email", r["email"]))
            )
            if uid is None:
                continue                       # skip rows we cannot map
            prepared.append(_row_tuple(r, uid))

        _bulk_upsert(dst, prepared, insert_batch)

        offset += fetch_batch
        log.info(" → %s / %s rows processed", min(offset, total), total)

# ---------------------------------------------------------------------------
# 3.  Helpers
# ---------------------------------------------------------------------------

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
    answers = r["form_responses"] or {}
    return (
        r["course_user_apply_form_mapping_id"],
        r["created_at"],
        r["course_user_mapping_id"],
        unified_id,                       # resolved UID
        r["user_id"],
        r["email"],
        r["phone"],
        r["course_id"],
        r["coursestructure_slug"],
        r["max_all_test_cases_passed"],
        r["max_assessment_marks"],
        Json({k: v for k, v in answers.items() if v is not None}),
    )


def _bulk_upsert(hook: PostgresHook, rows: List[tuple], chunk: int):
    if not rows:
        return
    sql = """
    INSERT INTO course_x_user_info (
        course_user_apply_form_mapping_id,
        course_user_apply_form_mapping_created_at,
        course_user_mapping_id,
        unified_user_id,
        user_id,
        email,
        phone,
        course_id,
        coursestructure_slug,
        max_all_test_cases_passed,
        max_assessment_marks,
        form_responses
    )
    VALUES %s
    ON CONFLICT (course_user_apply_form_mapping_id)
    DO UPDATE SET
        unified_user_id           = EXCLUDED.unified_user_id,
        email                     = EXCLUDED.email,
        phone                     = EXCLUDED.phone,
        max_all_test_cases_passed = EXCLUDED.max_all_test_cases_passed,
        max_assessment_marks      = EXCLUDED.max_assessment_marks,
        form_responses            = course_x_user_info.form_responses
                                    || jsonb_strip_nulls(EXCLUDED.form_responses),
        updated_at                = CURRENT_TIMESTAMP;
    """
    with hook.get_conn() as conn, conn.cursor() as cur:
        for i in range(0, len(rows), chunk):
            execute_values(cur, sql, rows[i : i + chunk])
        conn.commit()
