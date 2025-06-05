COURSE_X_USER_INFO_SCHEMA = """

CREATE TABLE IF NOT EXISTS course_structure_business_line (
    coursestructure_slug TEXT PRIMARY KEY,
    business_line        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS business_line_x_user_info (
    id                    BIGSERIAL PRIMARY KEY,
    business_line         TEXT NOT NULL,
    unified_user_id       BIGINT NOT NULL,
    counter               INTEGER    DEFAULT 0,
    latest_counter_updated_at TIMESTAMP,
    responses             JSONB      DEFAULT '{}'::jsonb,
    created_at            TIMESTAMP  DEFAULT CURRENT_TIMESTAMP,
    updated_at            TIMESTAMP  DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT bl_user_unique UNIQUE (business_line, unified_user_id)
);

CREATE TABLE IF NOT EXISTS course_x_user_info (
    course_user_apply_form_mapping_id       BIGINT PRIMARY KEY,
    course_user_apply_form_mapping_created_at TIMESTAMP,
    course_user_mapping_id                  BIGINT,
    unified_user_id                         BIGINT,
    user_id                                 BIGINT,
    email                                   TEXT,
    phone                                   TEXT,
    course_id                               BIGINT,
    coursestructure_slug                    TEXT,
    max_all_test_cases_passed               INTEGER,
    max_assessment_marks                    NUMERIC(10,2),
    form_responses                          JSONB,
    business_line_x_user_info_id            BIGINT REFERENCES business_line_x_user_info(id),
    created_at                              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at                              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

/* -------------------------------------------------------------------------- */
/*  TRIGGER – auto-create & maintain business_line_x_user_info                */
/* -------------------------------------------------------------------------- */

CREATE OR REPLACE FUNCTION bl_upsert_trg()
RETURNS trigger AS $$
DECLARE
    bl_id   BIGINT;
    bl_name TEXT;
    inc_ctr INT := CASE WHEN TG_OP = 'INSERT' THEN 1 ELSE 0 END;
BEGIN
    /* 1. slug → business_line */
    SELECT business_line
      INTO bl_name
      FROM course_structure_business_line
     WHERE coursestructure_slug = NEW.coursestructure_slug;

    IF bl_name IS NULL THEN             -- mapping missing: skip BL logic
        RETURN NEW;
    END IF;

    /* 2. ensure BL row exists / grab id */
    INSERT INTO business_line_x_user_info (business_line, unified_user_id)
         VALUES (bl_name, NEW.unified_user_id)
    ON CONFLICT (business_line, unified_user_id) DO
        UPDATE SET updated_at = NOW()
    RETURNING id INTO bl_id;

    /* 3. increment counter & merge JSONB (non-destructive) */
    UPDATE business_line_x_user_info bl
       SET counter = bl.counter + inc_ctr,
           latest_counter_updated_at = GREATEST(
                 COALESCE(bl.latest_counter_updated_at,'epoch'),
                 NEW.course_user_apply_form_mapping_created_at),
           responses = bl.responses
                      || jsonb_strip_nulls(NEW.form_responses),
           updated_at = NOW()
     WHERE bl.id = bl_id;

    /* 4. propagate FK back */
    NEW.business_line_x_user_info_id := bl_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trig_bl_upsert ON course_x_user_info;
CREATE TRIGGER trig_bl_upsert
AFTER INSERT OR UPDATE ON course_x_user_info
FOR EACH ROW EXECUTE FUNCTION bl_upsert_trg();
"""
