GENERIC_FORM_X_USER_INFO_SCHEMA = """
CREATE TABLE IF NOT EXISTS generic_form_response_x_user_info (
    id                              BIGSERIAL PRIMARY KEY,
    form_id                         BIGINT NOT NULL UNIQUE,
    form_created_at                 TIMESTAMP NOT NULL,
    unified_user_id                 BIGINT NOT NULL,
    business_line                   TEXT,
    business_line_x_user_info_id    BIGINT REFERENCES business_line_x_user_info(id),
    response_type                   TEXT NOT NULL,
    response                        JSONB DEFAULT '{}'::jsonb,
    user_id                         BIGINT,
    email                           TEXT,
    phone                           TEXT,
    created_at                      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at                      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

/* -------------------------------------------------------------------------- */
/*  TRIGGER – auto-create & maintain business_line_x_user_info                */
/* -------------------------------------------------------------------------- */

CREATE OR REPLACE FUNCTION generic_form_response_upsert_trg()
RETURNS trigger AS $$
DECLARE
    bl_id                   BIGINT;
    inc_ctr                 INT := CASE WHEN TG_OP = 'INSERT' THEN 1 ELSE 0 END;
    current_latest_ts       TIMESTAMP;
    is_latest_response      BOOLEAN;
BEGIN
    -- Insert or get existing business_line_x_user_info record
    INSERT INTO business_line_x_user_info (business_line, unified_user_id)
    VALUES (NEW.business_line, NEW.unified_user_id)
    ON CONFLICT (business_line, unified_user_id) DO UPDATE
        SET updated_at = NOW()
    RETURNING id INTO bl_id;
    
    -- Get the current latest timestamp for this business_line_x_user_info
    SELECT latest_counter_updated_at INTO current_latest_ts
    FROM business_line_x_user_info
    WHERE id = bl_id;
    
    -- Check if this is the latest response
    is_latest_response := (current_latest_ts IS NULL OR NEW.form_created_at > current_latest_ts);
    
    -- Update business_line_x_user_info
    UPDATE business_line_x_user_info bl
    SET counter = bl.counter + inc_ctr,
        -- Only update latest_counter_updated_at if this is the latest response
        latest_counter_updated_at = CASE 
            WHEN is_latest_response THEN NEW.form_created_at
            ELSE bl.latest_counter_updated_at
        END,
        -- Only append to responses if this is the latest response
        responses = CASE
            WHEN is_latest_response THEN bl.responses || jsonb_strip_nulls(NEW.response)
            ELSE bl.responses
        END,
        updated_at = NOW()
    WHERE bl.id = bl_id;
    
    -- Set the foreign key reference
    NEW.business_line_x_user_info_id := bl_id;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Drop existing trigger if it exists
DROP TRIGGER IF EXISTS trigger_generic_form_response_upsert ON generic_form_response_x_user_info;

-- Create trigger for INSERT and UPDATE operations
CREATE TRIGGER trigger_generic_form_response_upsert
    BEFORE INSERT OR UPDATE ON generic_form_response_x_user_info
    FOR EACH ROW 
    EXECUTE FUNCTION generic_form_response_upsert_trg();
"""
