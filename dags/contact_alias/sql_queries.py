"""
SQL queries for the contact alias system.
"""

# Table definition queries
TABLE_QUERIES = {
        "INITIALIZE_TABLE": """
        CREATE TABLE IF NOT EXISTS contact_aliases (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            identity_group_id UUID NOT NULL,          -- All contacts for same person share this
            email VARCHAR(512),
            phone VARCHAR(15),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT ck_contact_presence CHECK (
                (email IS NOT NULL) OR (phone IS NOT NULL)
            ),
            CONSTRAINT uq_email_phone UNIQUE (email, phone)
        );

        -- Indexes for performance
        DO $$
        BEGIN
            -- Check if email index exists
            IF NOT EXISTS (
                SELECT 1 FROM pg_indexes 
                WHERE indexname = 'idx_contact_email'
            ) THEN
                CREATE INDEX idx_contact_email ON contact_aliases(email) 
                WHERE email IS NOT NULL;
            END IF;

            -- Check if phone index exists
            IF NOT EXISTS (
                SELECT 1 FROM pg_indexes 
                WHERE indexname = 'idx_contact_phone'
            ) THEN
                CREATE INDEX idx_contact_phone ON contact_aliases(phone) 
                WHERE phone IS NOT NULL;
            END IF;
        END
        $$;
    """
}

# Auth user related queries
AUTH_USER_QUERIES = {
        "TOTAL_COUNT_ALL": """
        SELECT COUNT(*) 
        FROM auth_user 
        WHERE date_joined < DATE(NOW());
    """,

        "TOTAL_COUNT_YESTERDAY": """
        SELECT COUNT(*) 
        FROM auth_user 
        WHERE date_joined::date = (CURRENT_DATE - INTERVAL '1 day')::date;
    """,

        "FETCH_USER_DATA_ALL": """
        SELECT 
            auth_user.id AS user_id,
            email,
            phone
        FROM auth_user 
        LEFT JOIN users_userprofile ON auth_user.id = users_userprofile.user_id
        WHERE date_joined < DATE(NOW())
        LIMIT %s OFFSET %s;
    """,

        "FETCH_USER_DATA_YESTERDAY": """
        SELECT 
            auth_user.id AS user_id,
            email,
            phone
        FROM auth_user 
        LEFT JOIN users_userprofile ON auth_user.id = users_userprofile.user_id
        WHERE date_joined::date = (CURRENT_DATE - INTERVAL '1 day')::date
        LIMIT %s OFFSET %s;
    """
}

# Generic form responses related queries
FORM_RESPONSE_QUERIES = {
        "TOTAL_COUNT_ALL": """
        SELECT COUNT(*) 
        FROM marketing_genericformresponse
        WHERE
          (
            (
              response_json ? 'email'
              AND NULLIF(TRIM(response_json ->> 'email'), '') IS NOT NULL
              AND LOWER(TRIM(response_json ->> 'email')) <> 'null'
            )
            OR (
              response_json ? 'phone_number'
              AND NULLIF(TRIM(response_json ->> 'phone_number'), '') IS NOT NULL
              AND LOWER(TRIM(response_json ->> 'phone_number')) <> 'null'
            )
          )
          AND created_at < DATE(NOW());
    """,

        "TOTAL_COUNT_YESTERDAY": """
        SELECT COUNT(*) 
        FROM marketing_genericformresponse
        WHERE
          (
            (
              response_json ? 'email'
              AND NULLIF(TRIM(response_json ->> 'email'), '') IS NOT NULL
              AND LOWER(TRIM(response_json ->> 'email')) <> 'null'
            )
            OR (
              response_json ? 'phone_number'
              AND NULLIF(TRIM(response_json ->> 'phone_number'), '') IS NOT NULL
              AND LOWER(TRIM(response_json ->> 'phone_number')) <> 'null'
            )
          )
          AND created_at::date = (CURRENT_DATE - INTERVAL '1 day')::date;
    """,

        "FETCH_USER_DATA_ALL": """
        SELECT
          response_json ->> 'email' AS email,
          response_json ->> 'phone_number' AS phone
        FROM
          marketing_genericformresponse
        WHERE
          (
            (
              response_json ? 'email'
              AND NULLIF(TRIM(response_json ->> 'email'), '') IS NOT NULL
              AND LOWER(TRIM(response_json ->> 'email')) <> 'null'
            )
            OR (
              response_json ? 'phone_number'
              AND NULLIF(TRIM(response_json ->> 'phone_number'), '') IS NOT NULL
              AND LOWER(TRIM(response_json ->> 'phone_number')) <> 'null'
            )
          )
          AND created_at < DATE(NOW())
        LIMIT %s OFFSET %s;
    """,

        "FETCH_USER_DATA_YESTERDAY": """
        SELECT
          response_json ->> 'email' AS email,
          response_json ->> 'phone_number' AS phone
        FROM
          marketing_genericformresponse
        WHERE
          (
            (
              response_json ? 'email'
              AND NULLIF(TRIM(response_json ->> 'email'), '') IS NOT NULL
              AND LOWER(TRIM(response_json ->> 'email')) <> 'null'
            )
            OR (
              response_json ? 'phone_number'
              AND NULLIF(TRIM(response_json ->> 'phone_number'), '') IS NOT NULL
              AND LOWER(TRIM(response_json ->> 'phone_number')) <> 'null'
            )
          )
          AND created_at::date = (CURRENT_DATE - INTERVAL '1 day')::date
        LIMIT %s OFFSET %s;
    """
}

# Contact alias related queries
CONTACT_ALIAS_QUERIES = {
        "FETCH_BY_EMAIL_OR_PHONE": """
        SELECT id, identity_group_id, user_id 
        FROM contact_aliases 
        WHERE email = %s OR phone = %s
        ORDER BY created_at ASC;
    """,

        "FETCH_EXACT_MATCH": """
        SELECT id, user_id 
        FROM contact_aliases 
        WHERE email = %s AND phone = %s;
    """,

        "UPDATE_USER_ID": """
        UPDATE contact_aliases 
        SET user_id = %s 
        WHERE id = %s;
    """,

        "UPDATE_IDENTITY_GROUP": """
        UPDATE contact_aliases 
        SET identity_group_id = %s 
        WHERE identity_group_id = %s;
    """,

        "UPDATE_IDENTITY_GROUP_MULTIPLE": """
        UPDATE contact_aliases 
        SET identity_group_id = %s 
        WHERE identity_group_id IN %s;
    """,

        "UPDATE_USER_ID_FOR_GROUP": """
        UPDATE contact_aliases 
        SET user_id = %s 
        WHERE identity_group_id = %s AND user_id IS NULL;
    """,

        "INSERT_CONTACT_ALIAS": """
        INSERT INTO contact_aliases (user_id, email, phone, identity_group_id) 
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (email, phone) DO NOTHING;
    """
}

INITIALIZE_TABLE_QUERY = TABLE_QUERIES["INITIALIZE_TABLE"]
TOTAL_USER_COUNT_QUERY = AUTH_USER_QUERIES["TOTAL_COUNT_ALL"]
FETCH_USER_DATA_FROM_AUTH_USER_TABLE_QUERY = AUTH_USER_QUERIES["FETCH_USER_DATA_ALL"]
FETCH_USER_DATA_FROM_GENERIC_FORM_RESPONSES_QUERY = FORM_RESPONSE_QUERIES["FETCH_USER_DATA_ALL"]
TOTAL_GENERIC_FORM_RESPONSES_COUNT_QUERY = FORM_RESPONSE_QUERIES["TOTAL_COUNT_ALL"]
FETCH_ALL_CONTACT_ALIASES_QUERY = CONTACT_ALIAS_QUERIES["FETCH_BY_EMAIL_OR_PHONE"]
