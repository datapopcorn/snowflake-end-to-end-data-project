{{ config(schema='customer') }}

WITH signup_events AS (
    SELECT
        event_type,
        user_email,
        event_timestamp as signup_timestamp,
        event_json
    FROM 
        {{ ref('stg_user_events') }}
    WHERE
        event_type = 'signup'
),

json_extract_password_and_phone_number AS (
    SELECT
        user_email,
        signup_timestamp,
        JSON_EXTRACT_PATH_TEXT(EVENT_JSON, 'password') as password,
        JSON_EXTRACT_PATH_TEXT(EVENT_JSON, 'phone_number') as phone_number
    FROM 
        signup_events
)

SELECT
    user_email,
    signup_timestamp,
    password,
    phone_number
FROM 
    json_extract_password_and_phone_number