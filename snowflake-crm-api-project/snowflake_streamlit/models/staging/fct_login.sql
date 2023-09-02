{{ config(schema='customer') }}

WITH login_events AS (
    SELECT
        user_email,
        event_timestamp as login_timestamp
    FROM 
        {{ ref('stg_user_events') }}
    WHERE
        event_type = 'login'
)

SELECT 
    user_email,
    login_timestamp
FROM
    login_events

