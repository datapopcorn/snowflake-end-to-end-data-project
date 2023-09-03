{{ config(schema='customer') }}


WITH user_events AS (
    SELECT
        event_type,
        user_email,
        TO_TIMESTAMP_LTZ(event_timestamp) AS event_timestamp,
        event_json
    FROM 
        {{ source('customer', 'user_event') }}
)

SELECT
    event_type,
    user_email,
    event_timestamp,
    event_json
FROM 
    user_events

