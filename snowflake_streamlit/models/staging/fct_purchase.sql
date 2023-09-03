{{ config(schema='customer') }}

WITH purchase_events AS (
    SELECT
        event_type,
        user_email,
        event_timestamp as purchase_timestamp,
        event_json
    FROM 
        {{ ref('stg_user_events') }}
    WHERE
        event_type = 'purchase'
),

json_extract_product_name_and_quantity AS (
    SELECT
        user_email,
        purchase_timestamp,
        JSON_EXTRACT_PATH_TEXT(event_json, 'product_name') as product_name,
        JSON_EXTRACT_PATH_TEXT(event_json, 'quantity') as quantity
    FROM 
        purchase_events
)

SELECT
    user_email,
    purchase_timestamp,
    product_name,
    quantity
FROM 
    json_extract_product_name_and_quantity
