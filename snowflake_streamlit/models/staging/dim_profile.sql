{{ config(schema='customer') }}

WITH signup AS (
    SELECT
        user_email,
        signup_timestamp,
        password,
        phone_number
    FROM 
        {{ ref('fct_signup') }}
),

login AS (
    SELECT
        user_email,
        login_timestamp
    FROM 
        {{ ref('fct_login') }}
),

join_last_login_time AS (
    SELECT
        signup.user_email,
        signup.signup_timestamp,
        -- Get the last login time for each user if null then use signup time
        COALESCE(
            MAX(login.login_timestamp) OVER (PARTITION BY login.user_email),
            signup.signup_timestamp
        ) as last_login_timestamp,
        signup.password,
        signup.phone_number,
        ROW_NUMBER() OVER (PARTITION BY signup.user_email ORDER BY signup.signup_timestamp DESC) as row_number
    FROM 
        signup
    LEFT JOIN
        login
    ON
        signup.user_email = login.user_email
)

SELECT
    user_email,
    signup_timestamp,
    last_login_timestamp,
    password,
    phone_number
FROM 
    join_last_login_time
WHERE 
    row_number = 1
