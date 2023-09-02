{{ config(schema='customer') }}

WITH purchase AS (
    SELECT
        user_email,
        purchase_timestamp,
        product_name,
        quantity
    FROM 
        {{ ref('fct_purchase') }}
),


sum_product_sold AS(
    SELECT
        product_name,
        SUM(quantity) as total_sold
    FROM 
        purchase
    GROUP BY 
        product_name
),

count_buyer_num_by_product AS (
    SELECT
        product_name,
        COUNT(DISTINCT user_email) as buyer_num
    FROM 
        purchase
    GROUP BY 
        product_name
),

first_and_latest_purchase_timestamp_by_product AS (
    SELECT
        product_name,
        MIN(purchase_timestamp) as first_purchase_timestamp,
        MAX(purchase_timestamp) as latest_purchase_timestamp
    FROM 
        purchase
    GROUP BY 
        product_name
),

first_and_latest_purchase_timestamp_by_product_with_total_sold_and_total_buyer AS (
    SELECT
        first_and_latest_purchase_timestamp_by_product.product_name,
        first_and_latest_purchase_timestamp_by_product.first_purchase_timestamp,
        first_and_latest_purchase_timestamp_by_product.latest_purchase_timestamp,
        sum_product_sold.total_sold,
        count_buyer_num_by_product.buyer_num
    FROM
        first_and_latest_purchase_timestamp_by_product
    LEFT JOIN 
        sum_product_sold
    ON
        first_and_latest_purchase_timestamp_by_product.product_name = sum_product_sold.product_name
    LEFT JOIN
        count_buyer_num_by_product
    ON
        first_and_latest_purchase_timestamp_by_product.product_name = count_buyer_num_by_product.product_name
)

SELECT
    product_name,
    first_purchase_timestamp,
    latest_purchase_timestamp,
    total_sold,
    buyer_num
FROM 
    first_and_latest_purchase_timestamp_by_product_with_total_sold_and_total_buyer