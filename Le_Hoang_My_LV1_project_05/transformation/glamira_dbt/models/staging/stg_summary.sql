WITH source AS (
    SELECT *
    FROM {{ source('glamira_raw', 'summary_v2') }}
),

deduped AS (
    SELECT *
        , ROW_NUMBER() OVER (
            PARTITION BY time_stamp, ip, collection
            ORDER BY time_stamp
        ) AS row_num
    FROM source
),

final AS (
    SELECT
        time_stamp
        , TIMESTAMP_SECONDS(CAST(time_stamp AS INT64)) AS event_timestamp
        , DATE(TIMESTAMP_SECONDS(CAST(time_stamp AS INT64))) AS event_date
        , ip
        , user_agent
        , user_id_db
        , device_id
        , store_id
        , collection
        , order_id
        , product_id
        , CAST(REPLACE(price, ',', '.') AS NUMERIC) AS price
        , currency
        , is_paypal
        , email_address
        , option
        , cart_products
    FROM deduped
    WHERE row_num = 1
)

SELECT * FROM final
