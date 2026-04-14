WITH enriched AS (
    SELECT *
    FROM {{ ref('int_summary_enriched') }}
    WHERE collection = 'checkout_success'
        AND order_id IS NOT NULL
),

cart AS (
    SELECT
        enriched.order_id
        , TIMESTAMP_SECONDS(CAST(enriched.time_stamp AS INT64)) AS time_stamp
        , enriched.event_date
        , enriched.user_id_db
        , enriched.device_id
        , enriched.location_key
        , enriched.store_id
        , enriched.is_paypal
        , cp.product_id
        , CAST(cp.amount AS INT64) AS amount
        , SAFE_CAST(
            REGEXP_REPLACE(
                REGEXP_REPLACE(cp.price, r'[^\d,\.]', '')
                , r',(\d{2})$', r'.\1'
            ) AS NUMERIC
          ) AS price_local
        , CASE cp.currency
            WHEN '€' THEN 'EUR'
            WHEN '£' THEN 'GBP'
            WHEN '$' THEN 'USD'
            WHEN 'USD $' THEN 'USD'
            WHEN 'CAD $' THEN 'CAD'
            WHEN 'AU $' THEN 'AUD'
            WHEN 'NZD $' THEN 'NZD'
            WHEN 'HKD $' THEN 'HKD'
            WHEN 'SGD $' THEN 'SGD'
            WHEN 'MXN $' THEN 'MXN'
            WHEN 'COP $' THEN 'COP'
            WHEN 'DOP $' THEN 'DOP'
            WHEN 'CHF' THEN 'CHF'
            WHEN 'kr' THEN 'SEK'
            WHEN '₺' THEN 'TRY'
            WHEN '₹' THEN 'INR'
            WHEN '₫' THEN 'VND'
            WHEN '₱' THEN 'PHP'
            WHEN '₲' THEN 'PYG'
            WHEN 'R$' THEN 'BRL'
            WHEN '￥' THEN 'JPY'
            WHEN 'Kč' THEN 'CZK'
            WHEN 'Ft' THEN 'HUF'
            WHEN 'zł' THEN 'PLN'
            WHEN 'kn' THEN 'HRK'
            WHEN 'лв.' THEN 'BGN'
            WHEN 'Lei' THEN 'RON'
            WHEN 'CLP' THEN 'CLP'
            WHEN 'UYU' THEN 'UYU'
            WHEN 'CRC ₡' THEN 'CRC'
            WHEN 'GTQ Q' THEN 'GTQ'
            WHEN 'BOB Bs' THEN 'BOB'
            WHEN 'PEN S/.' THEN 'PEN'
            WHEN 'د.ك.‏' THEN 'KWD'
            WHEN ' din.' THEN 'RSD'
            ELSE 'UNKNOWN'
        END AS currency
        , cp.option AS cart_options
    FROM enriched
    , UNNEST(cart_products) AS cp
),

final AS (
    SELECT
        cart.order_id
        , cart.time_stamp
        , CAST(FORMAT_DATE('%Y%m%d', cart.event_date) AS INT64) AS date_key
        , cart.store_id
        , cart.is_paypal
        , cart.product_id
        , cart.amount
        , ROUND(cart.price_local * CASE cart.currency
            WHEN 'USD' THEN 1.0
            WHEN 'EUR' THEN 1.08
            WHEN 'GBP' THEN 1.27
            WHEN 'CHF' THEN 1.13
            WHEN 'CAD' THEN 0.74
            WHEN 'AUD' THEN 0.65
            WHEN 'NZD' THEN 0.61
            WHEN 'SGD' THEN 0.74
            WHEN 'HKD' THEN 0.13
            WHEN 'JPY' THEN 0.0067
            WHEN 'SEK' THEN 0.096
            WHEN 'PLN' THEN 0.25
            WHEN 'CZK' THEN 0.045
            WHEN 'HUF' THEN 0.0028
            WHEN 'RON' THEN 0.22
            WHEN 'BGN' THEN 0.55
            WHEN 'HRK' THEN 0.14
            WHEN 'TRY' THEN 0.031
            WHEN 'BRL' THEN 0.20
            WHEN 'MXN' THEN 0.058
            WHEN 'INR' THEN 0.012
            WHEN 'VND' THEN 0.000040
            WHEN 'PHP' THEN 0.018
            WHEN 'KWD' THEN 3.26
            WHEN 'RSD' THEN 0.0093
            WHEN 'CLP' THEN 0.0011
            WHEN 'COP' THEN 0.00025
            WHEN 'PEN' THEN 0.27
            WHEN 'UYU' THEN 0.026
            WHEN 'CRC' THEN 0.0019
            WHEN 'GTQ' THEN 0.13
            WHEN 'BOB' THEN 0.145
            WHEN 'DOP' THEN 0.017
            WHEN 'PYG' THEN 0.00014
            ELSE 1.0
        END, 2) AS price
        , cart.location_key
        , CASE
            WHEN cart.user_id_db IS NOT NULL AND cart.user_id_db != ''
            THEN MOD(ABS(FARM_FINGERPRINT(cart.user_id_db)), 10000000000)
            ELSE -1
        END AS customer_key
        , MOD(ABS(FARM_FINGERPRINT(
            COALESCE(cart.device_id, '')
        )), 10000000000) AS device_key
        , MOD(ABS(FARM_FINGERPRINT(
            COALESCE(cart.product_id, '')
        )), 10000000000) AS product_key
        , CASE
            WHEN ARRAY_LENGTH(cart.cart_options) >= 1
            THEN MOD(ABS(FARM_FINGERPRINT(CONCAT(
                COALESCE(cart.cart_options[OFFSET(0)].option_id, ''), '|',
                COALESCE(cart.cart_options[OFFSET(0)].value_id, '')
            ))), 10000000000)
            ELSE NULL
        END AS option_1_key
        , CASE
            WHEN ARRAY_LENGTH(cart.cart_options) >= 2
            THEN MOD(ABS(FARM_FINGERPRINT(CONCAT(
                COALESCE(cart.cart_options[OFFSET(1)].option_id, ''), '|',
                COALESCE(cart.cart_options[OFFSET(1)].value_id, '')
            ))), 10000000000)
            ELSE NULL
        END AS option_2_key
    FROM cart
)

SELECT * FROM final
