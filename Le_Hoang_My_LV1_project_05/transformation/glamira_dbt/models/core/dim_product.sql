WITH source AS (
    SELECT DISTINCT
        product_id
        , product_name
        , product_name_en
        , price_current
        , price_original
        , price_min
        , price_max
        , currency
        , gender
    FROM {{ ref('stg_product') }}
    WHERE product_id IS NOT NULL
),

final AS (
    SELECT
        MOD(ABS(FARM_FINGERPRINT(
            COALESCE(product_id, '')
        )), 10000000000) AS product_key
        , product_id
        , product_name
        , product_name_en
        , ROUND(price_current * CASE currency
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
            ELSE 1.0
          END, 2) AS price_current
        , ROUND(price_original * CASE currency
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
            ELSE 1.0
          END, 2) AS price_original
        , ROUND(price_min * CASE currency
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
            ELSE 1.0
          END, 2) AS price_min
        , ROUND(price_max * CASE currency
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
            ELSE 1.0
          END, 2) AS price_max
        , CASE
            WHEN LOWER(gender) IN ('male', 'men') THEN 'Men'
            WHEN LOWER(gender) IN ('female', 'women') THEN 'Women'
            ELSE 'Unisex'
          END AS gender
    FROM source
)

SELECT * FROM final
