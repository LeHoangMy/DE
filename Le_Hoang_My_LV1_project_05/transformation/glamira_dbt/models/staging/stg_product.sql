WITH source AS (
    SELECT *
    FROM {{ source('glamira_raw', 'product_data') }}
),

final AS (
    SELECT
        product_id
        , product_name
        , product_name_en
        , price_current
        , price_original
        , price_min
        , price_max
        , currency
        , gender
        , alloy
        , stone
        , diamond
    FROM source
    WHERE product_id IS NOT NULL
)

SELECT * FROM final
