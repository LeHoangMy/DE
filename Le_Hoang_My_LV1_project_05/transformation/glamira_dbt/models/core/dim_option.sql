WITH source AS (
    SELECT DISTINCT
        opt.option_id
        , opt.option_label
        , opt.value_id
        , opt.value_label
    FROM {{ ref("int_summary_enriched") }}
    , UNNEST(cart_products) AS cp
    , UNNEST(cp.option) AS opt
    WHERE opt.option_id IS NOT NULL
        AND opt.value_id IS NOT NULL
),

final AS (
    SELECT
        MOD(ABS(FARM_FINGERPRINT(CONCAT(
            COALESCE(option_id, ''), '|',
            COALESCE(value_id, '')
        ))), 10000000000) AS option_key
        , option_id
        , option_label
        , value_id
        , value_label
    FROM source
)

SELECT * FROM final
