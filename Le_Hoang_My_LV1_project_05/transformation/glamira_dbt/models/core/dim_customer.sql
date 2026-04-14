WITH source AS (
    SELECT
        user_id_db
        , email_address
        , time_stamp
        , ROW_NUMBER() OVER (
            PARTITION BY user_id_db
            ORDER BY
                CASE WHEN email_address IS NOT NULL AND email_address != '' THEN 0 ELSE 1 END ASC
                , time_stamp DESC
        ) AS rn
    FROM {{ ref("int_summary_enriched") }}
    WHERE user_id_db IS NOT NULL
        AND user_id_db != ''
),

final AS (
    SELECT
        ABS(FARM_FINGERPRINT(
            COALESCE(user_id_db, '')
        )) AS customer_key
        , user_id_db
        , NULLIF(email_address, '') AS email_address
    FROM source
    WHERE rn = 1
)

SELECT * FROM final
