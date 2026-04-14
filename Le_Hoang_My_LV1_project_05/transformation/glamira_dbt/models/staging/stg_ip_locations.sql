WITH source AS (
    SELECT *
    FROM {{ source('glamira_raw', 'ip_locations') }}
),

final AS (
    SELECT
        ip
        , country_code
        , country_name
        , region
        , city
    FROM source
    WHERE ip IS NOT NULL
)

SELECT * FROM final
