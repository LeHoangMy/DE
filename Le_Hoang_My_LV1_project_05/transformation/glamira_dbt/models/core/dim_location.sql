WITH source AS (
    SELECT DISTINCT
        country_code
        , country_name
        , city
        , region
    FROM {{ ref('stg_ip_locations') }}
    WHERE country_code IS NOT NULL
        AND city IS NOT NULL
),

final AS (
    SELECT
        MOD(ABS(FARM_FINGERPRINT(CONCAT(
            COALESCE(country_code, ''), '|',
            COALESCE(city, ''), '|',
            COALESCE(region, '')
        ))), 10000000000) AS location_key
        , country_code
        , country_name
        , city
        , region
    FROM source
)

SELECT * FROM final
