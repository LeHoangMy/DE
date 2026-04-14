WITH stg AS (
    SELECT *
    FROM {{ ref('stg_summary') }}
),

ip_loc AS (
    SELECT
        ip
        , MOD(ABS(FARM_FINGERPRINT(CONCAT(
            COALESCE(country_code, ''), '|',
            COALESCE(city, ''), '|',
            COALESCE(region, '')
        ))), 10000000000) AS location_key
    FROM {{ ref('stg_ip_locations') }}
),

final AS (
    SELECT
        stg.*
        , COALESCE(ip_loc.location_key, -1) AS location_key
    FROM stg
    LEFT JOIN ip_loc
        ON stg.ip = ip_loc.ip
)

SELECT * FROM final
