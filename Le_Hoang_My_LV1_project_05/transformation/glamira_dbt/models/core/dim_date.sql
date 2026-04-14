WITH dates AS (
    SELECT DISTINCT event_date AS date
    FROM {{ ref("int_summary_enriched") }}
    WHERE event_date IS NOT NULL
),

final AS (
    SELECT
        CAST(FORMAT_DATE('%Y%m%d', date) AS INT64) AS date_key
        , date
        , CAST(FORMAT_DATE('%Y%m', date) AS INT64) AS year_month
        , CONCAT(
            CAST(EXTRACT(YEAR FROM date) AS STRING)
            , '-Q'
            , CAST(EXTRACT(QUARTER FROM date) AS STRING)
        ) AS year_quarter
        , DATE_TRUNC(date, MONTH) AS first_day_of_month
    FROM dates
)

SELECT * FROM final
