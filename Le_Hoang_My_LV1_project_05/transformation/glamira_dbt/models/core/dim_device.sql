WITH source AS (
    SELECT
        device_id
        , user_agent
        , ROW_NUMBER() OVER (
            PARTITION BY device_id
            ORDER BY time_stamp DESC
        ) AS rn
    FROM {{ ref("int_summary_enriched") }}
    WHERE device_id IS NOT NULL
        AND device_id != ''
),

final AS (
    SELECT
        ABS(FARM_FINGERPRINT(
            COALESCE(device_id, '')
        )) AS device_key
        , device_id
        , user_agent
        , CASE
            WHEN REGEXP_CONTAINS(user_agent, r'(?i)bot|crawler|spider|facebook|pinterest|hexometer|dataprovider|google-shopping|google-adwords|bingpreview|woorank') THEN 'Bot'
            WHEN REGEXP_CONTAINS(user_agent, r'(?i)tablet|ipad|bb10.*kbd') THEN 'Tablet'
            WHEN REGEXP_CONTAINS(user_agent, r'(?i)mobile|android|iphone|kaios') THEN 'Mobile'
            ELSE 'Desktop'
        END AS device_type
        , CASE
            WHEN REGEXP_CONTAINS(user_agent, r'(?i)android') THEN 'Android'
            WHEN REGEXP_CONTAINS(user_agent, r'(?i)iphone|ipad') THEN 'iOS'
            WHEN REGEXP_CONTAINS(user_agent, r'(?i)windows') THEN 'Windows'
            WHEN REGEXP_CONTAINS(user_agent, r'(?i)macintosh|mac os|MAC') THEN 'Mac'
            WHEN REGEXP_CONTAINS(user_agent, r'(?i)cros') THEN 'ChromeOS'
            WHEN REGEXP_CONTAINS(user_agent, r'(?i)linux') THEN 'Linux'
            WHEN REGEXP_CONTAINS(user_agent, r'(?i)playstation') THEN 'PlayStation'
            WHEN REGEXP_CONTAINS(user_agent, r'(?i)kaios') THEN 'KaiOS'
            WHEN REGEXP_CONTAINS(user_agent, r'(?i)bb10') THEN 'BlackBerry'
            ELSE 'Other'
        END AS os
        , CASE
            WHEN REGEXP_CONTAINS(user_agent, r'(?i)edg') THEN 'Edge'
            WHEN REGEXP_CONTAINS(user_agent, r'(?i)trident|MSIE') THEN 'IE'
            WHEN REGEXP_CONTAINS(user_agent, r'(?i)FBAN|FBIOS') THEN 'Facebook'
            WHEN REGEXP_CONTAINS(user_agent, r'(?i)bingpreview') THEN 'Bing'
            WHEN REGEXP_CONTAINS(user_agent, r'(?i)chrome') THEN 'Chrome'
            WHEN REGEXP_CONTAINS(user_agent, r'(?i)firefox') THEN 'Firefox'
            WHEN REGEXP_CONTAINS(user_agent, r'(?i)safari') THEN 'Safari'
            ELSE 'Other'
        END AS browser
    FROM source
    WHERE rn = 1
)

SELECT * FROM final
