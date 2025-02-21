
-- Create a query to return a single row per day for each login_hash, server_hash, symbol, and currency
-- for every day in June, July, August, and September 2020, even if there is no data

WITH date_series AS (
    SELECT DATEADD(DAY, ROW_NUMBER() OVER (ORDER BY SEQ4()), '2020-05-31') AS dt_report
    FROM TABLE(GENERATOR(ROWCOUNT => 122)) -- 1 June to 30 September
),
filtered_users AS (
    SELECT DISTINCT login_hash, server_hash, currency
    FROM TESTDATA.PUBLIC.USERS
    WHERE enable = 1
),
daily_combinations AS (
    SELECT 
        ds.dt_report,
        fu.login_hash,
        fu.server_hash,
        t.symbol,
        fu.currency,
        MIN(TO_TIMESTAMP_NTZ(t.close_time)) OVER (
            PARTITION BY fu.login_hash, fu.server_hash, t.symbol
            ) AS original_date_first_trade,
    FROM date_series ds
    CROSS JOIN filtered_users fu
    CROSS JOIN (SELECT login_hash, server_hash, symbol, close_time FROM TESTDATA.PUBLIC.TRADES) t
    WHERE fu.login_hash = t.login_hash AND fu.server_hash = t.server_hash
),
daily_volume_aggregates AS (
    SELECT      
        DATE_TRUNC('DAY', TO_DATE(close_time)) AS dt_report,
        login_hash,
        server_hash,
        symbol,
        COUNT(*) AS trade_count,
        SUM(volume) AS daily_volume,      
    FROM TESTDATA.PUBLIC.TRADES
    WHERE symbol NOT LIKE '%,%' AND volume != 0 AND contractsize IS NOT NULL
    GROUP BY dt_report,login_hash,server_hash,symbol
),
volume_aggregates AS (
    SELECT 
    *,
    SUM(daily_volume) OVER (
        PARTITION BY login_hash,server_hash,symbol
        ORDER BY dt_report
        RANGE BETWEEN INTERVAL '6 DAY' PRECEDING AND CURRENT ROW
        ) AS sum_volume_prev_7d,
    COUNT(*) OVER (
        PARTITION BY login_hash,server_hash,symbol
        ORDER BY dt_report
        RANGE BETWEEN INTERVAL '6 DAY' PRECEDING AND CURRENT ROW
        ) AS trade_count,
    SUM(daily_volume) OVER (
        ORDER BY dt_report 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS sum_volume_prev_all,
    SUM(case WHEN dt_report BETWEEN '2020-08-01' AND '2020-08-31' THEN daily_volume ELSE 0 
        END) OVER (
        PARTITION BY login_hash, server_hash, symbol
        ORDER BY dt_report
        ) AS sum_volume_2020_08,
    DENSE_RANK() OVER (
        PARTITION BY dt_report 
        ORDER BY login_hash DESC
        ) AS rank_count_prev_7d,
FROM daily_volume_aggregates
ORDER BY dt_report,login_hash,server_hash,symbol
)
SELECT
    dc.dt_report::TIMESTAMP AS dt_report,
    dc.login_hash::STRING AS login_hash,
    dc.server_hash::STRING AS server_hash,
    dc.symbol::STRING AS symbol,
    dc.currency::STRING AS currency,
    va.sum_volume_prev_7d::DOUBLE AS sum_volume_prev_7d,
    va.sum_volume_prev_all::DOUBLE AS sum_volume_prev_all,
    DENSE_RANK() OVER (
        PARTITION BY va.login_hash, va.symbol 
        ORDER BY va.sum_volume_prev_7d DESC
        )::INT AS rank_volume_symbol_prev_7d,
    va.rank_count_prev_7d::INT AS rank_count_prev_7d,
    va.sum_volume_2020_08::DOUBLE AS sum_volume_2020_08,
    LAST_VALUE(dc.original_date_first_trade) OVER (
        PARTITION BY dc.login_hash, dc.server_hash, dc.symbol  
        ORDER BY dc.login_hash, dc.server_hash, dc.symbol  
        )::TIMESTAMP AS date_first_trade,
    ROW_NUMBER() OVER (
         PARTITION BY dc.dt_report, dc.login_hash, dc.server_hash, dc.symbol 
         ORDER BY dc.dt_report, dc.login_hash, dc.server_hash, dc.symbol
         )::INT AS row_number
FROM daily_combinations dc
LEFT JOIN volume_aggregates va
ON dc.dt_report = va.dt_report
AND dc.login_hash = va.login_hash 
AND dc.server_hash = va.server_hash
AND dc.symbol = va.symbol
ORDER BY row_number DESC;
