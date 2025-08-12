WITH clean_transform AS (
    SELECT
        TRIM(borrower_id) AS borrower_id,
        TRIM(loan_id) AS loan_id,
        DATE(loan_issued_at) AS loan_issued_at,
        DATE(report_date_local) AS report_date_local,
        -- Clean and cast outstanding balance
        CASE
            WHEN outstanding_balance IS NULL OR outstanding_balance < 0
                THEN 0.0
            ELSE CAST(outstanding_balance AS REAL)
        END AS outstanding_balance,
        -- Clean and cast repaid_amount_day
        CASE
            WHEN repaid_amount_day IS NULL OR repaid_amount_day < 0
                THEN 0.0
            ELSE CAST(repaid_amount_day AS REAL)
        END AS repaid_amount_day,
        -- Always calculate days_past_due from dates, as in pandas
        CASE
            WHEN loan_issued_at IS NOT NULL AND loan_issued_at != ''
              AND report_date_local IS NOT NULL AND report_date_local != ''
            THEN CAST(JULIANDAY(report_date_local) - JULIANDAY(loan_issued_at) AS INT)
            ELSE NULL
        END AS days_past_due
    FROM bandora_debt_collection
    WHERE borrower_id IS NOT NULL
      AND TRIM(borrower_id) != ''
      AND loan_id IS NOT NULL
      AND TRIM(loan_id) != ''
      AND report_date_local IS NOT NULL
      AND report_date_local != ''
      AND loan_issued_at IS NOT NULL
      AND loan_issued_at != ''
      AND report_date_local >= '2025-04-01'
      AND report_date_local <= '2025-06-30'
),

daily_aggregates AS (
    SELECT
        report_date_local,
        SUM(repaid_amount_day) as total_daily_payments,
        SUM(outstanding_balance) as total_outstanding_balance,
        COUNT(DISTINCT borrower_id) as active_borrowers,
        COUNT(*) as total_records
    FROM clean_transform
    GROUP BY report_date_local
),

daily_recovery_rates AS (
    SELECT
        report_date_local,
        total_daily_payments,
        total_outstanding_balance,
        active_borrowers,
        total_records,

        CASE
            WHEN total_outstanding_balance > 0
            THEN ROUND((total_daily_payments / total_outstanding_balance) * 100, 4)
            ELSE 0
        END as daily_recovery_rate_pct

    FROM daily_aggregates
),

moving_average_calculation AS (
    SELECT
        drr.*,

        AVG(drr.daily_recovery_rate_pct) OVER (
            ORDER BY drr.report_date_local
            ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
        ) as recovery_rate_3week_ma_pct,

        AVG(drr.daily_recovery_rate_pct) OVER (
            ORDER BY drr.report_date_local
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as recovery_rate_7day_ma_pct,

        COUNT(*) OVER (
            ORDER BY drr.report_date_local
            ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
        ) as ma_window_days

    FROM daily_recovery_rates drr
)

SELECT
    report_date_local,
    total_daily_payments,
    total_outstanding_balance,
    active_borrowers,
    total_records,

    daily_recovery_rate_pct,
    ROUND(recovery_rate_3week_ma_pct, 4) as recovery_rate_3week_ma_pct,
    ROUND(recovery_rate_7day_ma_pct, 4) as recovery_rate_7day_ma_pct,
    ma_window_days,
    ROUND(daily_recovery_rate_pct - recovery_rate_3week_ma_pct, 4) as variance_from_3week_ma,

    CASE CAST(strftime('%w', report_date_local) AS INTEGER)
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END as day_of_week,
    strftime('%W', report_date_local) as week_number

FROM moving_average_calculation
ORDER BY report_date_local;
