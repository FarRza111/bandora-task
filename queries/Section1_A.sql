WITH clean_transform AS (
    SELECT
        TRIM(borrower_id) AS borrower_id,
        TRIM(loan_id) AS loan_id,
        DATE(loan_issued_at) AS loan_issued_at,
        DATE(report_date_local) AS report_date_local,
        -- Clean and cast outstanding_balance
        CASE
            WHEN outstanding_balance IS NULL OR outstanding_balance < 0
                THEN 0.0
            ELSE CAST(outstanding_balance AS REAL)
        END AS outstanding_balance,
        -- Clean and cast repaid_amount_day
        CASE
            WHEN repaid_amount_day IS NULL OR repaid_amount_day < 0
                THEN 0
            ELSE CAST(repaid_amount_day AS FLOAT)
        END AS repaid_amount_day,
        -- Always calculate days_past_due from dates
        CASE
            WHEN loan_issued_at IS NOT NULL AND report_date_local IS NOT NULL
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

customer_analysis AS (
    SELECT
        borrower_id,
        loan_id,
        outstanding_balance,
        days_past_due,
        report_date_local,
        repaid_amount_day,
        ROW_NUMBER() OVER (
            PARTITION BY borrower_id
            ORDER BY report_date_local DESC
        ) as rn_latest
    FROM clean_transform
),

current_status AS (
    SELECT *
    FROM customer_analysis
    WHERE rn_latest = 1
      AND days_past_due > 10
      AND outstanding_balance > 2000
),

payment_analysis AS (
    SELECT
        ld.borrower_id,
        ld.report_date_local,
        ld.repaid_amount_day,
        ROW_NUMBER() OVER (
            PARTITION BY ld.borrower_id
            ORDER BY ld.report_date_local DESC
        ) as payment_rank
    FROM clean_transform ld
    INNER JOIN current_status cs ON ld.borrower_id = cs.borrower_id
    WHERE ld.repaid_amount_day > 0
),

last_two_payments AS (
    SELECT
        borrower_id,
        MAX(CASE WHEN payment_rank = 1 THEN repaid_amount_day END) as last_payment,
        MAX(CASE WHEN payment_rank = 2 THEN repaid_amount_day END) as previous_payment
    FROM payment_analysis
    WHERE payment_rank <= 2
    GROUP BY borrower_id
),

balance_trend AS (
    SELECT
        ld.borrower_id,
        MAX(ld.outstanding_balance) as current_balance,
        MIN(ld.outstanding_balance) as balance_start_period,
        MAX(CASE WHEN ld.report_date_local >= '2025-06-01' THEN ld.outstanding_balance END) as june_balance,
        MAX(CASE WHEN ld.report_date_local <= '2025-04-30' THEN ld.outstanding_balance END) as april_balance
    FROM clean_transform ld
    INNER JOIN current_status cs ON ld.borrower_id = cs.borrower_id
    GROUP BY ld.borrower_id
)

SELECT
    cs.borrower_id,
    cs.outstanding_balance as total_outstanding_amount,
    cs.days_past_due,
    cs.report_date_local as latest_report_date,

    CASE
        WHEN ltp.last_payment IS NOT NULL
             AND ltp.previous_payment IS NOT NULL
             AND ltp.last_payment < (ltp.previous_payment * 0.95)
        THEN 1
        ELSE 0
    END as payment_decreased_flag,

    CASE
        WHEN bt.june_balance IS NOT NULL
             AND bt.april_balance IS NOT NULL
             AND bt.june_balance > bt.april_balance
        THEN 1
        ELSE 0
    END as balance_increased_flag,

    ltp.last_payment,
    ltp.previous_payment,
    ROUND(
        CASE
            WHEN ltp.previous_payment > 0
            THEN ((ltp.previous_payment - ltp.last_payment) / ltp.previous_payment) * 100
            ELSE 0
        END, 2
    ) as payment_decrease_percentage,

    bt.current_balance,
    bt.balance_start_period,
    bt.june_balance,
    bt.april_balance

FROM current_status cs
LEFT JOIN last_two_payments ltp ON cs.borrower_id = ltp.borrower_id
LEFT JOIN balance_trend bt ON cs.borrower_id = bt.borrower_id
ORDER BY cs.outstanding_balance DESC;
