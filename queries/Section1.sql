-- question 1

SELECT
    borrower_id,loan_id,country,days_past_due,outstanding_balance
FROM Analyst_case_study
WHERE days_past_due > 10 AND outstanding_balance > 2000;

