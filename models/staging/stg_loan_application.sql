
-- This model creates a view of unique loan application information for applicants from the raw.loan_application_raw source.
-- It ranks loan application records based on the application date in descending order.
-- For each unique applicant_id, only the most recent loan application record (based on application_date) is selected.
-- The output includes loan_application_id, applicant_id, application date, loan amount requested, 
-- loan purpose, loan type, loan term, and interest rate type.
-- The WHERE clause filters out any entries where applicant_id is NULL,
-- ensuring that only valid loan application records are retained in the final result set.

{{ config(materialized='view') }}

-- Selects the most recent loan application per applicant.
WITH ranked_loans AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY applicant_id
            ORDER BY application_date DESC
        ) AS row_num
    FROM {{ source('raw', 'loan_application_raw') }}
    WHERE applicant_id IS NOT NULL
)

SELECT
    loan_application_id,
    applicant_id,
    application_date,
    loan_amount_requested,
    loan_purpose,
    loan_type,
    loan_term,
    interest_rate_type
FROM ranked_loans
WHERE row_num = 1
