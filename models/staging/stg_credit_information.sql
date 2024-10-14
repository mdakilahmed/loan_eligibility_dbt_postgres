
-- This model creates a view of unique credit information for applicants from the raw.credit_information_raw source.
-- It ranks credit records based on the credit score in descending order and, for ties, by credit history length in descending order.
-- For each unique applicant_id, only the highest credit score entry (with the longest credit history) is selected.
-- The output includes credit_id, applicant_id, credit score, credit history length, number of late payments, 
-- bankruptcies filed, foreclosures, credit card debt, total credit limit, and number of hard inquiries.
-- The WHERE clause filters out any entries where applicant_id is NULL,
-- ensuring that only valid credit records are retained in the final result set.

{{ config(materialized='view') }}

-- Selects highest credit score per applicant with longest credit history.
WITH ranked_credit AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY applicant_id
            ORDER BY credit_score DESC, credit_history_length DESC
        ) AS row_num
    FROM {{ source('raw', 'credit_information_raw') }}
    WHERE applicant_id IS NOT NULL
)

SELECT
    credit_id,
    applicant_id,
    credit_score,
    credit_history_length,
    number_of_late_payments,
    bankruptcies_filed,
    foreclosures,
    credit_card_debt,
    total_credit_limit,
    number_of_hard_inquiries
FROM ranked_credit
WHERE row_num = 1
