
-- This model creates a view of unique financial information for applicants from the raw.financial_information_raw source.
-- It ranks financial records based on post-tax annual income in descending order.
-- For each unique applicant_id, only the record with the highest post-tax annual income is selected.
-- The output includes applicant_id, post-tax annual income, total monthly expenses, monthly EMI amount, 
-- and other debts.
-- The WHERE clause filters out any entries where applicant_id is NULL,
-- ensuring that only valid financial records are retained in the final result set.

{{ config(materialized='view') }}

-- Selects the financial record with the highest post_tax_annual_income per applicant.
WITH ranked_financials AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY applicant_id
            ORDER BY post_tax_annual_income DESC
        ) AS row_num
    FROM {{ source('raw', 'financial_information_raw') }}
    WHERE applicant_id IS NOT NULL
)

SELECT
    applicant_id,
    post_tax_annual_income,
    total_monthly_expenses,
    monthly_emi_amount,
    other_debts
FROM ranked_financials
WHERE row_num = 1
