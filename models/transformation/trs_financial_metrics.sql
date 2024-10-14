
-- This model creates a view that aggregates financial, loan application, and credit information for applicants.
-- It uses Common Table Expressions (CTEs) to reference data from three staging models:
-- 1. 'stg_financial_information' - includes post-tax annual income, total monthly expenses, monthly EMI amount, 
--    other debts, and applicant ID.
-- 2. 'stg_loan_application' - includes loan amount requested and applicant ID.
-- 3. 'stg_credit_information' - includes credit card debt and applicant ID.
-- The final selection includes the applicant ID, financial details, credit card debt, and the loan amount requested.
-- It also calculates:
-- - Debt-to-Income (DTI) ratio: Total monthly payments (EMI + credit card debt + other debts) divided by monthly gross income.
-- - Loan-to-Income (LTI) ratio: Loan amount requested divided by annual income.
-- The NULLIF function is used to prevent division by zero in both DTI and LTI calculations, returning NULL when the denominator is zero.

{{ config(materialized='view') }}

-- Calculates 'dti_ratio' and 'lti_ratio' for applicants.
WITH financials AS (
    SELECT * FROM {{ ref('stg_financial_information') }}
),
loan_applications AS (
    SELECT
        applicant_id,
        loan_amount_requested
    FROM {{ ref('stg_loan_application') }}
),
credit_information AS (
    SELECT
        applicant_id,
        credit_card_debt
    FROM {{ ref('stg_credit_information') }}
)

SELECT
    f.applicant_id,
    f.post_tax_annual_income,
    f.total_monthly_expenses,
    f.monthly_emi_amount,
    c.credit_card_debt,
    f.other_debts,
    la.loan_amount_requested,
    ROUND(
        (COALESCE(f.monthly_emi_amount, 0) + COALESCE(c.credit_card_debt, 0) + COALESCE(f.other_debts, 0)) / NULLIF(f.post_tax_annual_income / 12, 0),
        2
    ) AS dti_ratio,
    ROUND(
        la.loan_amount_requested / NULLIF(f.post_tax_annual_income, 0),
        2
    ) AS lti_ratio
FROM financials f
LEFT JOIN loan_applications la ON f.applicant_id = la.applicant_id
LEFT JOIN credit_information c ON f.applicant_id = c.applicant_id
