
-- This model creates a view of credit information for applicants by selecting relevant fields
-- from the staging model 'stg_credit_information'.
-- It includes applicant_id, credit score, credit history length, number of late payments, bankruptcies filed, 
-- foreclosures, credit card debt, number of hard inquiries, and total credit limit.
-- Additionally, it calculates the credit utilization ratio by dividing the credit card debt by the total credit limit.
-- The NULLIF function is used to prevent division by zero, returning NULL when the total credit limit is zero.

{{ config(materialized='view') }}

-- Calculates 'credit_utilization_ratio' for each applicant.
SELECT
    ci.applicant_id,
    ci.credit_score,
    ci.credit_history_length,
    ci.number_of_late_payments,
    ci.bankruptcies_filed,
    ci.foreclosures,
    ci.credit_card_debt,
    ci.number_of_hard_inquiries,
    ci.total_credit_limit,
    ci.credit_card_debt / NULLIF(ci.total_credit_limit, 0) AS credit_utilization_ratio
FROM {{ ref('stg_credit_information') }} ci
