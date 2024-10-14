

-- This model creates a view that determines loan decisions for applicants based on risk assessment criteria.
-- It aggregates data from the 'trs_risk_assessment' table, which includes information about applicants' 
-- loan requests and their credit profiles.
-- The view includes the following fields:
-- - applicant_id, first name, last name, loan amount requested, credit score, DTI ratio, LTI ratio,
--   number of late payments, bankruptcies filed, foreclosures, and risk tier.
--
-- The model applies the following logic for loan decisions:
-- - If the applicant's risk tier is 'Excellent' or 'Very Good', the loan is 'Approved'.
-- - If the risk tier is 'Good' or 'Fair', the decision is 'Review: Manual Approval Required'.
-- - All other risk tiers result in 'Declined'.
--
-- The model also calculates:
-- - Offered Interest Rate: Based on the risk tier, with specific rates assigned for each tier.
-- - Adjusted Loan Amount: The approved loan amount is adjusted based on the risk tier, 
--   reducing the requested amount by certain percentages for lower tiers.
--
-- Additionally, the view includes specific reasons for declines when the loan decision is 'Declined':
-- - It checks various conditions (e.g., low credit score, high DTI ratio, multiple late payments, 
--   recent bankruptcies, and recent foreclosures) to provide a detailed decline reason.
-- If the loan is not declined, the decline reason is NULL.

{{ config(materialized='view') }}

-- Determines loan decisions and interest rates based on 'risk_tier'.
WITH decisions AS (
    SELECT
        ra.applicant_id,
        ra.first_name,
        ra.last_name,
        ra.loan_amount_requested,
        ra.credit_score,
        ra.dti_ratio,
        ra.lti_ratio,
        ra.number_of_late_payments,
        ra.bankruptcies_filed,
        ra.foreclosures,
        ra.risk_tier,
        CASE
            WHEN ra.risk_tier IN ('Excellent', 'Very Good') THEN 'Approved'
            WHEN ra.risk_tier IN ('Good', 'Fair') THEN 'Review: Manual Approval Required'
            ELSE 'Declined'
        END AS loan_decision,
        CASE
            WHEN ra.risk_tier = 'Excellent' THEN 0.09
            WHEN ra.risk_tier = 'Very Good' THEN 0.105
            WHEN ra.risk_tier = 'Good' THEN 0.112
            WHEN ra.risk_tier = 'Fair' THEN 0.118
            ELSE NULL
        END AS offered_interest_rate,
        CASE
            WHEN ra.risk_tier = 'Excellent' THEN ra.loan_amount_requested
            WHEN ra.risk_tier = 'Very Good' THEN ra.loan_amount_requested * 0.9
            WHEN ra.risk_tier = 'Good' THEN ra.loan_amount_requested * 0.8
            WHEN ra.risk_tier = 'Fair' THEN ra.loan_amount_requested * 0.7
            ELSE 0
        END AS approved_loan_amount
    FROM {{ ref('trs_risk_assessment') }} ra
)

SELECT
    d.*,
    CASE
        WHEN loan_decision = 'Declined' THEN
            CASE
                WHEN credit_score < 600 THEN 'Not Eligible: Low Credit Score'
                WHEN dti_ratio > 0.5 THEN 'Not Eligible: High Debt-to-Income Ratio'
                WHEN number_of_late_payments > 2 THEN 'Not Eligible: Multiple Late Payments'
                WHEN bankruptcies_filed THEN 'Not Eligible: Recent Bankruptcy'
                WHEN foreclosures THEN 'Not Eligible: Recent Foreclosure'
                ELSE 'Not Eligible: Other Risk Factors'
            END
        ELSE NULL
    END AS decline_reason
FROM decisions d
