
-- This model creates a view of comprehensive applicant details by aggregating information 
-- from various related tables: 'trs_applicant_details', 'trs_employment_analysis', 
-- 'trs_residence_stability', 'trs_financial_metrics', and 'trs_credit_analysis'.
-- The view includes the following fields for each applicant:
-- - applicant_id, first_name, last_name
-- - loan_amount_requested, post-tax annual income
-- - credit_score, DTI ratio (debt-to-income), LTI ratio (loan-to-income), and credit utilization ratio
-- - employment status and stability
-- - residence stability
-- - number of late payments, bankruptcies filed, foreclosures, and number of hard inquiries.
-- 
-- Additionally, it calculates a risk tier for each applicant based on their credit score, 
-- DTI ratio, and LTI ratio using a CASE statement. The risk tiers are classified as follows:
-- - 'Excellent': Credit score >= 800, DTI <= 0.36, LTI <= 0.28
-- - 'Very Good': Credit score between 750 and 799, DTI <= 0.36, LTI <= 0.28
-- - 'Good': Credit score between 670 and 749, DTI <= 0.43, LTI <= 0.36
-- - 'Fair': Credit score between 580 and 669, DTI between 0.36 and 0.50, LTI between 0.28 and 0.36
-- - 'Poor': All other cases
-- 
-- The resulting view provides a comprehensive overview of each applicant's financial health, 
-- employment status, and associated risk tier.

{{ config(materialized='view') }}

-- Aggregates applicant details and assigns 'risk_tier' based on credit metrics.
WITH applicant_details AS (
    SELECT
        a.applicant_id,
        a.first_name,
        a.last_name,
        fm.loan_amount_requested,
        ca.credit_score,
        fm.dti_ratio,
        fm.lti_ratio,
        ca.credit_utilization_ratio,
        ea.employment_status,
        ea.employment_stability,
        rs.residence_stability,
        fm.post_tax_annual_income,
        ca.number_of_late_payments,
        ca.bankruptcies_filed,
        ca.foreclosures,
        ca.number_of_hard_inquiries,
        CASE
            WHEN ca.credit_score >= 800 AND fm.dti_ratio BETWEEN 0 AND 0.36 AND fm.lti_ratio BETWEEN 0 AND 0.28 THEN 'Excellent'
            WHEN ca.credit_score BETWEEN 750 AND 799 AND fm.dti_ratio BETWEEN 0 AND 0.36 AND fm.lti_ratio BETWEEN 0 AND 0.28 THEN 'Very Good'
            WHEN ca.credit_score BETWEEN 670 AND 749 AND fm.dti_ratio BETWEEN 0 AND 0.43 AND fm.lti_ratio BETWEEN 0 AND 0.36 THEN 'Good'
            WHEN ca.credit_score BETWEEN 580 AND 669 AND fm.dti_ratio BETWEEN 0.36 AND 0.50 AND fm.lti_ratio BETWEEN 0.28 AND 0.36 THEN 'Fair'
            ELSE 'Poor'
        END AS risk_tier
    FROM {{ ref('trs_applicant_details') }} a
    LEFT JOIN {{ ref('trs_employment_analysis') }} ea ON a.applicant_id = ea.applicant_id
    LEFT JOIN {{ ref('trs_residence_stability') }} rs ON a.applicant_id = rs.applicant_id
    LEFT JOIN {{ ref('trs_financial_metrics') }} fm ON a.applicant_id = fm.applicant_id
    LEFT JOIN {{ ref('trs_credit_analysis') }} ca ON a.applicant_id = ca.applicant_id
)

SELECT
    *
FROM applicant_details
