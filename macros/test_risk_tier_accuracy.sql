{% test risk_tier_accuracy(model) %}

WITH calculated_risk AS (
    SELECT
        applicant_id,
        credit_score,
        dti_ratio,
        lti_ratio,
        risk_tier,
        CASE
            WHEN credit_score >= 800 AND dti_ratio BETWEEN 0 AND 0.36 AND lti_ratio BETWEEN 0 AND 0.28 THEN 'Excellent'
            WHEN credit_score BETWEEN 750 AND 799 AND dti_ratio BETWEEN 0 AND 0.36 AND lti_ratio BETWEEN 0 AND 0.28 THEN 'Very Good'
            WHEN credit_score BETWEEN 670 AND 749 AND dti_ratio BETWEEN 0 AND 0.43 AND lti_ratio BETWEEN 0 AND 0.36 THEN 'Good'
            WHEN credit_score BETWEEN 580 AND 669 AND dti_ratio BETWEEN 0.36 AND 0.50 AND lti_ratio BETWEEN 0.28 AND 0.36 THEN 'Fair'
            ELSE 'Poor'
        END AS calculated_risk_tier
    FROM {{ model }}
)

SELECT
    applicant_id,
    credit_score,
    dti_ratio,
    lti_ratio,
    risk_tier,
    calculated_risk_tier
FROM calculated_risk
WHERE risk_tier != calculated_risk_tier
  AND risk_tier IS NOT NULL
  AND calculated_risk_tier IS NOT NULL

{% endtest %}
