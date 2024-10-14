{% test loan_decision_accuracy(model) %}

SELECT
  applicant_id,
  risk_tier,
  loan_decision,
  CASE
      WHEN risk_tier IN ('Excellent', 'Very Good') THEN 'Approved'
      WHEN risk_tier IN ('Good', 'Fair') THEN 'Review: Manual Approval Required'
      ELSE 'Declined'
  END AS calculated_loan_decision
FROM {{ model }}
WHERE loan_decision != CASE
      WHEN risk_tier IN ('Excellent', 'Very Good') THEN 'Approved'
      WHEN risk_tier IN ('Good', 'Fair') THEN 'Review: Manual Approval Required'
      ELSE 'Declined'
  END
  AND loan_decision IS NOT NULL

{% endtest %}
