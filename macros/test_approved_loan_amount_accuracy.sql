{% test approved_loan_amount_accuracy(model) %}

SELECT
  applicant_id,
  risk_tier,
  loan_amount_requested,
  approved_loan_amount,
  CASE
      WHEN risk_tier = 'Excellent' THEN loan_amount_requested
      WHEN risk_tier = 'Very Good' THEN loan_amount_requested * 0.9
      WHEN risk_tier = 'Good' THEN loan_amount_requested * 0.8
      WHEN risk_tier = 'Fair' THEN loan_amount_requested * 0.7
      ELSE 0
  END AS calculated_approved_loan_amount
FROM {{ model }}
WHERE approved_loan_amount != CASE
      WHEN risk_tier = 'Excellent' THEN loan_amount_requested
      WHEN risk_tier = 'Very Good' THEN loan_amount_requested * 0.9
      WHEN risk_tier = 'Good' THEN loan_amount_requested * 0.8
      WHEN risk_tier = 'Fair' THEN loan_amount_requested * 0.7
      ELSE 0
  END
  AND approved_loan_amount IS NOT NULL

{% endtest %}
