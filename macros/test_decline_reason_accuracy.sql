{% test decline_reason_accuracy(model) %}

SELECT
  applicant_id,
  loan_decision,
  credit_score,
  dti_ratio,
  number_of_late_payments,
  bankruptcies_filed,
  foreclosures,
  decline_reason,
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
  END AS calculated_decline_reason
FROM {{ model }}
WHERE decline_reason != CASE
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
  END
  AND decline_reason IS NOT NULL

{% endtest %}
