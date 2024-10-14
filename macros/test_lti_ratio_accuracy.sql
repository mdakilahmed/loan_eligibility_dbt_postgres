{% test lti_ratio_accuracy(model) %}

SELECT
  applicant_id,
  lti_ratio,
  calculated_lti_ratio
FROM (
  SELECT
    applicant_id,
    lti_ratio,
    ROUND(
      (COALESCE(loan_amount_requested, 0) / NULLIF(post_tax_annual_income, 0)),
      2
    ) AS calculated_lti_ratio
  FROM {{ model }}
) sub
WHERE ABS(lti_ratio - calculated_lti_ratio) > 0.01
  AND lti_ratio IS NOT NULL
  AND calculated_lti_ratio IS NOT NULL

{% endtest %}
