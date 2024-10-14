{% test dti_ratio_accuracy(model) %}

SELECT
  applicant_id,
  dti_ratio,
  calculated_dti_ratio
FROM (
  SELECT
    applicant_id,
    dti_ratio,
    ROUND((
      (COALESCE(monthly_emi_amount, 0) + COALESCE(credit_card_debt, 0) + COALESCE(other_debts, 0))
      / NULLIF(post_tax_annual_income / 12, 0)
    ), 2) AS calculated_dti_ratio
  FROM {{ model }}
) sub
WHERE ABS(dti_ratio - calculated_dti_ratio) > 0.01
  AND dti_ratio IS NOT NULL
  AND calculated_dti_ratio IS NOT NULL

{% endtest %}
