{% test dti_ratio(model) %}

SELECT
  applicant_id,
  post_tax_annual_income,
  total_monthly_expenses,
  monthly_emi_amount,
  other_debts,
  ((total_monthly_expenses + monthly_emi_amount) * 12 + other_debts) / NULLIF(post_tax_annual_income, 0) AS dti_ratio
FROM {{ model }}
WHERE ((total_monthly_expenses + monthly_emi_amount) * 12 + other_debts) / NULLIF(post_tax_annual_income, 0) > 0.5

{% endtest %}
