{% test expenses_vs_income(model) %}

SELECT
  applicant_id,
  post_tax_annual_income,
  total_monthly_expenses,
  monthly_emi_amount,
  other_debts,
  (total_monthly_expenses * 12 + monthly_emi_amount * 12 + other_debts) AS total_annual_expenses
FROM {{ model }}
WHERE post_tax_annual_income < (total_monthly_expenses * 12 + monthly_emi_amount * 12 + other_debts)

{% endtest %}
