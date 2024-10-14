{% test no_zero_income(model) %}

SELECT
  applicant_id,
  post_tax_annual_income
FROM {{ model }}
WHERE post_tax_annual_income IS NULL OR post_tax_annual_income = 0

{% endtest %}
