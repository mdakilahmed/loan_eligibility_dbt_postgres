{% test debt_vs_limit(model) %}

SELECT
  credit_id,
  applicant_id,
  credit_card_debt,
  total_credit_limit
FROM {{ model }}
WHERE credit_card_debt > total_credit_limit

{% endtest %}
