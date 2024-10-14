{% test credit_utilization_accuracy(model) %}

SELECT
  applicant_id,
  credit_card_debt,
  total_credit_limit,
  credit_utilization_ratio,
  (credit_card_debt / NULLIF(total_credit_limit, 0)) AS calculated_ratio
FROM {{ model }}
WHERE credit_utilization_ratio != (credit_card_debt / NULLIF(total_credit_limit, 0))
  AND (credit_card_debt IS NOT NULL AND total_credit_limit IS NOT NULL)

{% endtest %}
