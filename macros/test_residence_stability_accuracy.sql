{% test residence_stability_accuracy(model) %}

SELECT
  applicant_id,
  years_at_current_residence,
  residence_stability,
  CASE
      WHEN years_at_current_residence >= 1 THEN TRUE
      ELSE FALSE
  END AS calculated_stability
FROM {{ model }}
WHERE residence_stability != CASE
    WHEN years_at_current_residence >= 1 THEN TRUE
    ELSE FALSE
  END
  AND years_at_current_residence IS NOT NULL
  AND residence_stability IS NOT NULL

{% endtest %}
