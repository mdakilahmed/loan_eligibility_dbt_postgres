{% test employment_stability_accuracy(model) %}

SELECT
  applicant_id,
  years_in_current_job,
  employment_stability,
  CASE
    WHEN years_in_current_job >= 1 THEN TRUE
    ELSE FALSE
  END AS calculated_stability
FROM {{ model }}
WHERE employment_stability != CASE
    WHEN years_in_current_job >= 1 THEN TRUE
    ELSE FALSE
  END
  AND years_in_current_job IS NOT NULL
  AND employment_stability IS NOT NULL

{% endtest %}
