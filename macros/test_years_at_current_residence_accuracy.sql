{% test years_at_current_residence_accuracy(model) %}

SELECT
  applicant_id,
  move_in_date,
  years_at_current_residence,
  DATE_PART('year', AGE(CURRENT_DATE, move_in_date)) AS calculated_years
FROM {{ model }}
WHERE years_at_current_residence != DATE_PART('year', AGE(CURRENT_DATE, move_in_date))
  AND move_in_date IS NOT NULL
  AND years_at_current_residence IS NOT NULL

{% endtest %}
