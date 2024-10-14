{% test age_consistency(model) %}

SELECT
  applicant_id,
  date_of_birth,
  age,
  DATE_PART('year', AGE(CURRENT_DATE, date_of_birth)) AS calculated_age
FROM {{ model }}
WHERE age != DATE_PART('year', AGE(CURRENT_DATE, date_of_birth))

{% endtest %}
