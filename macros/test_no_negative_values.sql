{% test no_negative_values(model, column_name) %}

SELECT
  applicant_id,
  {{ column_name }}
FROM {{ model }}
WHERE {{ column_name }} < 0

{% endtest %}
