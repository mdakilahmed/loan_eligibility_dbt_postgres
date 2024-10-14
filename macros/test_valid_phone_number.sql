{% test valid_phone_number(model, column_name) %}

SELECT
  {{ column_name }}
FROM {{ model }}
WHERE {{ column_name }} IS NOT NULL
  AND {{ column_name }} !~ '^\+?[0-9\s\-()]{7,15}$'

{% endtest %}
