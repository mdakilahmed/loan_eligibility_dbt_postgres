{% test date_not_in_future(model, column_name) %}

SELECT
  {{ column_name }}
FROM {{ model }}
WHERE {{ column_name }} > CURRENT_DATE

{% endtest %}
