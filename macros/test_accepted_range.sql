{% test accepted_range(model, column_name, min_value, max_value, data_type='numeric') %}

SELECT
  {{ column_name }}
FROM {{ model }}
WHERE {{ column_name }} < {% if data_type == 'date' %}'{{ min_value }}'{% else %}{{ min_value }}{% endif %} OR
      {{ column_name }} > {% if data_type == 'date' %}'{{ max_value }}'{% else %}{{ max_value }}{% endif %}

{% endtest %}
