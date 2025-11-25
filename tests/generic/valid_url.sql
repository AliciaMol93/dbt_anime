{% test valid_url(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} is not null
  and not regexp_like({{ column_name }}, '^https?://')

{% endtest %}
