{% macro get_vendor_data(vendor_id_expr) %}
case
  when {{ vendor_id_expr }} = 1 then 'Creative Mobile Technologies'
  when {{ vendor_id_expr }} = 2 then 'VeriFone Inc.'
  when {{ vendor_id_expr }} = 4 then 'Unknown/Other'
  else 'Not Available'
end
{% endmacro %}
