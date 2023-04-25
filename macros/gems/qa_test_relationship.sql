{% macro qa_epl_data_macro(football_clubs=['Man United', 'Liverpool', 'Man City']) %}

{% set status = ['HomeTeam','AwayTeam'] %}

with summary as (
{% for club in football_clubs %}
    {% for st in status %}
    select 
        {{ st }} as team,
        {% if st == 'HomeTeam' %}
                case 
                    when FTR = 'H' then 3
                    when FTR = 'D' then 1
                    else 0 end points
        {% else %}
                case 
                    when FTR = 'A' then 3
                    when FTR = 'D' then 1
                    else 0 end points
        {% endif %}
    from {{ source('staging', 'english-premier-league-table') }}
    where season = 'season-1819'
        and {{ st }} = '{{ club }}'
        {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
    {% if not loop.last %} UNION ALL {% endif %}
{% endfor %}
)


select 
    team, 
    sum(points) as total_points
from summary
group by team
order by total_points desc
{% endmacro %}

 {% macro qa_all_null(model, column_name='id') %}


select * from {{ model }} where {{ column_name }} is not null
{% endmacro %}

 {% macro qa_all_not_null(model='customers', column_name='id') %}


select * from {{ model }} where {{ column_name }} is not null
{% endmacro %}

 {% macro qa_complex_macro(model, column_name_int, accepted_values=[1, 2]) %}


with all_values as (
    select distinct {{column_name_int}} as col_int from {{model}}
),
payments_validation_errors as (
    select
        col_int
    from all_values
    where col_int not in (
        {% for accepted_value in accepted_values %}
            {% if accepted_value >= 5 %}
            5
            {% else %}
            {{ accepted_value }}
            {% endif %}
            {% if not loop.last %},{% endif %}
        {% endfor %}
    )
)
select * from payments_validation_errors
{% endmacro %}

 {% macro qa_model_all_above_given_id(model, col, id_min=2) %}


SELECT * from {{model}} where {{col}} > {{ id_min }}
{% endmacro %}

 {% macro qa_get_unique_count(model, column_name) %}


select count(*)
from (
    select
        {{ column_name }}
    from {{ model }}
    where {{ column_name }} is not null
    group by {{ column_name }}
    having count(*) >= 1
) validation_errors
{% endmacro %}

 {% macro qa_test_relationship(model1, model2, model1_col, model2_col) %}

select count(*)
from (
    select {{ model1_col }} as id from {{ model }}
) as child
left join (
    select {{ model2_col }} as id from {{ model2 }}
) as parent on parent.id = child.id
where child.id is not null
  and parent.id is null
{% endmacro %}

 