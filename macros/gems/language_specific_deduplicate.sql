{% macro language_specific_deduplicate(relation, partition_by, order_by) %}
{{ return(adapter.dispatch('language_specific_deduplicate', 'SQL_DatabricksParentProjectMain')(relation, partition_by, order_by)) }}
{% endmacro %}

 {% macro redshift__language_specific_deduplicate(relation, partition_by, order_by) %}
{{ return(dbt_utils.default__language_specific_deduplicate(relation, partition_by, order_by = order_by)) }}
{% endmacro %}

 {% macro snowflake__language_specific_deduplicate(relation, partition_by, order_by) %}


    select *
    from {{ relation }}
    qualify
        row_number() over (
            partition by {{ partition_by }}
            order by {{ order_by }}
        ) = 1
{% endmacro %}

 {% macro bigquery__language_specific_deduplicate(relation, partition_by, order_by) %}


    select unique.*
    from (
        select
            array_agg (
                original
                order by {{ order_by }}
                limit 1
            )[offset(0)] unique
        from {{ relation }} original
        group by {{ partition_by }}
    )
{% endmacro %}

 {% macro default__language_specific_deduplicate(relation, partition_by, order_by) %}


    with row_numbered as (
        select
            _inner.*,
            row_number() over (
                partition by {{ partition_by }}
                order by {{ order_by }}
            ) as rn
        from {{ relation }} as _inner
    )

    select
        distinct data.*
    from {{ relation }} as data
    
    natural join row_numbered
    where row_numbered.rn = 1
{% endmacro %}

 {% macro postgres__language_specific_deduplicate(relation, partition_by, order_by) %}


    select
        distinct on ({{ partition_by }}) *
    from {{ relation }}
    order by {{ partition_by }}{{ ',' ~ order_by }}
{% endmacro %}

 