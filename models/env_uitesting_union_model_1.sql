{{
  config({    
    "materialized": "table"
  })
}}

{% set model_1_int = 10 %}

WITH my_table1 AS (

  SELECT * 
  
  FROM {{ ref('raw_orders')}}

),

my_table2 AS (

  SELECT * 
  
  FROM {{ source('alias_spark_catalog_qa_db_warehouse', 'all_type_partitioned') }}

),

my_table_3 AS (

  SELECT * 
  
  FROM {{ source('spark_catalog.qa_database', 'delta_all_type_table') }}

),

final_table AS (

  SELECT id AS c_id
  
  FROM my_table1
  
  UNION
  
  SELECT c_tinyint AS c_id
  
  FROM my_table2
  
  UNION
  
  SELECT c_tinyint AS c_id
  
  FROM my_table_3

)

SELECT *

FROM final_table

{% if is_incremental() %}
  
  WHERE c_id > (
    SELECT MAX(c_id)
    
    FROM {{this}}
   )
{% endif %}
