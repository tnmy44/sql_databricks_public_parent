WITH all_type_partitioned AS (

  SELECT * 
  
  FROM {{ source('alias_spark_catalog_qa_db_warehouse', 'all_type_partitioned') }}

),

raw_orders AS (

  SELECT * 
  
  FROM {{ ref('raw_orders')}}

),

Join_1 AS (

  {#Combines order details with various data types for comprehensive order analysis.#}
  SELECT 
    in1.c_tinyint AS c_tinyint,
    in1.c_smallint AS c_smallint,
    in1.c_int AS c_int,
    in1.c_bigint AS c_bigint,
    in1.c_float AS c_float,
    in1.c_double AS c_double,
    in1.c_string AS c_string,
    in1.c_boolean AS c_boolean,
    in1.c_array AS c_array,
    in1.c_struct AS c_struct,
    in1.p_int AS p_int,
    in1.p_string AS p_string,
    in0.id AS id,
    in0.user_id AS user_id,
    in0.order_date AS order_date,
    in0.status AS status
  
  FROM raw_orders AS in0
  INNER JOIN all_type_partitioned AS in1
     ON in0.id != in1.c_tinyint

)

SELECT *

FROM Join_1
