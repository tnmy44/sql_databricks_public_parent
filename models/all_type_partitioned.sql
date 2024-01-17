WITH all_type_partitioned_1 AS (

  SELECT * 
  
  FROM {{ source('alias_spark_catalog_qa_db_warehouse', 'all_type_partitioned') }}

),

all_type_partitioned_1_p_int AS (

  SELECT p_int AS p_int
  
  FROM all_type_partitioned_1 AS in0

)

SELECT *

FROM all_type_partitioned_1_p_int
