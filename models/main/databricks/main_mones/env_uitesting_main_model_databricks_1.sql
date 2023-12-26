WITH my_table1 AS (

  SELECT * 
  
  FROM {{ ref('raw_orders')}}

),

all_type_partitioned AS (

  SELECT * 
  
  FROM {{ source('alias_spark_catalog_qa_db_warehouse', 'all_type_partitioned') }}

),

my_table2 AS (

  SELECT * 
  
  FROM all_type_partitioned

),

my_table2_1 AS (

  SELECT * 
  
  FROM all_type_partitioned

),

final_table AS (

  SELECT id AS c_id
  
  FROM my_table1
  
  UNION
  
  SELECT c_tinyint AS c_id
  
  FROM my_table2
  
  UNION
  
  SELECT c_tinyint AS c_id
  
  FROM my_table2_1

),

Limit_1 AS (

  SELECT * 
  
  FROM final_table AS in0
  
  LIMIT 10

),

all_type_non_partitioned AS (

  SELECT * 
  
  FROM {{ source('alias_spark_catalog_qa_db_warehouse', 'all_type_non_partitioned') }}

),

Join_1 AS (

  {#asd
  asdasddasd#}
  SELECT 
    all_type_partitioned.p_int AS p_int,
    all_type_partitioned.p_string AS p_string,
    all_type_non_partitioned.c_string AS c_string,
    all_type_non_partitioned.c_int AS c_int,
    all_type_non_partitioned.c_bigint + spark_catalog.qa_db_warehouse.area(10, 20) AS c_bigint,
    all_type_non_partitioned.c_smallint AS c_smallint,
    all_type_non_partitioned.c_tinyint AS c_tinyint,
    all_type_non_partitioned.c_float AS c_float,
    all_type_non_partitioned.c_boolean AS c_boolean,
    all_type_non_partitioned.c_array AS c_array,
    all_type_non_partitioned.c_double AS c_double,
    all_type_non_partitioned.c_struct AS c_struct,
    in2.c_id AS c_id
  
  FROM all_type_non_partitioned
  RIGHT JOIN all_type_partitioned
     ON all_type_non_partitioned.c_tinyint = all_type_partitioned.c_tinyint
    and all_type_non_partitioned.c_smallint = all_type_partitioned.c_smallint
  LEFT JOIN Limit_1 AS in2
     ON all_type_partitioned.c_int != in2.c_id

),

all_type_non_partitioned_1 AS (

  SELECT * 
  
  FROM {{ source('alias_spark_catalog_qa_db_warehouse', 'all_type_non_partitioned') }}

),

all_type_partitioned_1 AS (

  SELECT * 
  
  FROM {{ source('alias_spark_catalog_qa_db_warehouse', 'all_type_partitioned') }}

),

Join_1_1 AS (

  SELECT 
    all_type_partitioned.p_int AS p_int,
    all_type_partitioned.p_string AS p_string,
    all_type_non_partitioned.c_string AS c_string,
    all_type_non_partitioned.c_int AS c_int,
    all_type_non_partitioned.c_bigint + spark_catalog.qa_db_warehouse.area(10, 20) AS c_bigint,
    all_type_non_partitioned.c_smallint AS c_smallint,
    all_type_non_partitioned.c_tinyint AS c_tinyint,
    all_type_non_partitioned.c_float AS c_float,
    all_type_non_partitioned.c_boolean AS c_boolean,
    all_type_non_partitioned.c_array AS c_array,
    all_type_non_partitioned.c_double AS c_double,
    all_type_non_partitioned.c_struct AS c_struct,
    {{ SQL_BaseGitDepProjectAllFinal.qa_concat_macro_base_column('all_type_non_partitioned.c_string') }} AS c_base_dependency_macro,
    {{ SQL_DatabricksParentProjectMain.qa_boolean_macro('all_type_non_partitioned.c_string') }} AS c_current_project_macro,
    concat('{{ dbt_utils.pretty_time() }}', '{{ dbt_utils.pretty_log_format("my pretty message") }}') AS c_dbt_date
  
  FROM all_type_non_partitioned_1 AS all_type_non_partitioned
  INNER JOIN all_type_partitioned_1 AS all_type_partitioned
     ON all_type_non_partitioned.c_tinyint = all_type_partitioned.c_tinyint
    and all_type_non_partitioned.c_smallint = all_type_partitioned.c_smallint

),

Reformat_2_1 AS (

  SELECT 
    p_int AS p_int,
    p_string AS p_string,
    c_string AS c_string,
    c_int AS c_int,
    c_bigint AS c_bigint,
    c_smallint AS c_smallint,
    c_tinyint AS c_tinyint,
    c_float AS c_float,
    c_boolean AS c_boolean,
    c_array AS c_array,
    c_double AS c_double,
    c_struct AS c_struct,
    c_base_dependency_macro AS c_base_dependency_macro,
    c_current_project_macro AS c_current_project_macro,
    c_dbt_date AS c_dbt_date
  
  FROM Join_1_1 AS in0

),

Reformat_1 AS (

  SELECT 
    p_int AS p_int,
    p_string AS p_string,
    c_string AS c_string,
    c_int AS c_int,
    c_bigint AS c_bigint,
    c_smallint AS c_smallint,
    c_tinyint AS c_tinyint,
    c_float AS c_float,
    c_boolean AS c_boolean,
    c_array AS c_array,
    c_double AS c_double,
    c_struct AS c_struct,
    c_base_dependency_macro AS c_base_dependency_macro,
    c_current_project_macro AS c_current_project_macro,
    c_dbt_date AS c_dbt_date,
    c_struct.city AS city
  
  FROM Reformat_2_1 AS in0

),

Reformat_3 AS (

  SELECT * 
  
  FROM Join_1 AS in0

),

Reformat_4 AS (

  SELECT * 
  
  FROM Reformat_3 AS in0

),

Reformat_2 AS (

  SELECT * 
  
  FROM Reformat_4 AS in0

),

SQLStatement_2 AS (

  SELECT *
  
  FROM Reformat_4
  
  WHERE p_int != (
          SELECT count(*)
          
          FROM Reformat_1
         )

)

SELECT *

FROM SQLStatement_2
