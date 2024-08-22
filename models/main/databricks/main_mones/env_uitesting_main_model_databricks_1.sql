{% set v_expression = 'concat(c_struct.city, c_string)' %}
{% set v_int = 22 %}


WITH all_type_non_partitioned AS (

  SELECT * 
  
  FROM {{ source('alias_spark_catalog_qa_db_warehouse', 'all_type_non_partitioned') }}

),

all_type_non_partitioned_1 AS (

  SELECT * 
  
  FROM all_type_non_partitioned

),

env_uitesting_main_model_databricks_2 AS (

  SELECT * 
  
  FROM {{ ref('env_uitesting_main_model_databricks_2')}}

),

Limit_2 AS (

  SELECT * 
  
  FROM env_uitesting_main_model_databricks_2 AS in0
  
  LIMIT 10

),

all_type_partitioned AS (

  SELECT * 
  
  FROM {{ source('alias_spark_catalog_qa_db_warehouse', 'all_type_partitioned') }}

),

all_type_partitioned_1 AS (

  SELECT * 
  
  FROM all_type_partitioned

),

Subgraph_2 AS (

  WITH Join_1_1 AS (
  
    {#Combines various data types from different tables to create a comprehensive view for analysis.#}
    SELECT 
      in2.p_int AS p_int,
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
    INNER JOIN Limit_2 AS in2
       ON all_type_partitioned.c_tinyint = in2.c_tinyint
  
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
  
  )
  
  SELECT * 
  
  FROM Reformat_1

),

Subgraph_1 AS (

  WITH my_table2_1 AS (
  
    SELECT * 
    
    FROM all_type_partitioned
  
  ),
  
  raw_orders_seed AS (
  
    SELECT * 
    
    FROM {{ ref('raw_orders')}}
  
  ),
  
  AllStunningOne AS (
  
    {#Evaluates a wide range of logical and mathematical expressions to derive various values and insights.#}
    SELECT 
      (1 != 2)
      or (true != NULL)
      or (NULL != NULL)
      or (1 < 2)
      or (2 <= 2)
      or (2 <=> 2)
      or ((2 % 1.8) == 1)
      or (to_date('2009-07-30 04:17:52') < to_date('2009-07-30 04:17:52'))
      or (add_months('2016-08-31', 1) < add_months('2017-08-31', 3))
      or (true and false)
      or array_contains(array_distinct(array(1, 2, 3)), 2)
      or array_contains(array_except(array(1, 2, 3), array(1, 3, 5)), 2)
      or array_contains(array_intersect(array(1, 2, 3), array(1, 3, 5)), 10)
      or (array_join(array('hello', NULL, 'world'), ' ', ',') LIKE '%hello%')
      or (array_max(array(1, 20, NULL, 3)) > 10)
      or (array_min(array(1, 20, NULL, 3)) > 20)
      or array_contains(array_remove(array(1, 2, 3, NULL, 3), 3), 3)
      or array_contains(array_repeat(5, 2), 6)
      or array_contains(array_union(array(1, 2, 3), array(1, 3, 5)), 10)
      or arrays_overlap(array(1, 2, 3), array(3, 4, 5))
      or (10 BETWEEN 2 AND 20)
      or contains('Spark SQL', 'Spark')
      or endswith('Spark SQL', 'SQL')
      or (
           EXISTS(
             array(1, 2, 3), 
             x -> x % 2 == 0)
         )
      or array_contains(filter(
           array(1, 2, 3), 
           x -> x % 2 == 1), 5)
      or array_contains(flatten(array(array(1, 2), array(3, 4))), 10)
      or forall(
           array(1, 2, 3), 
           x -> x % 2 == 0)
      or ilike('Spark', '_Park')
      or (1 IN (2, 3, 4))
      or (isnan(CAST('NaN' AS double)))
      or isnotnull(1)
      or isnull(1)
      or array_contains(json_object_keys('{"key": "value"}'), 'key1')
      or like('Spark', '_park')
      or map_contains_key(map(1, 'a', 2, 'b'), 1)
      or map_contains_key(map_concat(map(1, 'a', 2, 'b'), map(3, 'c')), 4)
      or map_contains_key(map_filter(
           map(1, 0, 2, 2, 3, -1), 
           (k, v) -> k > v), 3)
      or map_contains_key(map_from_arrays(array(1.0, 3.0), array('2', '4')), 2)
      or map_contains_key(map_from_entries(array(struct(1, 'a'), struct(2, 'b'))), 1)
      or array_contains(map_keys(map(1, 'a', 2, 'b')), 2)
      or array_contains(map_values(map(1, 'a', 2, 'b')), 'a')
      or map_contains_key(map_zip_with(
           map(1, 'a', 2, 'b'), 
           map(1, 'x', 2, 'y'), 
           (k, v1, v2) -> concat(v1, v2)), 1)
      or (named_struct('a', 1, 'b', 2) IN (named_struct('a', 1, 'b', 1), named_struct('a', 1, 'b', 3)))
      or (NOT true)
      or array_contains(regexp_extract_all('100-200, 300-400', '(\\d+)-(\\d+)', 1), '100')
      or array_contains(sequence(1, 5), 4)
      or array_contains(shuffle(array(1, 20, 3, 5)), 10)
      or array_contains(slice(array(1, 2, 3, 4), 2, 2), 4)
      or array_contains(sort_array(array('b', 'd', NULL, 'c', 'a'), true), 'b')
      or array_contains(split('oneAtwoBthreeC', '[ABC]'), 'one')
      or startswith('Spark SQL', 'Spark')
      or map_contains_key(str_to_map('a:1,b:2,c:3', ',', ':'), 'a')
      or array_contains(transform(
           array(1, 2, 3), 
           x -> x + 1), 1)
      or map_contains_key(transform_keys(
           map_from_arrays(array(1, 2, 3), array(1, 2, 3)), 
           (k, v) -> k + 1), 1)
      or map_contains_key(transform_values(
           map_from_arrays(array(1, 2, 3), array(1, 2, 3)), 
           (k, v) -> v + 1), 2)
      or array_contains(xpath('<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>', 'a/b/text()'), 'a')
      or xpath_boolean('<a><b>1</b></a>', 'a/b')
      or array_contains(zip_with(
           array(1, 2), 
           array(3, 4), 
           (x, y) -> x + y), 1) AS c_bool,
      concat(
        c_array[0], 
        c_struct.city, 
        aes_decrypt(unhex('83F16B2AA704794132802D248E6BFD4E380078182D1544813898AC97E709B28A94'), '0000111122223333'), 
        base64(aes_encrypt('Spark SQL', '1234567890abcdef', 'ECB', 'PKCS')), 
        bin(13), 
        btrim('    SparkSQL   '), 
        char(65), 
        chr(65), 
        concat('Spark', 'SQL'), 
        concat_ws(' ', 'Spark', 'SQL'), 
        crc32('Spark'), 
        current_catalog(), 
        current_database(), 
        current_date(), 
        current_timestamp(), 
        current_timezone(), 
        current_user(), 
        date_add('2016-07-30', 1), 
        date_sub('2016-07-30', 1), 
        date_format('2016-04-08', 'y'), 
        date_from_unix_date(1), 
        date_part('YEAR', TIMESTAMP'2019-08-12 01:00:00.123456'), 
        date_part('MONTH', INTERVAL '2021-11' YEAR TO MONTH), 
        date_part('MINUTE', INTERVAL '123 23:55:59.002001' DAY TO SECOND), 
        date_trunc('HOUR', '2015-03-05T09:32:05.359'), 
        date_trunc('DD', '2015-03-05T09:32:05.359'), 
        datediff('2009-07-31', '2009-07-30'), 
        decode(encode('abc', 'utf-8'), 'utf-8'), 
        e(), 
        elt(1, 'scala', 'java'), 
        format_number(12332.123456, '##################.###'), 
        format_string('Hello World %d %s', 100, 'days'), 
        CAST(from_csv('1, 0.8', 'a INT, b DOUBLE') AS string), 
        CAST(from_json(
          '{"teacher": "Alice", "student": [{"name": "Bob", "rank": 1}, {"name": "Charlie", "rank": 2}]}', 
          'STRUCT<teacher: STRING, student: ARRAY<STRUCT<name: STRING, rank: INT>>>') AS string), 
        CAST(from_unixtime(0, 'yyyy-MM-dd HH:mm:ss') AS string), 
        CAST(from_utc_timestamp('2016-08-31', 'Asia/Seoul') AS string), 
        CAST(get_json_object('{"a":"b"}', '$.a') AS string), 
        hash('Spark', array(123), 2), 
        hex(17), 
        CAST(hour('2009-07-30 12:58:59') AS string), 
        CAST(hypot(3, 4) AS string), 
        CAST(ilike('Spark', '_Park') AS string), 
        CAST(initcap('sPark sql') AS string), 
        CAST(last_day('2009-01-12') AS string), 
        CAST(lcase('SparkSql') AS string), 
        CAST(if(
          1 < 2, 
          'a', 
          'b') AS string), 
        CAST(ifnull(NULL, array('2')) AS string), 
        LEFT('Spark SQL', 3), 
        lower('SparkSql'), 
        lpad('hi', 5, '??'), 
        ltrim('    SparkSQL   '), 
        CAST(make_date(2013, 7, 15) AS string), 
        CAST(make_dt_interval(1, 12, 30, 1.001001) AS string), 
        CAST(make_interval(100, 11, 1, 1, 12, 30, 1.001001) AS string), 
        CAST(make_timestamp(2019, 6, 30, 23, 59, 60) AS string), 
        CAST(make_ym_interval(1, 2) AS string), 
        md5('Spark'), 
        next_day('2015-01-14', 'TU'), 
        now(), 
        nullif(2, 2), 
        nvl(NULL, 'hello'), 
        CAST(overlay('Spark SQL' PLACING '_' FROM 6) AS string), 
        CAST(parse_url('http://spark.apache.org/path?query=1', 'HOST') AS string), 
        printf('Hello World %d %s', 100, 'days'), 
        CAST(regexp_extract('100-200', '(\\d+)-(\\d+)', 1) AS string), 
        CAST(regexp_replace('100-200', '(\\d+)', 'num') AS string), 
        repeat('123', 2), 
        replace('ABCabc', 'abc', 'DEF'), 
        reverse('Spark SQL'), 
        RIGHT('Spark SQL', 3), 
        rpad('hi', 5, '??'), 
        rtrim('    SparkSQL   '), 
        CAST(schema_of_json('[{"col":0}]') AS string), 
        sha('Spark'), 
        sha1('Spark'), 
        sha2('Spark', 256), 
        concat(space(2), '1'), 
        split_part('11.12.13', '.', 3), 
        substr('Spark SQL', 5), 
        substring('Spark SQL', 5), 
        substring_index('www.apache.org', '.', 2), 
        timestamp_micros(1230219000123123), 
        timestamp_millis(1230219000123), 
        timestamp_seconds(1.230219000123E9), 
        to_csv(named_struct('a', 1, 'b', 2)), 
        to_date('2009-07-30 04:17:52'), 
        to_timestamp('2016-12-31 00:12:00'), 
        to_unix_timestamp('2016-04-08', 'yyyy-MM-dd'), 
        to_utc_timestamp('2016-08-31', 'Asia/Seoul'), 
        translate('AaBbCc', 'abc', '123'), 
        trunc('2019-08-04', 'week'), 
        try_to_binary('abc', 'utf-8'), 
        try_to_number('454', '999'), 
        typeof(1), 
        ucase('SparkSql'), 
        unbase64('U3BhcmsgU1FM'), 
        decode(unhex('537061726B2053514C'), 'UTF-8'), 
        unix_date(DATE("1970-01-02")), 
        unix_micros(TIMESTAMP('1970-01-01 00:00:01Z')), 
        unix_millis(TIMESTAMP('1970-01-01 00:00:01Z')), 
        unix_seconds(TIMESTAMP('1970-01-01 00:00:01Z')), 
        unix_timestamp('2016-04-08', 'yyyy-MM-dd'), 
        upper('SparkSql'), 
        uuid(), 
        xpath_string('<a><b>b</b><c>cc</c></a>', 'a/c'), 
        xxhash64('Spark', array(123), 2), 
        YEAR ('2016-07-30'), 
        to_json(array(named_struct('a', 1, 'b', 2)))) AS c_string,
      (2 % 1.8)
      + '20'::INTEGER
      + (MOD(2, 1.8))
      + (3 & 5)
      + (2 * 3)
      + (5 + 10)
      - (100 + 45)
      + (3 / 2)
      + (3 ^ 5)
      + abs(-1)
      + acos(1)
      + acosh(1)
      + array_position(array(3, 2, 1), 1)
      + array_size(array('b', 'd', 'c', 'a'))
      + ascii(2)
      + asin(0)
      + asinh(0)
      + atan(0)
      + atan2(0, 0)
      + atanh(0)
      + bit_count(0)
      + bit_get(11, 0)
      + bit_length('Spark SQL')
      + bround(25, -1)
      + cardinality(array('b', 'd', 'c', 'a'))
      + cardinality(map('a', 1, 'b', 2))
      + CAST('10' AS int)
      + cbrt(27.0)
      + ceil(3.1411, 3)
      + ceiling(3.1411, 3)
      + char_length('Spark SQL ')
      + coalesce(NULL, 1, NULL)
      + conv('100', 2, 10)
      + cos(0)
      + cosh(0)
      + cot(1)
      + csc(1)
      + day('2009-07-30')
      + dayofmonth('2009-07-30')
      + dayofweek('2009-07-30')
      + dayofyear('2016-04-09')
      + degrees(3.141592653589793)
      + element_at(array(1, 2, 3), 2)
      + exp(0)
      + expm1(0)
      + EXTRACT(SECONDS FROM TIMESTAMP'2019-10-01 00:00:01.000001')
      + EXTRACT(MINUTE FROM INTERVAL '123 23:55:59.002001' DAY TO SECOND)
      + factorial(2)
      + find_in_set('ab', 'abc,b,ab,c,def')
      + floor(-0.1)
      + getbit(11, 0)
      + greatest(10, 9, 2, 4, 3)
      + instr('SparkSQL', 'SQL')
      + json_array_length('[1,2,3,{"f1":1,"f2":[5,6]},4]')
      + least(10, 9, 2, 4, 3)
      + length('Spark SQL ')
      + levenshtein('kitten', 'sitting')
      + ln(10)
      + locate('bar', 'foobarbar')
      + log(10, 100)
      + log10(10)
      + log1p(0)
      + log2(2)
      + minute('2009-07-30 12:58:59')
      + (2 % 1.8)
      + month('2016-07-30')
      + months_between('1997-02-28 10:30:00', '1996-10-30', false)
      + nanvl(CAST('NaN' AS double), 123)
      + negative(100)
      + nvl2(NULL, 2, 1)
      + octet_length('Spark SQL')
      + pi()
      + pmod(10, 3)
      + position('bar', 'foobarbar')
      + positive(1)
      + pow(2, 3)
      + power(2, 3)
      + quarter('2016-08-31')
      + radians(180)
      + rand()
      + randn()
      + random()
      + rint(12.3456)
      + round(2.5, 0)
      + sec(0)
      + second('2009-07-30 12:58:59')
      + shiftleft(2, 1)
      + shiftright(4, 1)
      + shiftrightunsigned(4, 1)
      + sign(40)
      + signum(40)
      + sin(0)
      + sinh(0)
      + size(array('b', 'd', 'c', 'a'))
      + sqrt(4)
      + tan(0)
      + tanh(0)
      + to_number('454.00', '000.00')
      + try_add(1, 2)
      + try_divide(2L, 2L)
      + try_element_at(array(1, 2, 3), 2)
      + try_multiply(2, 3)
      + try_subtract(2, 1)
      + weekday('2009-07-30')
      + weekofyear('2008-02-20')
      + (
          CASE
            WHEN 1 > 0
              THEN 1
            WHEN 2 > 0
              THEN 2.0
            ELSE 1.2
          END
        )
      + width_bucket(5.3, 0.2, 10.6, 5)
      + xpath_double('<a><b>1</b><b>2</b></a>', 'sum(a/b)')
      + xpath_int('<a><b>1</b><b>2</b></a>', 'sum(a/b)')
      + xpath_long('<a><b>1</b><b>2</b></a>', 'sum(a/b)')
      + xpath_number('<a><b>1</b><b>2</b></a>', 'sum(a/b)')
      + xpath_short('<a><b>1</b><b>2</b></a>', 'sum(a/b)')
      + (~ 0) AS c_bigint,
      {{ SQL_DatabricksParentProjectMain.qa_concat_macro_column('c_string') }} AS c_macro,
      {% if v_int > 20 %}
        concat(c_string, c_float) AS c_if,
      {% elif var('v_project_dict_parent') ['b'] == 'hello' %}
        concat(c_string, c_int) AS c_if,
      {% else %}
        concat(c_string, c_bigint) AS c_if,
      {% endif %}
      {% for c_i in range(0, 5) %}
        concat(c_string, {{c_i}}) AS `cfor_col_{{c_i}}`,
      {% endfor %}
      
      concat('{{ dbt_utils.pretty_time() }}', '{{ dbt_utils.pretty_log_format("my pretty message") }}') AS c_dbt_utils,
      area(c_int, c_int) AS c_use_databricks_function,
      to_json(named_struct('a', 1, 'b', 2)) AS c_to_json,
      {{v_expression}} AS c_use_config_expression
    
    FROM all_type_partitioned AS in0
  
  ),
  
  my_table2 AS (
  
    SELECT * 
    
    FROM all_type_partitioned
  
  ),
  
  final_table AS (
  
    {#Consolidates unique identifiers from multiple order sources for comprehensive analysis.#}
    SELECT id AS c_id
    
    FROM raw_orders_seed
    
    UNION
    
    SELECT c_tinyint AS c_id
    
    FROM my_table2
    
    UNION
    
    SELECT c_tinyint AS c_id
    
    FROM my_table2_1
    
    WHERE my_table2_1.c_bigint > (
            SELECT count(*)
            
            FROM AllStunningOne
           )
  
  ),
  
  Limit_1_1 AS (
  
    SELECT * 
    
    FROM final_table AS in0
    
    LIMIT 10
  
  )
  
  SELECT * 
  
  FROM Limit_1_1

),

Filter_1 AS (

  SELECT * 
  
  FROM all_type_partitioned AS in0
  
  WHERE true

),

OrderBy_1 AS (

  {#Sorts filtered data by various criteria to prioritize specific records.#}
  SELECT * 
  
  FROM Filter_1 AS in0
  
  ORDER BY c_tinyint ASC NULLS FIRST, c_smallint DESC NULLS LAST, c_double ASC

),

SetOperation_1 AS (

  SELECT * 
  
  FROM OrderBy_1 AS in0
  
  UNION
  
  SELECT * 
  
  FROM OrderBy_1 AS in1

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
  RIGHT JOIN SetOperation_1 AS all_type_partitioned
     ON all_type_non_partitioned.c_tinyint = all_type_partitioned.c_tinyint
    and all_type_non_partitioned.c_smallint = all_type_partitioned.c_smallint
  LEFT JOIN Subgraph_1 AS in2
     ON all_type_partitioned.c_int != in2.c_id

),

Reformat_3 AS (

  SELECT * 
  
  FROM Join_1 AS in0

),

Reformat_4 AS (

  SELECT * 
  
  FROM Reformat_3 AS in0

),

qa_all_not_null_1 AS (

  {#Ensures data integrity by checking for non-null values in a specific column of a dataset.#}
  {{ SQL_DatabricksParentProjectMain.qa_all_not_null(model = 'Reformat_4', column_name = 'p_string') }}

),

SQLStatement_2 AS (

  {#Identifies records where a specific integer does not match the count of entries in another dataset.#}
  SELECT *
  
  FROM qa_all_not_null_1
  
  WHERE p_int != (
          SELECT count(*)
          
          FROM Subgraph_2
         )

)

SELECT *

FROM SQLStatement_2
