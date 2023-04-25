{% macro qa_macro_call_another_macro_column(param_column) %}
concat({{ SQL_DatabricksParentProjectMain.qa_concat_macro_column(param_column) }}, {{param_column}})
{% endmacro %}

 {% macro qa_concat_macro(input_string_col) %}
concat(aes_decrypt(unhex('83F16B2AA704794132802D248E6BFD4E380078182D1544813898AC97E709B28A94'), '0000111122223333'), base64(aes_encrypt({{input_string_col}}, '1234567890abcdef', 'ECB', 'PKCS')), bin(13), btrim('    SparkSQL   '), char(65), chr(65), concat({{input_string_col}}, 'SQL'), concat_ws(' ', {{input_string_col}}, 'SQL'), crc32('Spark'), current_catalog(), current_database(), current_date(), current_timestamp(), current_timezone(), current_user(), date_add('2016-07-30', 1), date_sub('2016-07-30', 1), date_format('2016-04-08', 'y'), date_from_unix_date(1), date_part('YEAR', TIMESTAMP'2019-08-12 01:00:00.123456'), date_part('MONTH', INTERVAL '2021-11' YEAR TO MONTH), date_part('MINUTE', INTERVAL '123 23:55:59.002001' DAY TO SECOND), date_trunc('HOUR', '2015-03-05T09:32:05.359'), date_trunc('DD', '2015-03-05T09:32:05.359'), datediff('2009-07-31', '2009-07-30'), decode(encode('abc', 'utf-8'), 'utf-8'), e(), elt(1, 'scala', 'java'), format_number(12332.123456, '##################.###'), format_string('Hello World %d %s', 100, 'days'), CAST(from_csv('1, 0.8', 'a INT, b DOUBLE') AS string), CAST(from_json('{"teacher": "Alice", "student": [{"name": "Bob", "rank": 1}, {"name": "Charlie", "rank": 2}]}', 'STRUCT<teacher: STRING, student: ARRAY<STRUCT<name: STRING, rank: INT>>>') AS string), CAST(from_unixtime(0, 'yyyy-MM-dd HH:mm:ss') AS string), CAST(from_utc_timestamp('2016-08-31', 'Asia/Seoul') AS string), CAST(get_json_object('{"a":"b"}', '$.a') AS string), hash('Spark', array(123), 2), hex(17), CAST(hour('2009-07-30 12:58:59') AS string), CAST(hypot(3, 4) AS string), CAST(ilike('Spark', '_Park') AS string), CAST(initcap('sPark sql') AS string), CAST(last_day('2009-01-12') AS string), CAST(lcase('SparkSql') AS string), CAST(if(1 < 2, 'a', 'b') AS string), CAST(ifnull(NULL, array('2')) AS string))
{% endmacro %}

 {% macro round_function(n1, scale=2) %}
ROUND({{n1}}, {{scale}})
{% endmacro %}

 {% macro qa_boolean_macro(input_string_column) %}

(1 != 2)
or (true != NULL)
or (NULL != NULL)
or (1 < 2)
or (2 <= 2)
or (2 <=> 2)
or ((2 % 1.8) == 1)
or (
  to_date ('2009-07-30 04:17:52') < to_date ('2009-07-30 04:17:52')
)
or (
  add_months ('2016-08-31', 1) < add_months ('2017-08-31', 3)
)
or (
  true
  and false
)
or array_contains (array_distinct (array (1, 2, 3)), 2)
or array_contains (
  array_except (array (1, 2, 3), array (1, 3, 5)),
  2
)
or array_contains (
  array_intersect (array (1, 2, 3), array (1, 3, 5)),
  10
)
or (
  array_join (array ({{input_string_column}}, null, 'world'), ' ', ',') like '%hello%'
)
or (array_max (array (1, 20, null, 3)) > 10)
or (array_min (array (1, 20, null, 3)) > 20)
or array_contains (array_remove (array (1, 2, 3, null, 3), 3), 3)
or array_contains (array_repeat (5, 2), 6)
or array_contains (
  array_union (array (1, 2, 3), array (1, 3, 5)),
  10
)
or arrays_overlap (array (1, 2, 3), array (3, 4, 5))
or (10 between 2 and 20)
or contains ({{input_string_column}}, 'Spark')
or endswith ({{input_string_column}}, 'SQL')
or (EXISTS (array (1, 2, 3), x -> x % 2 == 0))
or array_contains (filter(array (1, 2, 3), x -> x % 2 == 1), 5)
or array_contains (flatten(array (array (1, 2), array (3, 4))), 10)
or forall (array(1, 2, 3), x -> x % 2 == 0)
or (1 in (2, 3, 4))
or (isnan (cast('NaN' as double)))
{% endmacro %}

 {% macro qa_concat_macro_calling_complex_concat(param_column) %}
concat({{ SQL_DatabricksParentProjectMain.qa_concat_macro(param_column) }}, {{param_column}})
{% endmacro %}

 {% macro qa_number_macro(input_number_col=10) %}
(2 % 1.8) + (MOD(2, 1.8)) + ({{input_number_col}} & 5) + ({{input_number_col}} * 3) + (5 + {{input_number_col}}) - (100 + 45) + (3 / 2) + (3 ^ 5) + abs(-1) + acos(1) + acosh(1) + array_position(array(3, 2, 1), 1) + array_size(array('b', 'd', 'c', 'a')) + ascii(2) + asin(0) + asinh(0) + atan(0) + atan2(0, 0) + atanh(0) + bit_count(0) + bit_get(11, 0) + bit_length('Spark SQL') + bround(25, -1) + cardinality(array('b', 'd', 'c', 'a')) + cardinality(map('a', 1, 'b', 2)) + CAST('10' AS int) + cbrt(27.0) + ceil(3.1411, 3) + ceiling(3.1411, 3) + char_length('Spark SQL ') + coalesce(NULL, 1, NULL) + conv('100', 2, 10) + cos(0) + cosh(0) + cot(1) + csc(1) + day('2009-07-30') + dayofmonth('2009-07-30') + dayofweek('2009-07-30') + dayofyear('2016-04-09') + degrees(3.141592653589793) + element_at(array(1, 2, 3), 2) + exp(0) + expm1(0) + EXTRACT(SECONDS FROM TIMESTAMP'2019-10-01 00:00:01.000001') + EXTRACT(MINUTE FROM INTERVAL '123 23:55:59.002001' DAY TO SECOND) + factorial(2) + find_in_set('ab', 'abc,b,ab,c,def') + floor(-0.1) + getbit(11, 0) + greatest(10, 9, 2, 4, 3) + instr('SparkSQL', 'SQL') + json_array_length('[1,2,3,{"f1":1,"f2":[5,6]},4]') + least(10, 9, 2, 4, 3) + length('Spark SQL ') + levenshtein('kitten', 'sitting') + ln(10) + locate('bar', 'foobarbar') + log(10, 100) + log10(10) + log1p(0) + log2(2) + minute('2009-07-30 12:58:59')
{% endmacro %}

 {% macro multi_macro_expressions(param_float=12, param_array=[1, 2, 3], param_dict=[1, 2, 3]) %}
concat({{param_float}} + {{param_array[0]}}, 'hello')
{% endmacro %}

 {% macro qa_concat_macro_column(param1_column) %}
concat({{param1_column}}, 'hellomain')
{% endmacro %}

 