{%- macro language_specific_concat() -%}
  {{ return(adapter.dispatch('language_specific_concat', 'SQL_DatabricksParentProjectMain')()) }}
{%- endmacro -%}

{% macro default__language_specific_concat() %}
concat(TRIM('?-?ABC-?-', '?-'),REPLACE('abcd', 'bc'),RIGHT('ABCDEFG', 3),cast(HASH(SEQ8()) as string),
    ASCII('A'),REPEAT('xy', 5),REVERSE('Hello, world!'),SUBSTR('testing 1 2 3', 9, 5),
    INSERT('abc', 1, 2, 'Z'),RTRIM('$125.00', '0.'),UUID_STRING(),sha1('Snowflake'),cast(md5_binary('Snowflake') as string),
    LPAD(' hello ', 10, ' '), DECOMPRESS_STRING(TO_BINARY('0920536E6F77666C616B65', 'HEX'), 'SNAPPY'),
    LPAD('.  hi. ', 10, '$'),
    DAYNAME(TO_DATE('2015-05-01')),
    cast(LAST_DAY(TO_DATE('2015-05-08T23:39:20.123-07:00')) as string),
    cast(DATEADD(year, 2, TO_DATE('2013-05-08')) as string),
    cast(DATEDIFF(month, '2021-01-01'::DATE, '2021-02-28'::DATE) as string),
    cast(DATEDIFF(hour, '2013-05-08T23:39:20.123-07:00'::TIMESTAMP, DATEADD(year, 2, ('2013-05-08T23:39:20.123-07:00')::TIMESTAMP)) as string),
    cast(TIMEDIFF(YEAR, '2017-01-01', '2019-01-01') as string),
    cast(TIME_SLICE('2019-02-28'::DATE, 4, 'MONTH', 'START') as string),
    cast(TRY_TO_TIME('12:30:00') as string)
)
{% endmacro %}

{% macro snowflake__language_specific_concat() %}
concat(TRIM('?-?ABC-?-', '?-'),REPLACE('abcd', 'bc'),RIGHT('ABCDEFG', 3),cast(HASH(SEQ8()) as string),
    ASCII('A'),REPEAT('xy', 5),REVERSE('Hello, world!'),SUBSTR('testing 1 2 3', 9, 5),
    INSERT('abc', 1, 2, 'Z'),RTRIM('$125.00', '0.'),UUID_STRING(),sha1('Snowflake'),cast(md5_binary('Snowflake') as string),
    LPAD(' hello ', 10, ' '), DECOMPRESS_STRING(TO_BINARY('0920536E6F77666C616B65', 'HEX'), 'SNAPPY'),
    LPAD('.  hi. ', 10, '$'),
    DAYNAME(TO_DATE('2015-05-01')),
    cast(LAST_DAY(TO_DATE('2015-05-08T23:39:20.123-07:00')) as string),
    cast(DATEADD(year, 2, TO_DATE('2013-05-08')) as string),
    cast(DATEDIFF(month, '2021-01-01'::DATE, '2021-02-28'::DATE) as string),
    cast(DATEDIFF(hour, '2013-05-08T23:39:20.123-07:00'::TIMESTAMP, DATEADD(year, 2, ('2013-05-08T23:39:20.123-07:00')::TIMESTAMP)) as string),
    cast(TIMEDIFF(YEAR, '2017-01-01', '2019-01-01') as string),
    cast(TIME_SLICE('2019-02-28'::DATE, 4, 'MONTH', 'START') as string),
    cast(TRY_TO_TIME('12:30:00') as string)
)
{% endmacro %}

{% macro bigquery__language_specific_concat() %}
concat(
    FORMAT_DATE('%b %Y', DATE '2008-12-25'), cast(LAST_DAY(DATE '2008-11-25') as string), cast(PARSE_DATE('%A %b %e %Y', 'Thursday Dec 25 2008') as string), cast(PARSE_DATE('%Y%m%d', '20081225') as string), cast(UNIX_DATE(DATE '2008-12-25') as string), cast(CURRENT_DATETIME() as string), cast(DATETIME(TIMESTAMP "2008-12-25 05:30:00+00", "America/Los_Angeles") as string), cast(DATETIME_ADD(DATETIME "2008-12-25 15:30:00", INTERVAL 10 MINUTE) as string), cast(DATETIME_SUB(DATETIME "2008-12-25 15:30:00", INTERVAL 10 MINUTE) as string), cast(DATETIME_DIFF(DATETIME "2010-07-07 10:20:00",DATETIME "2008-12-25 15:30:00", DAY) as string), cast(DATETIME_TRUNC(DATETIME "2008-12-25 15:30:00", DAY) as string), cast(FORMAT_DATETIME("%b-%d-%Y", DATETIME "2008-12-25 15:30:00") as string), cast(PARSE_DATETIME("%c", "Thu Dec 25 07:30:00 2008") as string), cast(PARSE_DATETIME("%a %b %e %I:%M:%S %Y", "Thu Dec 25 07:30:00 2008") as string), cast(PARSE_DATETIME('%m/%d/%Y %I:%M:%S %p', '8/30/2018 2:23:38 pm') as string), cast(PARSE_DATETIME('%A, %B %e, %Y','Wednesday, December 19, 2018') as string), cast(CURRENT_TIME() as string), cast(TIME(TIMESTAMP "2008-12-25 15:30:00+08", "America/Los_Angeles") as string), cast(TIME(15, 30, 00) as string), cast(TIME_ADD(TIME "15:30:00", INTERVAL 10 MINUTE) as string), cast(TIME_SUB(TIME "15:30:00", INTERVAL 10 MINUTE) as string), cast(FORMAT_TIME("%R", TIME "15:30:00") as string), cast(PARSE_TIME("%I:%M:%S", "07:30:00") as string), cast(PARSE_TIME('%I:%M:%S %p', '2:23:38 pm') as string), cast(CURRENT_TIMESTAMP() as string), STRING(TIMESTAMP "2008-12-25 15:30:00+00", "UTC"), STRING(TIMESTAMP("2008-12-25 15:30:00", "America/Los_Angeles")), cast(TIMESTAMP_ADD(TIMESTAMP "2008-12-25 15:30:00+00", INTERVAL 10 MINUTE) as string), cast(TIMESTAMP_SUB(TIMESTAMP "2008-12-25 15:30:00+00", INTERVAL 10 MINUTE) as string), cast(TIMESTAMP_TRUNC(TIMESTAMP "2008-12-25 15:30:00+00", DAY, "UTC") as string), cast(FORMAT_TIMESTAMP("%b-%d-%Y", TIMESTAMP "2008-12-25 15:30:00+00") as string), cast(PARSE_TIMESTAMP("%a %b %e %I:%M:%S %Y", "Thu Dec 25 07:30:00 2008") as string),
    cast(PARSE_TIMESTAMP("%c", "Thu Dec 25 07:30:00 2008") as string),
    cast(TIMESTAMP_SECONDS(1230219000) as string),
    cast(TIMESTAMP_MILLIS(1230219000000) as string),
    cast(TIMESTAMP_MICROS(1230219000000000) as string),
    cast(UNIX_SECONDS(TIMESTAMP "2008-12-25 15:30:00+00") as string),
    cast(UNIX_MILLIS(TIMESTAMP "2008-12-25 15:30:00+00") as string),
    cast(UNIX_MICROS(TIMESTAMP "2008-12-25 15:30:00+00") as string),
    SESSION_USER(),
    GENERATE_UUID(),
    cast(NET.IP_FROM_STRING('48.49.50.51') as string),
    cast(NET.SAFE_IP_FROM_STRING('48.49.50.51') as string)
)
{% endmacro %}


{% macro databricks__language_specific_concat() %}
concat(
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
    date_part('YEAR', TIMESTAMP '2019-08-12 01:00:00.123456'),
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
    cast(from_csv('1, 0.8', 'a INT, b DOUBLE') as string),
    cast(from_json('{"teacher": "Alice", "student": [{"name": "Bob", "rank": 1}, {"name": "Charlie", "rank": 2}]}', 'STRUCT<teacher: STRING, student: ARRAY<STRUCT<name: STRING, rank: INT>>>') as string),
    cast(from_unixtime(0, 'yyyy-MM-dd HH:mm:ss') as string),
    cast(from_utc_timestamp('2016-08-31', 'Asia/Seoul') as string),
    cast(get_json_object('{"a":"b"}', '$.a') as string),
    hash('Spark', array(123), 2),
    hex(17),
    cast(hour('2009-07-30 12:58:59') as string),
    cast(hypot(3, 4) as string),
    cast(ilike('Spark', '_Park') as string))
{% endmacro %}