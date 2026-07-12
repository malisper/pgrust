--jsonpath io

select ''::jsonpath;
-- pgrust:rowsort
select '$'::jsonpath;
-- pgrust:rowsort
select 'strict $'::jsonpath;
-- pgrust:rowsort
select 'lax $'::jsonpath;
-- pgrust:rowsort
select '$.a'::jsonpath;
-- pgrust:rowsort
select '$.a.v'::jsonpath;
-- pgrust:rowsort
select '$.a.*'::jsonpath;
-- pgrust:rowsort
select '$.*[*]'::jsonpath;
-- pgrust:rowsort
select '$.a[*]'::jsonpath;
-- pgrust:rowsort
select '$.a[*][*]'::jsonpath;
-- pgrust:rowsort
select '$[*]'::jsonpath;
-- pgrust:rowsort
select '$[0]'::jsonpath;
-- pgrust:rowsort
select '$[*][0]'::jsonpath;
-- pgrust:rowsort
select '$[*].a'::jsonpath;
-- pgrust:rowsort
select '$[*][0].a.b'::jsonpath;
-- pgrust:rowsort
select '$.a.**.b'::jsonpath;
-- pgrust:rowsort
select '$.a.**{2}.b'::jsonpath;
-- pgrust:rowsort
select '$.a.**{2 to 2}.b'::jsonpath;
-- pgrust:rowsort
select '$.a.**{2 to 5}.b'::jsonpath;
-- pgrust:rowsort
select '$.a.**{0 to 5}.b'::jsonpath;
-- pgrust:rowsort
select '$.a.**{5 to last}.b'::jsonpath;
-- pgrust:rowsort
select '$.a.**{last}.b'::jsonpath;
-- pgrust:rowsort
select '$.a.**{last to 5}.b'::jsonpath;
-- pgrust:rowsort
select '$+1'::jsonpath;
-- pgrust:rowsort
select '$-1'::jsonpath;
-- pgrust:rowsort
select '$--+1'::jsonpath;
-- pgrust:rowsort
select '$.a/+-1'::jsonpath;
-- pgrust:rowsort
select '1 * 2 + 4 % -3 != false'::jsonpath;

-- pgrust:rowsort
select '"\b\f\r\n\t\v\"\''\\"'::jsonpath;
-- pgrust:rowsort
select '"\x50\u0067\u{53}\u{051}\u{00004C}"'::jsonpath;
-- pgrust:rowsort
select '$.foo\x50\u0067\u{53}\u{051}\u{00004C}\t\"bar'::jsonpath;
-- pgrust:rowsort
select '"\z"'::jsonpath;  -- unrecognized escape is just the literal char

-- pgrust:rowsort
select '$.g ? ($.a == 1)'::jsonpath;
-- pgrust:rowsort
select '$.g ? (@ == 1)'::jsonpath;
-- pgrust:rowsort
select '$.g ? (@.a == 1)'::jsonpath;
-- pgrust:rowsort
select '$.g ? (@.a == 1 || @.a == 4)'::jsonpath;
-- pgrust:rowsort
select '$.g ? (@.a == 1 && @.a == 4)'::jsonpath;
-- pgrust:rowsort
select '$.g ? (@.a == 1 || @.a == 4 && @.b == 7)'::jsonpath;
-- pgrust:rowsort
select '$.g ? (@.a == 1 || !(@.a == 4) && @.b == 7)'::jsonpath;
-- pgrust:rowsort
select '$.g ? (@.a == 1 || !(@.x >= 123 || @.a == 4) && @.b == 7)'::jsonpath;
-- pgrust:rowsort
select '$.g ? (@.x >= @[*]?(@.a > "abc"))'::jsonpath;
-- pgrust:rowsort
select '$.g ? ((@.x >= 123 || @.a == 4) is unknown)'::jsonpath;
-- pgrust:rowsort
select '$.g ? (exists (@.x))'::jsonpath;
-- pgrust:rowsort
select '$.g ? (exists (@.x ? (@ == 14)))'::jsonpath;
-- pgrust:rowsort
select '$.g ? ((@.x >= 123 || @.a == 4) && exists (@.x ? (@ == 14)))'::jsonpath;
-- pgrust:rowsort
select '$.g ? (+@.x >= +-(+@.a + 2))'::jsonpath;

-- pgrust:rowsort
select '$a'::jsonpath;
-- pgrust:rowsort
select '$a.b'::jsonpath;
-- pgrust:rowsort
select '$a[*]'::jsonpath;
-- pgrust:rowsort
select '$.g ? (@.zip == $zip)'::jsonpath;
-- pgrust:rowsort
select '$.a[1,2, 3 to 16]'::jsonpath;
-- pgrust:rowsort
select '$.a[$a + 1, ($b[*]) to -($[0] * 2)]'::jsonpath;
-- pgrust:rowsort
select '$.a[$.a.size() - 3]'::jsonpath;
select 'last'::jsonpath;
-- pgrust:rowsort
select '"last"'::jsonpath;
-- pgrust:rowsort
select '$.last'::jsonpath;
select '$ ? (last > 0)'::jsonpath;
-- pgrust:rowsort
select '$[last]'::jsonpath;
-- pgrust:rowsort
select '$[$[0] ? (last > 0)]'::jsonpath;

-- pgrust:rowsort
select 'null.type()'::jsonpath;
select '1.type()'::jsonpath;
-- pgrust:rowsort
select '(1).type()'::jsonpath;
-- pgrust:rowsort
select '1.2.type()'::jsonpath;
-- pgrust:rowsort
select '"aaa".type()'::jsonpath;
-- pgrust:rowsort
select 'true.type()'::jsonpath;
-- pgrust:rowsort
select '$.double().floor().ceiling().abs()'::jsonpath;
-- pgrust:rowsort
select '$.keyvalue().key'::jsonpath;
-- pgrust:rowsort
select '$.datetime()'::jsonpath;
-- pgrust:rowsort
select '$.datetime("datetime template")'::jsonpath;
-- pgrust:rowsort
select '$.bigint().integer().number().decimal()'::jsonpath;
-- pgrust:rowsort
select '$.boolean()'::jsonpath;
-- pgrust:rowsort
select '$.date()'::jsonpath;
-- pgrust:rowsort
select '$.decimal(4,2)'::jsonpath;
-- pgrust:rowsort
select '$.string()'::jsonpath;
-- pgrust:rowsort
select '$.time()'::jsonpath;
-- pgrust:rowsort
select '$.time(6)'::jsonpath;
-- pgrust:rowsort
select '$.time_tz()'::jsonpath;
-- pgrust:rowsort
select '$.time_tz(4)'::jsonpath;
-- pgrust:rowsort
select '$.timestamp()'::jsonpath;
-- pgrust:rowsort
select '$.timestamp(2)'::jsonpath;
-- pgrust:rowsort
select '$.timestamp_tz()'::jsonpath;
-- pgrust:rowsort
select '$.timestamp_tz(0)'::jsonpath;

-- pgrust:rowsort
select '$ ? (@ starts with "abc")'::jsonpath;
-- pgrust:rowsort
select '$ ? (@ starts with $var)'::jsonpath;

select '$ ? (@ like_regex "(invalid pattern")'::jsonpath;
-- pgrust:rowsort
select '$ ? (@ like_regex "pattern")'::jsonpath;
-- pgrust:rowsort
select '$ ? (@ like_regex "pattern" flag "")'::jsonpath;
-- pgrust:rowsort
select '$ ? (@ like_regex "pattern" flag "i")'::jsonpath;
-- pgrust:rowsort
select '$ ? (@ like_regex "pattern" flag "is")'::jsonpath;
-- pgrust:rowsort
select '$ ? (@ like_regex "pattern" flag "isim")'::jsonpath;
select '$ ? (@ like_regex "pattern" flag "xsms")'::jsonpath;
-- pgrust:rowsort
select '$ ? (@ like_regex "pattern" flag "q")'::jsonpath;
-- pgrust:rowsort
select '$ ? (@ like_regex "pattern" flag "iq")'::jsonpath;
-- pgrust:rowsort
select '$ ? (@ like_regex "pattern" flag "smixq")'::jsonpath;
select '$ ? (@ like_regex "pattern" flag "a")'::jsonpath;

-- pgrust:rowsort
select '$ < 1'::jsonpath;
-- pgrust:rowsort
select '($ < 1) || $.a.b <= $x'::jsonpath;
select '@ + 1'::jsonpath;

-- pgrust:rowsort
select '($).a.b'::jsonpath;
-- pgrust:rowsort
select '($.a.b).c.d'::jsonpath;
-- pgrust:rowsort
select '($.a.b + -$.x.y).c.d'::jsonpath;
-- pgrust:rowsort
select '(-+$.a.b).c.d'::jsonpath;
-- pgrust:rowsort
select '1 + ($.a.b + 2).c.d'::jsonpath;
-- pgrust:rowsort
select '1 + ($.a.b > 2).c.d'::jsonpath;
-- pgrust:rowsort
select '($)'::jsonpath;
-- pgrust:rowsort
select '(($))'::jsonpath;
-- pgrust:rowsort
select '((($ + 1)).a + ((2)).b ? ((((@ > 1)) || (exists(@.c)))))'::jsonpath;

-- pgrust:rowsort
select '$ ? (@.a < 1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < -1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < +1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < .1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < -.1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < +.1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < 0.1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < -0.1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < +0.1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < 10.1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < -10.1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < +10.1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < 1e1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < -1e1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < +1e1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < .1e1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < -.1e1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < +.1e1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < 0.1e1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < -0.1e1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < +0.1e1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < 10.1e1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < -10.1e1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < +10.1e1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < 1e-1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < -1e-1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < +1e-1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < .1e-1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < -.1e-1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < +.1e-1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < 0.1e-1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < -0.1e-1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < +0.1e-1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < 10.1e-1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < -10.1e-1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < +10.1e-1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < 1e+1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < -1e+1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < +1e+1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < .1e+1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < -.1e+1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < +.1e+1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < 0.1e+1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < -0.1e+1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < +0.1e+1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < 10.1e+1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < -10.1e+1)'::jsonpath;
-- pgrust:rowsort
select '$ ? (@.a < +10.1e+1)'::jsonpath;

-- numeric literals

-- pgrust:rowsort
select '0'::jsonpath;
select '00'::jsonpath;
select '0755'::jsonpath;
-- pgrust:rowsort
select '0.0'::jsonpath;
-- pgrust:rowsort
select '0.000'::jsonpath;
-- pgrust:rowsort
select '0.000e1'::jsonpath;
-- pgrust:rowsort
select '0.000e2'::jsonpath;
-- pgrust:rowsort
select '0.000e3'::jsonpath;
-- pgrust:rowsort
select '0.0010'::jsonpath;
-- pgrust:rowsort
select '0.0010e-1'::jsonpath;
-- pgrust:rowsort
select '0.0010e+1'::jsonpath;
-- pgrust:rowsort
select '0.0010e+2'::jsonpath;
-- pgrust:rowsort
select '.001'::jsonpath;
-- pgrust:rowsort
select '.001e1'::jsonpath;
-- pgrust:rowsort
select '1.'::jsonpath;
-- pgrust:rowsort
select '1.e1'::jsonpath;
select '1a'::jsonpath;
select '1e'::jsonpath;
select '1.e'::jsonpath;
select '1.2a'::jsonpath;
select '1.2e'::jsonpath;
-- pgrust:rowsort
select '1.2.e'::jsonpath;
-- pgrust:rowsort
select '(1.2).e'::jsonpath;
-- pgrust:rowsort
select '1e3'::jsonpath;
-- pgrust:rowsort
select '1.e3'::jsonpath;
-- pgrust:rowsort
select '1.e3.e'::jsonpath;
-- pgrust:rowsort
select '1.e3.e4'::jsonpath;
-- pgrust:rowsort
select '1.2e3'::jsonpath;
select '1.2e3a'::jsonpath;
-- pgrust:rowsort
select '1.2.e3'::jsonpath;
-- pgrust:rowsort
select '(1.2).e3'::jsonpath;
-- pgrust:rowsort
select '1..e'::jsonpath;
-- pgrust:rowsort
select '1..e3'::jsonpath;
-- pgrust:rowsort
select '(1.).e'::jsonpath;
-- pgrust:rowsort
select '(1.).e3'::jsonpath;
-- pgrust:rowsort
select '1?(2>3)'::jsonpath;

-- nondecimal
-- pgrust:rowsort
select '0b100101'::jsonpath;
-- pgrust:rowsort
select '0o273'::jsonpath;
-- pgrust:rowsort
select '0x42F'::jsonpath;

-- error cases
select '0b'::jsonpath;
select '1b'::jsonpath;
select '0b0x'::jsonpath;

select '0o'::jsonpath;
select '1o'::jsonpath;
select '0o0x'::jsonpath;

select '0x'::jsonpath;
select '1x'::jsonpath;
select '0x0y'::jsonpath;

-- underscores
-- pgrust:rowsort
select '1_000_000'::jsonpath;
-- pgrust:rowsort
select '1_2_3'::jsonpath;
-- pgrust:rowsort
select '0x1EEE_FFFF'::jsonpath;
-- pgrust:rowsort
select '0o2_73'::jsonpath;
-- pgrust:rowsort
select '0b10_0101'::jsonpath;

-- pgrust:rowsort
select '1_000.000_005'::jsonpath;
-- pgrust:rowsort
select '1_000.'::jsonpath;
-- pgrust:rowsort
select '.000_005'::jsonpath;
-- pgrust:rowsort
select '1_000.5e0_1'::jsonpath;

-- error cases
select '_100'::jsonpath;
select '100_'::jsonpath;
select '100__000'::jsonpath;

select '_1_000.5'::jsonpath;
select '1_000_.5'::jsonpath;
select '1_000._5'::jsonpath;
select '1_000.5_'::jsonpath;
select '1_000.5e_1'::jsonpath;

-- underscore after prefix not allowed in JavaScript (but allowed in SQL)
select '0b_10_0101'::jsonpath;
select '0o_273'::jsonpath;
select '0x_42F'::jsonpath;


-- test non-error-throwing API

SELECT str as jsonpath,
       pg_input_is_valid(str,'jsonpath') as ok,
       errinfo.sql_error_code,
       errinfo.message,
       errinfo.detail,
       errinfo.hint
FROM unnest(ARRAY['$ ? (@ like_regex "pattern" flag "smixq")'::text,
                  '$ ? (@ like_regex "pattern" flag "a")',
                  '@ + 1',
                  '00',
                  '1a']) str,
     LATERAL pg_input_error_info(str, 'jsonpath') as errinfo;
