-- json (text) family differential corpus: datum_to_json rendering,
-- builders, aggregates, lex-based operators/SRFs, strip_nulls/typeof.
-- Two-binary via scripts/regress-diff.sh (pgrust vs C 18.3). ARRAY[]
-- literals and VALUES-alias column lists avoided; arrays use '{...}'.
\set VERBOSITY verbose
set timezone = 'UTC';

-- json text preservation: order, duplicates, whitespace
select '{"b":1, "a":2, "b":3}'::json;
select '  [1,  2,3 ]  '::json;
select '{"a":
1}'::json;
select '"é"'::json;
select 'nope'::json;
select '{"a":1'::json;
select '[1,2'::json;
select '"abc'::json;

-- json_typeof
select json_typeof('{}');
select json_typeof('[1]');
select json_typeof('"x"');
select json_typeof('-1.5');
select json_typeof('true');
select json_typeof('false');
select json_typeof('null');
select json_typeof('  {"a":1}  ');

-- to_json across categories
select to_json(1::int2);
select to_json(2::int4);
select to_json(3::int8);
select to_json(4.5::float4);
select to_json(6.75::float8);
select to_json('NaN'::float8);
select to_json('Infinity'::float8);
select to_json('-Infinity'::float4);
select to_json(1.50::numeric);
select to_json('NaN'::numeric);
select to_json(true);
select to_json(false);
select to_json('plain'::text);
select to_json('with "quotes" and \ backslash'::text);
select to_json(E'tab\tnewline\nctrl\x01'::text);
select to_json('vc'::varchar);
select to_json('bp'::bpchar);
select to_json('name'::name);
select to_json('2024-02-29'::date);
select to_json('infinity'::date);
select to_json('2024-02-29 12:34:56.789'::timestamp);
select to_json('-infinity'::timestamp);
select to_json('2024-02-29 12:34:56+05:30'::timestamptz);
select to_json('{"k": [1, 2] , "k":2}'::json);
select to_json('{"b":2,"a":1,"a":3}'::jsonb);
select to_json('{1,2,3}'::int4[]);
select to_json('{}'::int4[]);
select to_json('{{1,2},{3,4}}'::int4[]);
select to_json('{a,"b c",NULL}'::text[]);
select to_json('{1.5,NaN}'::numeric[]);
select to_json('a'::char);
select to_json(interval '1 day 02:03:04');
select to_json('08:09:10'::time);

-- row_to_json / composites
select row_to_json(row(1, 'foo', null));
select row_to_json(row(1.5, true, '2024-01-02'::date));
select row_to_json(row(row(1,2), '{3,4}'::int4[]));
select row_to_json(row(1,'a'), true);
select row_to_json(row(1,'a'), false);
select to_json(row(7, 'x'));

-- array_to_json
select array_to_json('{1,2,3}'::int4[]);
select array_to_json('{{1,2},{3,4}}'::int4[], true);
select array_to_json('{{1,2},{3,4}}'::int4[], false);
select array_to_json('{}'::text[]);

-- json_build_object / json_build_array
select json_build_object();
select json_build_object('a', 1, 'b', 'txt', 'c', null, 'd', true);
select json_build_object('a', '{"x":1}'::json, 'b', '{"y":2}'::jsonb);
select json_build_object(1, 2);
select json_build_object('a'::text, '{1,2}'::int4[]);
select json_build_object('k', 1.50::numeric);
select json_build_object('a');
select json_build_object(null, 1);
select json_build_object('{1,2}'::int4[], 1);
select json_build_array();
select json_build_array(1, 'a', null, true, 2.5);
select json_build_array('{"x":1}'::json, '{1,2}'::int4[]);
select json_build_array(null::int4);
select json_build_object(variadic '{a,1,b,2}'::text[]);
select json_build_array(variadic '{1,2,3}'::int4[]);
select json_build_array(variadic '{a,NULL}'::text[]);
select json_build_object(variadic '{a,1,b}'::text[]);
select json_build_object(variadic null::text[]);

-- json_object
select json_object('{}'::text[]);
select json_object('{a,1,b,"two words"}'::text[]);
select json_object('{{a,1},{b,2}}'::text[]);
select json_object('{a,NULL}'::text[]);
select json_object('{NULL,1}'::text[]);
select json_object('{a,1,b}'::text[]);
select json_object('{{a,1,c},{b,2,d}}'::text[]);
select json_object('{a,b}'::text[], '{1,2}'::text[]);
select json_object('{a,b}'::text[], '{1}'::text[]);
select json_object('{}'::text[], '{}'::text[]);
select json_object('{a}'::text[], '{NULL}'::text[]);
select json_object('{NULL}'::text[], '{a}'::text[]);

-- operators -> ->> #> #>> (text json: whitespace/order/dup preserved)
select '{"a": {"x": 1},  "b": [1, 2],  "a": "second"}'::json -> 'a';
select '{"a": {"x": 1},  "b": [1, 2]}'::json -> 'b';
select '{"a": {"x": 1}}'::json -> 'z';
select '{"a": null}'::json -> 'a';
select '{"a": null}'::json ->> 'a';
select '{"a": "séq"}'::json ->> 'a';
select '{"a": {"x": 1}}'::json ->> 'a';
select '[10, 20, null, {"a": 1}]'::json -> 0;
select '[10, 20, null, {"a": 1}]'::json -> 3;
select '[10, 20, null, {"a": 1}]'::json -> -1;
select '[10, 20, null, {"a": 1}]'::json -> -4;
select '[10, 20, null, {"a": 1}]'::json -> -5;
select '[10, 20, null, {"a": 1}]'::json -> 4;
select '[10, 20, null]'::json ->> 2;
select '[10, 20, null]'::json ->> 1;
select '"scalar"'::json -> 'a';
select '"scalar"'::json -> 0;
select '{"a":1}'::json -> 0;
select '[1,2]'::json -> 'a';
select '{"a": {"b": {"c": [0, 42]}}}'::json #> '{a,b,c}';
select '{"a": {"b": {"c": [0, 42]}}}'::json #> '{a,b,c,1}';
select '{"a": {"b": {"c": [0, 42]}}}'::json #>> '{a,b,c,1}';
select '{"a": {"b": 2}}'::json #> '{a,z}';
select '{"a": {"b": 2}}'::json #> '{}';
select '"x"'::json #> '{}';
select '"x"'::json #>> '{}';
select '{"a": [1, 2]}'::json #> '{a,-1}';
select '{"a": [1, 2]}'::json #> '{a,5}';
select '{"a": [1, 2]}'::json #> '{a,junk}';
select '{"a": 1}'::json #> '{NULL}'::text[];
select json_extract_path('{"a": {"b": "v"}}', 'a', 'b');
select json_extract_path_text('{"a": {"b": "v"}}', 'a', 'b');
select json_extract_path('{"a": [7]}', 'a', '0');
select json_extract_path_text('{"a": null}', 'a');

-- SRFs
select * from json_object_keys('{"b":1, "a":2, "b":3, "":4}');
select * from json_object_keys('{}');
select * from json_object_keys('[1]');
select * from json_object_keys('"x"');
select * from json_array_elements('[1, "a" , null, {"b": 2}, [3,4]]');
select * from json_array_elements('[]');
select * from json_array_elements('{"a":1}');
select * from json_array_elements('"x"');
select * from json_array_elements_text('[1, "a\nb", null, {"b": 2}]');
select * from json_array_elements_text('[]');
select * from json_each('{"a": 1, "b": {"c": 2},  "d" : null, "b": "dup"}');
select * from json_each('{}');
select * from json_each('[1]');
select * from json_each('"x"');
select * from json_each_text('{"a":1, "b":{"c":2}, "d":null, "e":"sé"}');
select * from json_each_text('{}');
select key from json_each('{"k1":1,"k2":2}');
select t.value from json_each_text('{"a":"v1","b":"v2"}') t;
select value ->> 'n' from json_array_elements('[{"n":1},{"n":2}]') e;

-- json_array_length
select json_array_length('[]');
select json_array_length('[1, [2,3], null, "x"]');
select json_array_length('{"a":1}');
select json_array_length('4');
select json_array_length('"x"');

-- json_strip_nulls
select json_strip_nulls('{"a":1,"b":null,"c":[2,null,3],"d":{"e":null,"f":4}}');
select json_strip_nulls('{"a":1,"b":null,"c":[2,null,3],"d":{"e":null,"f":4}}', true);
select json_strip_nulls('[1,{"a":null},null]');
select json_strip_nulls('[1,{"a":null},null]', true);
select json_strip_nulls('null');
select json_strip_nulls('{"a": "séq", "b\nkey": null, "c": "s"}');
select json_strip_nulls('  {"a" : 1}  ');

-- aggregates
create table jagg(g int, k text, j json, n numeric, s text, b jsonb);
insert into jagg values
  (1, 'a', '{"x": 1,  "x":2}', 1.5, 'one', '{"x":1}'),
  (1, 'b', '[1, 2]', 2, null, '[1,2]'),
  (1, 'c', 'null', null, 'three', 'null'),
  (2, 'd', '"str"', 4.25, 'four', '"str"'),
  (2, 'e', '{"y": {"z": true}}', -5, 'five', '{"y":{"z":true}}');
select json_agg(j) from jagg;
select json_agg(n) from jagg;
select json_agg(s) from jagg;
select json_agg(k) from jagg;
select json_agg(b) from jagg;
select json_agg(g) from jagg;
select g, json_agg(j) from jagg group by g order by g;
select g, json_agg(n) from jagg group by g order by g;
select json_agg(j) from jagg where false;
select json_agg_strict(s) from jagg;
select json_agg_strict(n) from jagg;
select json_object_agg(k, j) from jagg;
select json_object_agg(k, n) from jagg;
select g, json_object_agg(k, s) from jagg group by g order by g;
select json_object_agg(k, j) from jagg where false;
select json_object_agg(s, n) from jagg;
select json_object_agg(g, k) from jagg;
select json_object_agg_strict(k, s) from jagg;
select json_agg(j -> 'x') from jagg where g = 1;
select json_object_agg(k, to_json(n)) from jagg where g = 2;
drop table jagg;

-- to_json over table-fed composites (whole-row refs are the wholerow lane)
create table jrow(a int, txt text);
insert into jrow values (1, 'one'), (2, null);
select row_to_json(row(a, txt)) from jrow;
select to_json(row(a, txt)) from jrow;
select json_agg(row(a, txt)) from jrow;
drop table jrow;

-- operator + builder composition
select json_build_object('outer', json_build_array(1, json_build_object('k', 'v'))) -> 'outer';
select (json_build_object('a', 1, 'b', 2) ->> 'b')::int4;
select json_extract_path(to_json('{{1,2},{3,4}}'::int4[]), '1', '0');
