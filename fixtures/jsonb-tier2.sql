-- jsonb tier 2 differential corpus: mutations, construction, aggregates,
-- SRFs, casts. Run two-binary via scripts/regress-diff.sh (pgrust vs C 18.3).
-- ARRAY[] literals and VALUES-alias column lists are avoided (parser units
-- outside the jsonb lane); text[] literals use the '{...}' form.
\set VERBOSITY verbose

-- || concatenation
select '{"a":1}'::jsonb || '{"b":2}'::jsonb;
select '{"a":1,"b":2}'::jsonb || '{"b":3,"c":4}'::jsonb;
select '{"a":{"x":1}}'::jsonb || '{"a":{"y":2}}'::jsonb;
select '[1,2]'::jsonb || '[3,4]'::jsonb;
select '[1,2]'::jsonb || '"x"'::jsonb;
select '"x"'::jsonb || '[1,2]'::jsonb;
select '"a"'::jsonb || '"b"'::jsonb;
select '{"a":1}'::jsonb || '[1]'::jsonb;
select '[1]'::jsonb || '{"a":1}'::jsonb;
select '{}'::jsonb || '{"a":1}'::jsonb;
select '{"a":1}'::jsonb || '{}'::jsonb;
select '[]'::jsonb || '[1]'::jsonb;
select '[1]'::jsonb || '[]'::jsonb;
select '{}'::jsonb || '[]'::jsonb;
select '[]'::jsonb || '{}'::jsonb;
select '3'::jsonb || '[]'::jsonb;
select '3'::jsonb || '4'::jsonb;
select '{"a":1}'::jsonb || 'null'::jsonb;
select 'null'::jsonb || '{"a":1}'::jsonb;
select '{"a":[1,2],"b":{"c":3}}'::jsonb || '{"b":{"d":4},"e":[5]}'::jsonb;

-- - text
select '{"a":1,"b":2}'::jsonb - 'a';
select '{"a":1,"b":2}'::jsonb - 'z';
select '["a","b","a",1]'::jsonb - 'a';
select '{}'::jsonb - 'a';
select '[]'::jsonb - 'a';
select '{"a":{"a":1}}'::jsonb - 'a';
select '"a"'::jsonb - 'a';
select '1'::jsonb - 'a';

-- - int
select '[1,2,3]'::jsonb - 0;
select '[1,2,3]'::jsonb - 2;
select '[1,2,3]'::jsonb - 3;
select '[1,2,3]'::jsonb - (-1);
select '[1,2,3]'::jsonb - (-3);
select '[1,2,3]'::jsonb - (-4);
select '[]'::jsonb - 0;
select '{"a":1}'::jsonb - 0;
select '"x"'::jsonb - 0;

-- - text[]
select '{"a":1,"b":2,"c":3}'::jsonb - '{a,c}'::text[];
select '{"a":1,"b":2}'::jsonb - '{}'::text[];
select '["a","b","c"]'::jsonb - '{a,c}'::text[];
select '{"a":1}'::jsonb - '{"z"}'::text[];
select '"s"'::jsonb - '{a}'::text[];
select jsonb_delete('{"a":1,"b":2}', variadic '{a,b}'::text[]);

-- #- path delete
select '{"a":{"b":1,"c":2}}'::jsonb #- '{a,b}';
select '{"a":{"b":1}}'::jsonb #- '{a}';
select '{"a":[1,2,3]}'::jsonb #- '{a,1}';
select '{"a":[1,2,3]}'::jsonb #- '{a,-1}';
select '{"a":[1,2,3]}'::jsonb #- '{a,-4}';
select '{"a":1}'::jsonb #- '{z,b}';
select '{"a":1}'::jsonb #- '{}'::text[];
select '{"a":{"b":1}}'::jsonb #- '{a,b,c}';
select '"x"'::jsonb #- '{a}';
select '[1,2]'::jsonb #- '{0}';
select '{"a":1}'::jsonb #- '{a,x}';
select '[[1,2],[3]]'::jsonb #- '{0,1}';
select '{"a":[1,2]}'::jsonb #- '{a,x}';

-- jsonb_set
select jsonb_set('{"a":1,"b":2}', '{a}', '9');
select jsonb_set('{"a":1}', '{b}', '2');
select jsonb_set('{"a":1}', '{b}', '2', false);
select jsonb_set('{"a":1}', '{b}', '2', true);
select jsonb_set('{"a":{"b":1}}', '{a,b}', '"x"');
select jsonb_set('{"a":{"b":1}}', '{a,c}', '[1,2]');
select jsonb_set('{"a":{"b":1}}', '{a,c,d}', '1');
select jsonb_set('[1,2,3]', '{1}', '"two"');
select jsonb_set('[1,2,3]', '{-1}', '99');
select jsonb_set('[1,2,3]', '{5}', '99');
select jsonb_set('[1,2,3]', '{5}', '99', false);
select jsonb_set('[1,2,3]', '{-5}', '99');
select jsonb_set('[1,2,3]', '{-5}', '99', false);
select jsonb_set('{"a":[1,2]}', '{a,0}', '{"x":true}');
select jsonb_set('{}', '{a}', '1');
select jsonb_set('{}', '{a}', '1', false);
select jsonb_set('[]', '{0}', '1');
select jsonb_set('[]', '{0}', '1', false);
select jsonb_set('"x"', '{a}', '1');
select jsonb_set('{"a":1}', '{a,b}', '2');
select jsonb_set('{"a":1}', '{NULL}', '2');
select jsonb_set('[1,2]', '{a}', '3');
select jsonb_set('[1,2]', '{1a}', '3');
select jsonb_set('{"a":[1]}', '{a,99999999999}', '3');

-- jsonb_insert
select jsonb_insert('[1,3]', '{1}', '2');
select jsonb_insert('[1,3]', '{1}', '2', true);
select jsonb_insert('[1,3]', '{-1}', '2');
select jsonb_insert('[1,3]', '{-1}', '2', true);
select jsonb_insert('[1,3]', '{0}', '0');
select jsonb_insert('[1,3]', '{99}', '9');
select jsonb_insert('[1,3]', '{-99}', '9');
select jsonb_insert('{"a":{"b":[1,2]}}', '{a,b,1}', '"new"');
select jsonb_insert('{"a":1}', '{b}', '2');
select jsonb_insert('{"a":1}', '{a}', '2');
select jsonb_insert('{"a":1}', '{a}', '2', true);
select jsonb_insert('"x"', '{0}', '1');
select jsonb_insert('{}', '{a}', '1');
select jsonb_insert('[]', '{0}', '1');

-- jsonb_set_lax
select jsonb_set_lax('{"a":1}', '{a}', '2');
select jsonb_set_lax('{"a":1}', '{a}', null);
select jsonb_set_lax('{"a":1}', '{a}', null, true, 'use_json_null');
select jsonb_set_lax('{"a":1}', '{a}', null, true, 'delete_key');
select jsonb_set_lax('{"a":1}', '{a}', null, true, 'return_target');
select jsonb_set_lax('{"a":1}', '{a}', null, true, 'raise_exception');
select jsonb_set_lax('{"a":1}', '{a}', null, true, 'nonsense');
select jsonb_set_lax('{"a":1}', '{a}', null, true, null);
select jsonb_set_lax(null, '{a}', '1', true, 'use_json_null');
select jsonb_set_lax('{"a":1}', null, '1', true, 'use_json_null');
select jsonb_set_lax('{"a":1}', '{a}', '1', null, 'use_json_null');

-- jsonb_pretty
select jsonb_pretty('{}');
select jsonb_pretty('[]');
select jsonb_pretty('"x"');
select jsonb_pretty('null');
select jsonb_pretty('{"a":1}');
select jsonb_pretty('[1,2,3]');
select jsonb_pretty('{"a":[1,2,{"b":null}],"c":{"d":true,"e":[]},"f":{}}');
select jsonb_pretty('[[1,[2,[3]]],{"a":{"b":{"c":1}}}]');
select jsonb_pretty('{"a":"quoted \"string\" here","b":"é中"}');

-- casts
select 'true'::jsonb::bool;
select 'false'::jsonb::bool;
select 'null'::jsonb::bool is null;
select '1'::jsonb::bool;
select '"true"'::jsonb::bool;
select '[true]'::jsonb::bool;
select '{"a":true}'::jsonb::bool;
select '1.500'::jsonb::numeric;
select '-0.0'::jsonb::numeric;
select '1e10'::jsonb::numeric;
select 'null'::jsonb::numeric is null;
select '"1"'::jsonb::numeric;
select 'true'::jsonb::numeric;
select '32767'::jsonb::int2;
select '32768'::jsonb::int2;
select '1.7'::jsonb::int2;
select '2147483647'::jsonb::int4;
select '2147483648'::jsonb::int4;
select '-2.5'::jsonb::int4;
select 'null'::jsonb::int4 is null;
select '[1]'::jsonb::int4;
select '9223372036854775807'::jsonb::int8;
select '9223372036854775808'::jsonb::int8;
select '1.25'::jsonb::float4;
select '1e40'::jsonb::float4;
select '1.25'::jsonb::float8;
select '1e10'::jsonb::float8;
select 'null'::jsonb::float8 is null;
select '"x"'::jsonb::float8;

-- to_jsonb
select to_jsonb(42);
select to_jsonb(32767::int2);
select to_jsonb(9223372036854775807::int8);
select to_jsonb(1.5::numeric);
select to_jsonb('NaN'::numeric);
select to_jsonb('Infinity'::float8);
select to_jsonb('-Infinity'::float4);
select to_jsonb('NaN'::float8);
select to_jsonb(1.25::float4);
select to_jsonb(1.5e300::float8);
select to_jsonb(true), to_jsonb(false);
select to_jsonb('plain'::text);
select to_jsonb('quo"te\and\\back'::text);
select to_jsonb(''::text);
-- varchar/interval/inet/rowexpr sources: out-fn and RowExpr lanes unported
select to_jsonb('c'::"char");
select to_jsonb('2024-01-02'::date);
select to_jsonb('infinity'::date);
select to_jsonb('2024-01-02 03:04:05.678901'::timestamp);
select to_jsonb('infinity'::timestamp);
select to_jsonb('-infinity'::timestamp);
set timezone = 'UTC';
select to_jsonb('2024-01-02 03:04:05.6+05'::timestamptz);
set timezone = 'America/New_York';
select to_jsonb('2024-07-02 03:04:05+00'::timestamptz);
set timezone = 'UTC';
select to_jsonb('{1,2,3}'::int4[]);
select to_jsonb('{}'::int4[]);
select to_jsonb('{{1,2},{3,4}}'::int4[]);
select to_jsonb('{a,NULL,c}'::text[]);
select to_jsonb('{1.5,NaN}'::float8[]);


select to_jsonb('{"j":1}'::json);
select to_jsonb('[1,"two"]'::json);
select to_jsonb('"scal"'::json);
select to_jsonb('{"j":1}'::jsonb);
select to_jsonb('"scal"'::jsonb);
select to_jsonb('7'::jsonb);


select to_jsonb('\x1234'::bytea);

-- jsonb_build_object / jsonb_build_array
select jsonb_build_object();
select jsonb_build_object('a', 1);
select jsonb_build_object('a', 1, 'b', null, 'c', 'x', 'd', true);
select jsonb_build_object('a', '{"n":1}'::jsonb, 'b', '{1,2}'::int4[]);

select jsonb_build_object(1, 2);
select jsonb_build_object(1.5, 2);
select jsonb_build_object(true, 2);
select jsonb_build_object('2024-01-02'::date, 1);
select jsonb_build_object('a');
select jsonb_build_object('a', 1, 'b');
select jsonb_build_object(null, 1);
select jsonb_build_object('a'::text, null);
select jsonb_build_object('{"a":1}'::jsonb, 1);
select jsonb_build_array();
select jsonb_build_array(1, 'a', null, true, 2.5);
select jsonb_build_array('{"x":1}'::jsonb, '{1,2}'::int4[]);
select jsonb_build_array(null::int4);
select jsonb_build_object(variadic '{a,1,b,2}'::text[]);
select jsonb_build_array(variadic '{1,2,3}'::int4[]);
select jsonb_build_array(variadic '{a,NULL}'::text[]);
select jsonb_build_object(variadic '{a,1,b}'::text[]);
select jsonb_build_object(variadic null::text[]);

-- aggregates
create table t2agg(g int, k text, j jsonb, n numeric, s text);
insert into t2agg values
  (1, 'a', '{"x":1}', 1.5, 'one'),
  (1, 'b', '[1,2]', 2, null),
  (1, 'c', 'null', null, 'three'),
  (2, 'd', '"str"', 4.25, 'four'),
  (2, 'e', '{"y":{"z":true}}', -5, 'five');
select jsonb_agg(j) from t2agg;
select jsonb_agg(n) from t2agg;
select jsonb_agg(s) from t2agg;
select jsonb_agg(k) from t2agg;
select g, jsonb_agg(j) from t2agg group by g order by g;
select g, jsonb_agg(n) from t2agg group by g order by g;
select jsonb_agg(j) from t2agg where false;
select jsonb_object_agg(k, j) from t2agg;
select jsonb_object_agg(k, n) from t2agg;
select g, jsonb_object_agg(k, s) from t2agg group by g order by g;
select jsonb_object_agg(k, j) from t2agg where false;
select jsonb_object_agg(s, n) from t2agg;
select jsonb_agg(g) from t2agg;
select jsonb_object_agg(g, k) from t2agg;

-- SRFs in FROM
select * from jsonb_object_keys('{"b":1,"a":2,"":3}');
select * from jsonb_object_keys('{}');
select * from jsonb_object_keys('[1]');
select * from jsonb_object_keys('"x"');
select * from jsonb_array_elements('[1,"a",null,{"b":2},[3,4]]');
select * from jsonb_array_elements('[]');
select * from jsonb_array_elements('{"a":1}');
select * from jsonb_array_elements('"x"');
select * from jsonb_array_elements_text('[1,"a",null,{"b":2}]');
select * from jsonb_array_elements_text('[]');
select * from jsonb_each('{"a":1,"b":{"c":2},"d":null}');
select * from jsonb_each('{}');
select * from jsonb_each('[1]');
select * from jsonb_each('"x"');
select * from jsonb_each_text('{"a":1,"b":{"c":2},"d":null,"e":"s"}');
select * from jsonb_each_text('{}');
select key from jsonb_each('{"k1":1,"k2":2}');
select t.value from jsonb_each_text('{"a":"v1","b":"v2"}') t;
select value ->> 'n' from jsonb_array_elements('[{"n":1},{"n":2}]') e;
select * from jsonb_object_keys('{"x":1}'::jsonb);

-- mutations over table rows (agg + mutation composition)
select jsonb_agg(j || '{"tag":true}'::jsonb) from t2agg where g = 1;
select jsonb_object_agg(k, jsonb_set(j, '{added}', to_jsonb(g), true)) from t2agg where g = 2;
select g, jsonb_pretty(jsonb_agg(j)) from t2agg group by g order by g;
drop table t2agg;

-- deep nesting round trips
select jsonb_set(jsonb_set('{"a":{"b":{"c":[1,2,3]}}}', '{a,b,c,1}', '{"deep":true}'), '{a,z}', '"w"');
select ('{"a":{"b":{"c":1}}}'::jsonb #- '{a,b,c}') || '{"n":[]}'::jsonb;
select jsonb_insert(jsonb_insert('[[],[1]]', '{0,0}', '0'), '{1,0}', '9', true);
