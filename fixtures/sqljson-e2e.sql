-- sqljson lane differential battery: SQL/JSON constructors (JSON_OBJECT/
-- JSON_ARRAY/JSON_OBJECTAGG/JSON_ARRAYAGG/JSON()/JSON_SCALAR/JSON_SERIALIZE),
-- IS JSON, RETURNING coercions, NULL/ABSENT ON NULL, error texts.
-- Known-loud legs stay out: FORMAT JSON ENCODING + bytea (convert_from/to),
-- varchar-typmod RETURNING (varchar_support prosupport lane), DROP VIEW +
-- EXPLAIN of agg ORDER BY (pre-existing explain/DDL gaps),
-- json-returning WITH UNIQUE KEYS (json_unique hash), jsonb ABSENT+UNIQUE
-- object skip-uniquify, JSON_EXISTS/QUERY/VALUE execution (jsonpath exec),
-- JSON_TABLE (grammar loud).
\set VERBOSITY verbose
-- JSON_OBJECT
SELECT JSON_OBJECT();
SELECT JSON_OBJECT(RETURNING json);
SELECT JSON_OBJECT(RETURNING jsonb);
SELECT JSON_OBJECT(RETURNING text);
SELECT JSON_OBJECT('a': 1);
SELECT JSON_OBJECT('a' VALUE 1);
SELECT JSON_OBJECT('a': 1, 'b': 'two', 'c': true, 'd': NULL);
SELECT JSON_OBJECT('a': 1, 'b': 'two' RETURNING jsonb);
SELECT JSON_OBJECT('a': NULL NULL ON NULL);
SELECT JSON_OBJECT('a': NULL ABSENT ON NULL);
SELECT JSON_OBJECT('a': NULL ABSENT ON NULL RETURNING jsonb);
SELECT JSON_OBJECT('a': 1, 'a': 2);
SELECT JSON_OBJECT('a': 1, 'a': 2 RETURNING jsonb);
SELECT JSON_OBJECT('a': 1 WITH UNIQUE KEYS RETURNING jsonb);
SELECT JSON_OBJECT('a': 1, 'a': 2 WITH UNIQUE KEYS RETURNING jsonb);
SELECT JSON_OBJECT('a': JSON_OBJECT('b': 2), 'c': 3);
SELECT JSON_OBJECT('a': '{"x":1}'::json, 'b': '{"y":2}'::jsonb);
SELECT JSON_OBJECT('k': 1.5, 'l': '2026-01-02'::date);
SELECT JSON_OBJECT(NULL: 1);
SELECT pg_typeof(JSON_OBJECT()), pg_typeof(JSON_OBJECT(RETURNING jsonb)), pg_typeof(JSON_OBJECT(RETURNING text));
-- legacy json_object() form still routes to the function
SELECT JSON_OBJECT('{a,1,b,2}');
-- JSON_ARRAY
SELECT JSON_ARRAY();
SELECT JSON_ARRAY(RETURNING jsonb);
SELECT JSON_ARRAY(1, 'two', true, NULL);
SELECT JSON_ARRAY(1, NULL, 2 NULL ON NULL);
SELECT JSON_ARRAY(1, NULL, 2 ABSENT ON NULL);
SELECT JSON_ARRAY(1, NULL, 2 RETURNING jsonb);
SELECT JSON_ARRAY(1, NULL, 2 NULL ON NULL RETURNING jsonb);
SELECT JSON_ARRAY('{"a":1}'::json, '[2]'::jsonb);
SELECT JSON_ARRAY(JSON_ARRAY(1,2), JSON_OBJECT('a': 3));
-- JSON_ARRAY(query): parses/plans; execution rides the EXPR-sublink-over-agg
-- path, which returns NULL on current main (pre-existing, no SQL/JSON syntax:
-- `SELECT (SELECT json_agg(a) FROM (SELECT generate_series(1,3)) q(a));`
-- reproduces, as does the SRF-in-subquery-tlist form
-- `SELECT a FROM (SELECT generate_series(1,3)) q(a)` returning 0 rows) —
-- kept out of the differential until those lanes land.
-- JSON() / JSON_SCALAR / JSON_SERIALIZE
SELECT JSON('{"a": 1}');
SELECT JSON('  {"a" : 1 }  ');
SELECT JSON('{"a": 1}' RETURNING jsonb);
SELECT JSON('{"a": 1, "a": 2}' WITH UNIQUE KEYS RETURNING jsonb);
SELECT JSON('{"a": 1}'::jsonb);
SELECT JSON(NULL::text);
SELECT JSON('not json');
SELECT JSON_SCALAR(1);
SELECT JSON_SCALAR(1.5);
SELECT JSON_SCALAR('a "quoted" string');
SELECT JSON_SCALAR(true);
SELECT JSON_SCALAR(NULL);
SELECT JSON_SCALAR(NULL::int);
SELECT JSON_SCALAR(1 RETURNING jsonb);
SELECT JSON_SERIALIZE('{"a": 1}');
SELECT JSON_SERIALIZE('{"a": 1}'::jsonb);
SELECT pg_typeof(JSON_SERIALIZE('1'));
-- IS JSON predicate
SELECT '{"a":1}' IS JSON, '[1]' IS JSON, '1' IS JSON, 'oops' IS JSON;
SELECT '{"a":1}' IS NOT JSON, 'oops' IS NOT JSON;
SELECT '{"a":1}' IS JSON VALUE, '{"a":1}' IS JSON OBJECT, '{"a":1}' IS JSON ARRAY, '{"a":1}' IS JSON SCALAR;
SELECT '[1,2]' IS JSON OBJECT, '[1,2]' IS JSON ARRAY, '"x"' IS JSON SCALAR, '12' IS JSON SCALAR;
SELECT '{"a":1,"a":2}' IS JSON WITHOUT UNIQUE KEYS;
SELECT '{"a":1}'::json IS JSON OBJECT;
SELECT '{"a":1}'::jsonb IS JSON, '[1]'::jsonb IS JSON ARRAY, '1'::jsonb IS JSON SCALAR, '{"a":1}'::jsonb IS JSON SCALAR;
SELECT NULL::text IS JSON, NULL::jsonb IS NOT JSON;
SELECT 1::int IS JSON;
-- JSON_OBJECTAGG / JSON_ARRAYAGG
CREATE TABLE sqljson_t (k text, v int);
INSERT INTO sqljson_t VALUES ('a', 1), ('b', 2), ('c', NULL), ('d', 4);
SELECT JSON_OBJECTAGG(k: v) FROM sqljson_t;
SELECT JSON_OBJECTAGG(k VALUE v) FROM sqljson_t;
SELECT JSON_OBJECTAGG(k: v NULL ON NULL) FROM sqljson_t;
SELECT JSON_OBJECTAGG(k: v ABSENT ON NULL) FROM sqljson_t;
SELECT JSON_OBJECTAGG(k: v RETURNING jsonb) FROM sqljson_t;
SELECT JSON_OBJECTAGG(k: v ABSENT ON NULL RETURNING jsonb) FROM sqljson_t;
SELECT JSON_OBJECTAGG(k: v) FILTER (WHERE v > 1) FROM sqljson_t;
SELECT JSON_ARRAYAGG(v) FROM sqljson_t;
SELECT JSON_ARRAYAGG(v NULL ON NULL) FROM sqljson_t;
SELECT JSON_ARRAYAGG(v ABSENT ON NULL) FROM sqljson_t;
SELECT JSON_ARRAYAGG(v ORDER BY v DESC NULLS LAST) FROM sqljson_t;
SELECT JSON_ARRAYAGG(v RETURNING jsonb) FROM sqljson_t;
SELECT JSON_ARRAYAGG(v ORDER BY k DESC ABSENT ON NULL RETURNING jsonb) FROM sqljson_t;
SELECT k, JSON_ARRAYAGG(v) FROM sqljson_t GROUP BY k ORDER BY k;
-- constructors over table columns
SELECT JSON_OBJECT(k: v) FROM sqljson_t ORDER BY k;
SELECT JSON_ARRAY(k, v) FROM sqljson_t ORDER BY k;
-- EXPLAIN / deparse
EXPLAIN (VERBOSE, COSTS OFF) SELECT JSON_OBJECT('a': 1 RETURNING jsonb);
EXPLAIN (VERBOSE, COSTS OFF) SELECT JSON_ARRAY(1, NULL ABSENT ON NULL);
EXPLAIN (VERBOSE, COSTS OFF) SELECT JSON_OBJECTAGG(k: v ABSENT ON NULL) FROM sqljson_t;
EXPLAIN (VERBOSE, COSTS OFF) SELECT JSON_ARRAYAGG(v ABSENT ON NULL RETURNING jsonb) FROM sqljson_t;
EXPLAIN (VERBOSE, COSTS OFF) SELECT k IS JSON FROM sqljson_t;
EXPLAIN (VERBOSE, COSTS OFF) SELECT JSON('{"x":1}');
EXPLAIN (VERBOSE, COSTS OFF) SELECT JSON_SCALAR(v) FROM sqljson_t;
EXPLAIN (VERBOSE, COSTS OFF) SELECT JSON_SERIALIZE('{"a":1}' RETURNING text);
-- views (outfuncs/readfuncs + deparse round trip)
CREATE VIEW sqljson_v AS
  SELECT JSON_OBJECT('k': k, 'v': v ABSENT ON NULL RETURNING jsonb) AS o,
         JSON_ARRAY(v, k NULL ON NULL) AS a,
         k IS JSON VALUE AS isj
  FROM sqljson_t;
SELECT pg_get_viewdef('sqljson_v'::regclass);
SELECT * FROM sqljson_v ORDER BY o::text;
-- error shapes
SELECT JSON_OBJECT(1: 1);
SELECT JSON_OBJECT('a': 1 RETURNING int);
SELECT JSON_SERIALIZE('{"a":1}' RETURNING int);
SELECT JSON('{"a":1}' RETURNING int);
SELECT JSON_SCALAR(1 RETURNING text);
SELECT JSON_ARRAYAGG(v RETURNING int) FROM sqljson_t;
SELECT JSON_OBJECT('a': 1) IS JSON OBJECT;
-- column naming
SELECT JSON_OBJECT('a': 1) AS explicit_name;
SELECT JSON_OBJECT('a': 1);
SELECT JSON_ARRAY(1);
SELECT JSON('1');
SELECT JSON_SCALAR(1);
SELECT JSON_SERIALIZE('1');
SELECT JSON_OBJECTAGG('a': 1);
SELECT JSON_ARRAYAGG(1);
