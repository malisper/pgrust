-- bugs/batch-69-misc-backend: SQL-reachable rows, expected captured from C 18.6.
\set VERBOSITY verbose
CREATE FUNCTION b69_canon(text) RETURNS text AS '$libdir/regress', 'test_canonicalize_path' LANGUAGE C STRICT;
CREATE FUNCTION b69_b2t(bytea) RETURNS text AS '$libdir/regress', 'test_bytea_to_text' LANGUAGE C STRICT;
CREATE FUNCTION b69_t2b(text) RETURNS bytea AS '$libdir/regress', 'test_text_to_bytea' LANGUAGE C STRICT;
CREATE FUNCTION b69_valid_enc(text) RETURNS bool AS '$libdir/regress', 'test_valid_server_encoding' LANGUAGE C STRICT;
CREATE FUNCTION b69_bad_enc(bytea, name, name, boolean) RETURNS integer AS '$libdir/regress', 'test_enc_conversion' LANGUAGE C STRICT;
-- fp-regress-regress#6: test_canonicalize_path works on the raw C string
-- (bytes kept, embedded NUL terminates).
SELECT encode(b69_t2b(b69_canon(b69_b2t(decode('ff2f2f78', 'hex')))), 'hex');
SELECT encode(b69_t2b(b69_canon(b69_b2t(decode('612f2f622f006464', 'hex')))), 'hex');
SELECT b69_canon('/a//b/./c/../d/');
-- fp-regress-regress#7: test_valid_server_encoding stops at an embedded NUL.
SELECT b69_valid_enc(b69_b2t(decode('555446380078', 'hex')));
SELECT b69_valid_enc(b69_b2t(decode('5554463878', 'hex')));
SELECT b69_valid_enc('UTF8');
-- fp-regress-regress#8: the result-type check precedes input validation.
SELECT b69_bad_enc(decode('ff', 'hex'), 'UTF8', 'UTF8', false);
SELECT b69_bad_enc(decode('ff', 'hex'), 'UTF8', 'LATIN1', false);
SELECT b69_bad_enc(decode('ff', 'hex'), 'NOSUCH', 'UTF8', false);
SELECT b69_bad_enc(decode('ff', 'hex'), 'UTF8', 'NOSUCH', false);
DROP FUNCTION b69_bad_enc(bytea, name, name, boolean);
DROP FUNCTION b69_valid_enc(text);
DROP FUNCTION b69_t2b(text);
DROP FUNCTION b69_b2t(bytea);
DROP FUNCTION b69_canon(text);
-- fp-rewrite-b1#1: get_rewrite_oid goes through the RULERELNAME syscache
-- (positive, negative, overlong-name, rename-collision and post-drop lookups).
CREATE TABLE b69r(i int);
CREATE RULE b69rule AS ON INSERT TO b69r DO INSTEAD NOTHING;
SELECT pg_describe_object(classid, objid, objsubid) FROM pg_get_object_address('rule', ARRAY['public', 'b69r', 'b69rule'], ARRAY[]::text[]);
SELECT pg_describe_object(classid, objid, objsubid) FROM pg_get_object_address('rule', ARRAY['public', 'b69r', 'b69rule'], ARRAY[]::text[]);
SELECT pg_get_object_address('rule', ARRAY['public', 'b69r', 'nosuch'], ARRAY[]::text[]);
SELECT pg_get_object_address('rule', ARRAY['public', 'b69r', repeat('x', 70)], ARRAY[]::text[]);
ALTER RULE b69rule ON b69r RENAME TO b69rule2;
SELECT pg_describe_object(classid, objid, objsubid) FROM pg_get_object_address('rule', ARRAY['public', 'b69r', 'b69rule2'], ARRAY[]::text[]);
SELECT pg_get_object_address('rule', ARRAY['public', 'b69r', 'b69rule'], ARRAY[]::text[]);
CREATE RULE b69rule3 AS ON UPDATE TO b69r DO INSTEAD NOTHING;
ALTER RULE b69rule3 ON b69r RENAME TO b69rule2;
ALTER RULE nosuch ON b69r RENAME TO b69rule4;
DROP RULE b69rule2 ON b69r;
SELECT pg_get_object_address('rule', ARRAY['public', 'b69r', 'b69rule2'], ARRAY[]::text[]);
DROP TABLE b69r;
