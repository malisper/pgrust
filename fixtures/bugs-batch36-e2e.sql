-- bugs/batch-36-backend-replication-misc: expected captured from C 18.6.
-- fp-logical-origin#1: replication origin names keep their database-encoded
-- bytes (text_to_cstring, origin.c:1308): in a SQL_ASCII database two
-- distinct non-UTF-8 names are two origins.
CREATE DATABASE b36_ascii TEMPLATE template0 ENCODING 'SQL_ASCII' LC_COLLATE 'C' LC_CTYPE 'C';
\c b36_ascii
SELECT pg_replication_origin_create(convert_from(decode('ff','hex'),'SQL_ASCII'));
SELECT pg_replication_origin_create(convert_from(decode('fe','hex'),'SQL_ASCII'));
SELECT encode(convert_to(roname,'SQL_ASCII'),'hex') AS roname_hex FROM pg_replication_origin ORDER BY 1;
SELECT pg_replication_origin_oid(convert_from(decode('fe','hex'),'SQL_ASCII'))
     = (SELECT roident FROM pg_replication_origin WHERE encode(convert_to(roname,'SQL_ASCII'),'hex') = 'fe') AS oid_matches;
SELECT pg_replication_origin_oid(convert_from(decode('fd','hex'),'SQL_ASCII')) IS NULL AS unknown_is_null;
SELECT pg_replication_origin_drop(convert_from(decode('ff','hex'),'SQL_ASCII'));
SELECT pg_replication_origin_drop(convert_from(decode('fe','hex'),'SQL_ASCII'));
SELECT count(*) FROM pg_replication_origin;
\c postgres
DROP DATABASE b36_ascii;
