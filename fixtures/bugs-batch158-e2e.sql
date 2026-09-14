-- bugs/batch-158-backend-tsearch: SQL-level regressions, expected captured from C 18.6.
\set VERBOSITY verbose

-- fp-tsearch-wparser#1: a lextype method returning a NULL list is an empty set (tt_process_call).
CREATE FUNCTION b158_lt(internal) RETURNS internal LANGUAGE internal AS 'pg_stat_clear_snapshot';
CREATE TEXT SEARCH PARSER b158_p (START=prsd_start, GETTOKEN=prsd_nexttoken, END=prsd_end, LEXTYPES=b158_lt);
SELECT * FROM ts_token_type('b158_p');
SELECT count(*) FROM ts_token_type('default');

-- fp-tsearch-wparser#2: ts_headline_*_byid_opt called with three arguments treats options as NULL.
CREATE FUNCTION b158_h3(oid, text, tsquery) RETURNS text LANGUAGE internal AS 'ts_headline_byid_opt';
SELECT b158_h3('english'::regconfig::oid, 'the quick brown fox', 'fox'::tsquery);
CREATE FUNCTION b158_hj3(oid, jsonb, tsquery) RETURNS jsonb LANGUAGE internal AS 'ts_headline_jsonb_byid_opt';
SELECT b158_hj3('english'::regconfig::oid, '{"a":"the quick brown fox"}', 'fox'::tsquery);
CREATE FUNCTION b158_hs3(oid, json, tsquery) RETURNS json LANGUAGE internal AS 'ts_headline_json_byid_opt';
SELECT b158_hs3('english'::regconfig::oid, '{"a":"the quick brown fox"}', 'fox'::tsquery);

-- fp-tsearch-wparser#4: SRF_FIRSTCALL_INIT runs before the parser lookup.
CREATE FUNCTION b158_tt(oid) RETURNS record LANGUAGE internal AS 'ts_token_type_byid';
SELECT b158_tt(999999);
CREATE FUNCTION b158_tp(oid, text) RETURNS record LANGUAGE internal AS 'ts_parse_byid';
SELECT b158_tp(999999, 'x');
CREATE FUNCTION b158_ttn(text) RETURNS record LANGUAGE internal AS 'ts_token_type_byname';
SELECT b158_ttn('nosuch');

-- fp-tsearch-wparser_def#1: an overflowing headline option value overflows (22003) before its trailing non-ASCII byte is seen.
CREATE FUNCTION b158_bi(cstring) RETURNS text LANGUAGE internal AS 'byteain';
SELECT ts_headline('a b', 'a'::tsquery, 'MaxWords=99999999999' || b158_bi('\xff'));
SELECT ts_headline('a b', 'a'::tsquery, 'MaxWords=12' || b158_bi('\xff'));
SELECT ts_headline('a b', 'a'::tsquery, 'MaxWords=99999999999');

-- fp-adt-pg_locale_libc#3 / fp-contrib-ltree-ltxtquery_io#1 / fp-tsearch-b1#2 (22021 from the
-- classifiers under a non-C LC_CTYPE) are unit-tested in ts_locale: this harness pins LC_ALL=C.
