-- fmgr_sql parses the function body once (functions.c init_sql_fcache) and
-- retains the raw trees; prepare_next_query copies the retained tree instead
-- of re-lexing prosrc. A statement that flips standard_conforming_strings
-- therefore cannot change the literals of later statements: '\n' was lexed
-- as two characters when the body was parsed, and stays two.
-- (escape_string_warning is off only to keep scanner warnings out of the
-- expected output; they are not what this case pins.)
SET escape_string_warning = off;
SET standard_conforming_strings = on;
CREATE FUNCTION sqlfn_parse_once() RETURNS integer LANGUAGE sql VOLATILE
AS $$SET standard_conforming_strings = off; SELECT length('\n');$$;
SELECT sqlfn_parse_once();
SHOW standard_conforming_strings;
SELECT sqlfn_parse_once();
RESET standard_conforming_strings;
CREATE FUNCTION sqlfn_parse_once_multi() RETURNS text LANGUAGE sql VOLATILE
AS $$SELECT 1; SET standard_conforming_strings = off; SELECT 2; SELECT 'a\b' || length('\n');$$;
SELECT sqlfn_parse_once_multi();
SELECT sqlfn_parse_once_multi() FROM generate_series(1, 2);
RESET standard_conforming_strings;
RESET escape_string_warning;
DROP FUNCTION sqlfn_parse_once();
DROP FUNCTION sqlfn_parse_once_multi();
