-- bugs/batch-28-backend-utils-cache: cache fixes, expected captured from C 18.6.
\set VERBOSITY verbose
-- fp-adt-arrayfuncs-p2#1 / fp-cache-typcache#1: nested anonymous records
-- re-enter the RECORD typcache comparator (C aliases the FmgrInfo).
SELECT ARRAY[ROW(ROW(ROW(1)))] = ARRAY[ROW(ROW(ROW(1)))];
SELECT ARRAY[ROW(ROW(ROW(1)))] < ARRAY[ROW(ROW(ROW(2)))];
SELECT ARRAY[ROW(ARRAY[ROW(ARRAY[ROW(1)])])] = ARRAY[ROW(ARRAY[ROW(ARRAY[ROW(1)])])];
SELECT ARRAY[ROW(ROW(ROW(1)))] @> ARRAY[ROW(ROW(ROW(1)))];
SELECT array_position(ARRAY[ROW(ROW(ROW(1))), ROW(ROW(ROW(2)))], ROW(ROW(ROW(2))));
SELECT ROW(ROW(ROW(ROW(1)))) = ROW(ROW(ROW(ROW(1))));
SELECT ROW(ROW(ROW(ROW(1)))) < ROW(ROW(ROW(ROW(2))));
-- fp-plan-setrefs-p2#1: a retained CoerceToDomain is not a query-source
-- dependency (fix_expr_common), so ALTER DOMAIN does not re-analyze the
-- prepared statement; its runtime check still sees the new constraint.
CREATE DOMAIN b28_d AS integer CONSTRAINT positive CHECK (VALUE > 0);
SET plan_cache_mode = force_generic_plan;
PREPARE b28_p(integer) AS SELECT 'now'::timestamp AS prepared_at, $1::b28_d AS v;
EXECUTE b28_p(1) \gset first_
SELECT pg_sleep(0.05);
ALTER DOMAIN b28_d DROP CONSTRAINT positive;
EXECUTE b28_p(1) \gset second_
SELECT :'first_prepared_at' = :'second_prepared_at' AS same_prepared_at;
ALTER DOMAIN b28_d ADD CONSTRAINT positive CHECK (VALUE > 0);
EXECUTE b28_p(-1);
DEALLOCATE b28_p;
RESET plan_cache_mode;
DROP DOMAIN b28_d;
-- fp-cache-relcache-p2#1: a trigger on a bootstrap catalog fires in a fresh
-- backend (RelationCacheInitializePhase3 reloads relhastriggers).
CREATE FUNCTION b28_victim() RETURNS int LANGUAGE SQL AS 'SELECT 1';
CREATE FUNCTION b28_guard() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'b28_guard fired for %', OLD.proname; END $$;
SET allow_system_table_mods = on;
CREATE TRIGGER b28_guard_tr BEFORE UPDATE ON pg_catalog.pg_proc FOR EACH ROW EXECUTE FUNCTION b28_guard();
\c
SET allow_system_table_mods = on;
UPDATE pg_catalog.pg_proc SET prosrc = prosrc WHERE proname = 'b28_victim';
DROP TRIGGER b28_guard_tr ON pg_catalog.pg_proc;
\c
SET allow_system_table_mods = on;
UPDATE pg_catalog.pg_proc SET prosrc = prosrc WHERE proname = 'b28_victim';
RESET allow_system_table_mods;
DROP FUNCTION b28_guard();
DROP FUNCTION b28_victim();
