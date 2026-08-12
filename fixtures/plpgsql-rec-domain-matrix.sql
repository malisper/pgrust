\set VERBOSITY default
CREATE TYPE q4comp AS (x int, y text);
CREATE DOMAIN q4dom AS q4comp CHECK ((VALUE).x > 0);
CREATE DOMAIN q4domnn AS q4comp CHECK (VALUE IS NOT NULL);
CREATE DOMAIN q4dnnf AS q4comp CHECK ((VALUE).x IS NOT NULL);
CREATE DOMAIN q4dd AS q4dom CHECK ((VALUE).x < 100);
CREATE TABLE q4t (c q4comp);
INSERT INTO q4t VALUES (ROW(1,'a')), (ROW(2,'b'));
CREATE TABLE q4s (x int, y text);
INSERT INTO q4s VALUES (1,'a'), (2,'b');
-- 1: original repro
DO $$ DECLARE d q4dom; BEGIN d := ROW(1, 'ok'); END $$;
DO $$ DECLARE d q4dom; BEGIN BEGIN d := ROW(-1, 'bad'); EXCEPTION WHEN check_violation THEN RAISE NOTICE 'trapped'; END; END $$;
-- 2: whole assign, read whole and fields
DO $$ DECLARE d q4dom; BEGIN d := ROW(2,'two'); RAISE NOTICE 'd=% x=% y=%', d, d.x, d.y; END $$;
-- 3: declare with default (domain and plain composite)
DO $$ DECLARE d q4dom := ROW(5,'dflt'); BEGIN RAISE NOTICE 'ddef=%', d; END $$;
DO $$ DECLARE c q4comp := ROW(7,'z'); BEGIN RAISE NOTICE 'cdef=% % %', c, c.x, c.y; END $$;
DO $$ DECLARE d q4dom := ROW(-5,'bad'); BEGIN RAISE NOTICE 'unreached %', d; END $$;
-- 4: field assign: ok then violating
DO $$ DECLARE d q4dom; BEGIN d.x := 3; RAISE NOTICE 'fx=% d=%', d.x, d; END $$;
DO $$ DECLARE d q4dom; BEGIN d.x := 3; d.x := -1; RAISE NOTICE 'unreached'; END $$;
DO $$ DECLARE d q4dom; BEGIN d.y := 'only-y'; RAISE NOTICE 'y-only=%', d; END $$;
-- 5: defaultless declare of NULL-rejecting domain: C checks NULL at entry
DO $$ DECLARE d q4domnn; BEGIN RAISE NOTICE 'unreached'; END $$;
-- 6: NULL assignment
DO $$ DECLARE d q4dom; BEGIN d := ROW(1,'a'); d := NULL; RAISE NOTICE 'isnull=%', d IS NULL; END $$;
DO $$ DECLARE d q4domnn := ROW(1,'a'); BEGIN d := NULL; RAISE NOTICE 'unreached'; END $$;
-- 7: SELECT INTO (fields), ok / violating / no rows
DO $$ DECLARE d q4dom; BEGIN SELECT 3,'q' INTO d; RAISE NOTICE 'into=%', d; END $$;
DO $$ DECLARE d q4dom; BEGIN SELECT -3,'q' INTO d; RAISE NOTICE 'unreached'; END $$;
DO $$ DECLARE d q4dnnf; BEGIN SELECT x,y INTO d FROM q4s WHERE false; RAISE NOTICE 'norows=% isnull=%', d, d IS NULL; END $$;
-- 8: FOR rec IN query
DO $$ DECLARE d q4dom; BEGIN FOR d IN SELECT x,y FROM q4s ORDER BY x LOOP RAISE NOTICE 'for=%', d; END LOOP; END $$;
DO $$ DECLARE d q4dom; BEGIN INSERT INTO q4s VALUES (-9,'neg'); FOR d IN SELECT x,y FROM q4s ORDER BY x LOOP RAISE NOTICE 'for2=%', d; END LOOP; END $$;
DELETE FROM q4s WHERE x = -9;
-- 9: FOREACH over composite array
DO $$ DECLARE d q4dom; BEGIN FOREACH d IN ARRAY ARRAY[ROW(1,'a')::q4comp, ROW(2,'b')::q4comp] LOOP RAISE NOTICE 'fe=%', d; END LOOP; END $$;
DO $$ DECLARE d q4dom; BEGIN FOREACH d IN ARRAY ARRAY[ROW(1,'a')::q4comp, ROW(-1,'b')::q4comp] LOOP RAISE NOTICE 'fe2=%', d; END LOOP; END $$;
-- 10: cross assignment domain <-> composite <-> record
DO $$ DECLARE d q4dom; c q4comp; r record; BEGIN c := ROW(4,'c'); d := c; r := d; RAISE NOTICE 'r=% rx=%', r, r.x; c := d; RAISE NOTICE 'c=%', c; END $$;
DO $$ DECLARE d q4dom; c q4comp; BEGIN c := ROW(-4,'c'); d := c; RAISE NOTICE 'unreached'; END $$;
-- 11: nested domain over domain over composite
DO $$ DECLARE d q4dd; BEGIN d := ROW(5,'n'); RAISE NOTICE 'dd=%', d; END $$;
DO $$ DECLARE d q4dd; BEGIN d := ROW(-5,'n'); END $$;
DO $$ DECLARE d q4dd; BEGIN d := ROW(500,'n'); END $$;
-- 12: %ROWTYPE
DO $$ DECLARE v q4s%ROWTYPE; BEGIN SELECT x,y INTO v FROM q4s ORDER BY x LIMIT 1; RAISE NOTICE 'rt=%', v; END $$;
-- 13: functions RETURNing domain / taking domain arg
CREATE FUNCTION q4f_ret() RETURNS q4dom LANGUAGE plpgsql AS $$ DECLARE d q4dom; BEGIN d := ROW(6,'r'); RETURN d; END $$;
SELECT q4f_ret(), (q4f_ret()).x;
CREATE FUNCTION q4f_bad() RETURNS q4dom LANGUAGE plpgsql AS $$ DECLARE c q4comp; BEGIN c := ROW(-6,'r'); RETURN c; END $$;
SELECT q4f_bad();
CREATE FUNCTION q4f_arg(d q4dom) RETURNS int LANGUAGE plpgsql AS $$ BEGIN RETURN d.x; END $$;
SELECT q4f_arg(ROW(9,'p')::q4comp);
-- 14: CONSTANT and NOT NULL markers on composite/domain variables
DO $$ DECLARE d CONSTANT q4dom := ROW(1,'k'); BEGIN d := ROW(2,'k'); END $$;
DO $$ DECLARE c CONSTANT q4comp := ROW(1,'k'); BEGIN c := ROW(2,'k'); END $$;
DO $$ DECLARE d q4dom NOT NULL := ROW(1,'k'); BEGIN d := NULL; END $$;
DO $$ DECLARE c q4comp NOT NULL := ROW(1,'k'); BEGIN c := NULL; END $$;
DO $$ DECLARE c q4comp NOT NULL; BEGIN NULL; END $$;
-- 15: record var declared plain, gets domain value, field update
DO $$ DECLARE r record; BEGIN r := ROW(3,'rec')::q4dom; RAISE NOTICE 'r=% x=%', r, r.x; r.x := 8; RAISE NOTICE 'r2=%', r; END $$;
-- 16: whole-var read of never-assigned domain rec (empty reads as NULL)
DO $$ DECLARE d q4dom; BEGIN RAISE NOTICE 'null=% isnull=%', d, d IS NULL; END $$;
DO $$ DECLARE d q4dom; BEGIN RAISE NOTICE 'fx=%', d.x; END $$;
-- 17: unchecked row-of-nulls paths (INTO no rows / FOR zero rows) on a
-- domain whose check rejects null fields, reachable via default
DO $$ DECLARE d q4dnnf := ROW(1,'a'); BEGIN SELECT x,y INTO d FROM q4s WHERE false; RAISE NOTICE 'nr=% n=%', d, d IS NULL; END $$;
DO $$ DECLARE d q4dnnf := ROW(1,'a'); BEGIN FOR d IN SELECT x,y FROM q4s WHERE false LOOP END LOOP; RAISE NOTICE 'zf=%', d; END $$;
-- 18: EXECUTE INTO
DO $$ DECLARE d q4dom; BEGIN EXECUTE 'SELECT 4, ''dyn''' INTO d; RAISE NOTICE 'dyn=%', d; END $$;
DO $$ DECLARE d q4dom; BEGIN EXECUTE 'SELECT -4, ''dyn''' INTO d; RAISE NOTICE 'unreached'; END $$;
-- 19: cursor FETCH INTO
DO $$ DECLARE cur CURSOR FOR SELECT x,y FROM q4s ORDER BY x; d q4dom; BEGIN OPEN cur; FETCH cur INTO d; RAISE NOTICE 'cf=%', d; CLOSE cur; END $$;
-- 20: RETURNING INTO
DO $$ DECLARE d q4dom; BEGIN INSERT INTO q4s VALUES (11,'ins') RETURNING x,y INTO d; RAISE NOTICE 'ret=%', d; DELETE FROM q4s WHERE x=11; END $$;
DO $$ DECLARE d q4dom; BEGIN INSERT INTO q4s VALUES (-11,'ins') RETURNING x,y INTO d; RAISE NOTICE 'unreached'; END $$;
DELETE FROM q4s WHERE x = -11;
-- 21: OUT parameter of domain type + SETOF/RETURN NEXT
CREATE FUNCTION q4f_out(OUT d q4dom) LANGUAGE plpgsql AS $$ BEGIN d := ROW(12,'o'); END $$;
SELECT q4f_out(), (q4f_out()).y;
CREATE FUNCTION q4f_outbad(OUT d q4dom) LANGUAGE plpgsql AS $$ BEGIN d.x := -12; END $$;
SELECT q4f_outbad();
CREATE FUNCTION q4f_set() RETURNS SETOF q4dom LANGUAGE plpgsql AS $$ DECLARE d q4dom; BEGIN d := ROW(13,'s1'); RETURN NEXT d; d.y := 's2'; RETURN NEXT d; RETURN; END $$;
SELECT * FROM q4f_set();
-- 22: array of composite-domain elements in a scalar array var
DO $$ DECLARE a q4dom[]; BEGIN a[1] := ROW(1,'a'); a[2] := ROW(2,'b'); RAISE NOTICE 'arr=% e=%', a, a[2].y; END $$;
DO $$ DECLARE a q4dom[]; BEGIN a[1] := ROW(-1,'a'); RAISE NOTICE 'unreached'; END $$;
-- 23: domain over a table rowtype
CREATE DOMAIN q4dt AS q4s CHECK ((VALUE).x <> 0);
DO $$ DECLARE d q4dt; BEGIN SELECT x,y INTO d FROM q4s ORDER BY x LIMIT 1; RAISE NOTICE 'dt=%', d; END $$;
DO $$ DECLARE d q4dt; BEGIN d := ROW(0,'z'); END $$;
DROP DOMAIN q4dt;
-- 24: exception recovery keeps the old value (check aborts before commit)
DO $$ DECLARE d q4dom := ROW(1,'keep'); BEGIN BEGIN d := ROW(-2,'clobber'); EXCEPTION WHEN check_violation THEN NULL; END; RAISE NOTICE 'kept=%', d; BEGIN d.x := -3; EXCEPTION WHEN check_violation THEN NULL; END; RAISE NOTICE 'kept2=%', d; END $$;
DROP FUNCTION q4f_out; DROP FUNCTION q4f_outbad; DROP FUNCTION q4f_set;
DROP FUNCTION q4f_ret; DROP FUNCTION q4f_bad; DROP FUNCTION q4f_arg;
DROP TABLE q4t; DROP TABLE q4s;
DROP DOMAIN q4dd; DROP DOMAIN q4dom; DROP DOMAIN q4domnn; DROP DOMAIN q4dnnf;
DROP TYPE q4comp;
