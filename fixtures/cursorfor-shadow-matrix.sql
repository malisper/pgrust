-- P2-CURSORFOR-SHADOW spec matrix: FOR-loop target name colliding with a
-- declared variable, across {cursor FOR, FOR-over-query, FOR-over-EXECUTE}
-- x {auto-declared, declared record, declared rowtype, declared+assigned,
--    declared in outer block} x {body reads var, EXIT-only, read after loop,
--    zero-row}.  C 18.4 behavior is the spec.
\set VERBOSITY verbose
CREATE TABLE mt(v int);
INSERT INTO mt VALUES (1),(2),(3);
CREATE TABLE empty_t(v int);

-- ============ cursor FOR ============
-- 1. auto-declared loop var (canonical)
CREATE FUNCTION c_auto() RETURNS int LANGUAGE plpgsql AS $f$
DECLARE c CURSOR FOR SELECT v FROM mt ORDER BY v; s int := 0; BEGIN
  FOR r IN c LOOP s := s + r.v; END LOOP;
  RETURN s; END $f$;
SELECT c_auto();

-- 2. explicitly declared record, body reads
CREATE FUNCTION c_decl_rec() RETURNS int LANGUAGE plpgsql AS $f$
DECLARE c CURSOR FOR SELECT v FROM mt ORDER BY v; r record; s int := 0; BEGIN
  FOR r IN c LOOP s := s + r.v; END LOOP;
  RETURN s; END $f$;
SELECT c_decl_rec();

-- 3. explicitly declared record, EXIT-only body
CREATE FUNCTION c_decl_rec_exit() RETURNS int LANGUAGE plpgsql AS $f$
DECLARE c CURSOR FOR SELECT v FROM mt ORDER BY v; r record; s int := 0; BEGIN
  FOR r IN c LOOP EXIT; END LOOP;
  RETURN s; END $f$;
SELECT c_decl_rec_exit();

-- 4. declared rowtype of the cursor's shape
CREATE FUNCTION c_decl_rowtype() RETURNS int LANGUAGE plpgsql AS $f$
DECLARE c CURSOR FOR SELECT v FROM mt ORDER BY v; r mt%ROWTYPE; s int := 0; BEGIN
  FOR r IN c LOOP s := s + r.v; END LOOP;
  RETURN s; END $f$;
SELECT c_decl_rowtype();

-- 5. declared + previously assigned; also read AFTER the loop
CREATE FUNCTION c_decl_assigned() RETURNS text LANGUAGE plpgsql AS $f$
DECLARE c CURSOR FOR SELECT v FROM mt ORDER BY v; r record; s int := 0; BEGIN
  SELECT 99 AS v INTO r;
  FOR r IN c LOOP s := s + r.v; END LOOP;
  RETURN s::text || '/' || r.v::text; END $f$;
SELECT c_decl_assigned();

-- 6. declared in OUTER block (nested)
CREATE FUNCTION c_outer_block() RETURNS text LANGUAGE plpgsql AS $f$
DECLARE r record; s int := 0; BEGIN
  SELECT 77 AS v INTO r;
  DECLARE c CURSOR FOR SELECT v FROM mt ORDER BY v; BEGIN
    FOR r IN c LOOP s := s + r.v; END LOOP;
  END;
  RETURN s::text || '/' || r.v::text; END $f$;
SELECT c_outer_block();

-- 7. zero-row cursor, declared+assigned record, read after loop
CREATE FUNCTION c_zero_row() RETURNS text LANGUAGE plpgsql AS $f$
DECLARE c CURSOR FOR SELECT v FROM empty_t; r record; BEGIN
  SELECT 42 AS v INTO r;
  FOR r IN c LOOP NULL; END LOOP;
  RETURN COALESCE(r.v::text, 'null'); END $f$;
SELECT c_zero_row();

-- 8. zero-row cursor, declared but never assigned, read after loop
CREATE FUNCTION c_zero_row_unassigned() RETURNS text LANGUAGE plpgsql AS $f$
DECLARE c CURSOR FOR SELECT v FROM empty_t; r record; BEGIN
  FOR r IN c LOOP NULL; END LOOP;
  RETURN COALESCE(r.v::text, 'null'); END $f$;
SELECT c_zero_row_unassigned();

-- ============ FOR-over-query ============
-- 9. declared record, body reads (C: uses the DECLARED var directly)
CREATE FUNCTION q_decl_rec() RETURNS int LANGUAGE plpgsql AS $f$
DECLARE r record; s int := 0; BEGIN
  FOR r IN SELECT v FROM mt ORDER BY v LOOP s := s + r.v; END LOOP;
  RETURN s; END $f$;
SELECT q_decl_rec();

-- 10. declared record, EXIT-only
CREATE FUNCTION q_decl_rec_exit() RETURNS int LANGUAGE plpgsql AS $f$
DECLARE r record; s int := 0; BEGIN
  FOR r IN SELECT v FROM mt ORDER BY v LOOP EXIT; END LOOP;
  RETURN s; END $f$;
SELECT q_decl_rec_exit();

-- 11. declared record, read AFTER loop (C: retains last row)
CREATE FUNCTION q_after() RETURNS int LANGUAGE plpgsql AS $f$
DECLARE r record; BEGIN
  FOR r IN SELECT v FROM mt ORDER BY v LOOP NULL; END LOOP;
  RETURN r.v; END $f$;
SELECT q_after();

-- 12. declared rowtype, read after loop
CREATE FUNCTION q_rowtype_after() RETURNS int LANGUAGE plpgsql AS $f$
DECLARE r mt%ROWTYPE; BEGIN
  FOR r IN SELECT v FROM mt ORDER BY v LOOP NULL; END LOOP;
  RETURN r.v; END $f$;
SELECT q_rowtype_after();

-- 13. zero-row query, previously assigned, read after (C: fields go NULL)
CREATE FUNCTION q_zero_row() RETURNS text LANGUAGE plpgsql AS $f$
DECLARE r record; BEGIN
  SELECT 42 AS v INTO r;
  FOR r IN SELECT v FROM empty_t LOOP NULL; END LOOP;
  RETURN COALESCE(r.v::text, 'null'); END $f$;
SELECT q_zero_row();

-- 14. outer-block declared record, read after loop
CREATE FUNCTION q_outer_block() RETURNS int LANGUAGE plpgsql AS $f$
DECLARE r record; BEGIN
  BEGIN
    FOR r IN SELECT v FROM mt ORDER BY v LOOP NULL; END LOOP;
  END;
  RETURN r.v; END $f$;
SELECT q_outer_block();

-- ============ FOR-over-EXECUTE ============
-- 15. declared record, body reads
CREATE FUNCTION e_decl_rec() RETURNS int LANGUAGE plpgsql AS $f$
DECLARE r record; s int := 0; BEGIN
  FOR r IN EXECUTE 'SELECT v FROM mt ORDER BY v' LOOP s := s + r.v; END LOOP;
  RETURN s; END $f$;
SELECT e_decl_rec();

-- 16. declared record, EXIT-only
CREATE FUNCTION e_decl_rec_exit() RETURNS int LANGUAGE plpgsql AS $f$
DECLARE r record; s int := 0; BEGIN
  FOR r IN EXECUTE 'SELECT v FROM mt ORDER BY v' LOOP EXIT; END LOOP;
  RETURN s; END $f$;
SELECT e_decl_rec_exit();

-- 17. declared record, read AFTER loop
CREATE FUNCTION e_after() RETURNS int LANGUAGE plpgsql AS $f$
DECLARE r record; BEGIN
  FOR r IN EXECUTE 'SELECT v FROM mt ORDER BY v' LOOP NULL; END LOOP;
  RETURN r.v; END $f$;
SELECT e_after();

-- 18. zero-row EXECUTE, previously assigned, read after
CREATE FUNCTION e_zero_row() RETURNS text LANGUAGE plpgsql AS $f$
DECLARE r record; BEGIN
  SELECT 42 AS v INTO r;
  FOR r IN EXECUTE 'SELECT v FROM empty_t' LOOP NULL; END LOOP;
  RETURN COALESCE(r.v::text, 'null'); END $f$;
SELECT e_zero_row();

-- ============ cursor FOR: post-loop / shadow visibility ============
-- 19. cursor FOR, declared+assigned, var value AFTER loop (C: loop var is a
--     PRIVATE shadow; outer r keeps its pre-loop value)
CREATE FUNCTION c_shadow_after() RETURNS text LANGUAGE plpgsql AS $f$
DECLARE c CURSOR FOR SELECT v FROM mt ORDER BY v; r record; BEGIN
  SELECT 88 AS v INTO r;
  FOR r IN c LOOP NULL; END LOOP;
  RETURN r.v::text; END $f$;
SELECT c_shadow_after();

-- 20. cursor FOR over scalar var name (C: error - must be record/row? cursor
--     loop always builds its own record, so a declared int name is shadowed)
CREATE FUNCTION c_scalar_name() RETURNS int LANGUAGE plpgsql AS $f$
DECLARE c CURSOR FOR SELECT v FROM mt ORDER BY v; r int := 5; s int := 0; BEGIN
  FOR r IN c LOOP s := s + r.v; END LOOP;
  RETURN s + r; END $f$;
SELECT c_scalar_name();

-- 21. integer FOR shadowing declared int (control: both should shadow)
CREATE FUNCTION i_shadow() RETURNS int LANGUAGE plpgsql AS $f$
DECLARE i int := 100; s int := 0; BEGIN
  FOR i IN 1..3 LOOP s := s + i; END LOOP;
  RETURN s + i; END $f$;
SELECT i_shadow();

-- ============ general shadowed-unassigned-record class ============
-- 22. nested blocks both declaring r record; inner assigned, outer never
CREATE FUNCTION n_nested_shadow() RETURNS int LANGUAGE plpgsql AS $f$
DECLARE r record; BEGIN
  DECLARE r record; BEGIN
    SELECT 5 AS v INTO r;
    RETURN r.v;
  END;
END $f$;
SELECT n_nested_shadow();

-- 23. cursor FOR shadow, EXIT WHEN reads the loop var
CREATE FUNCTION c_exit_when() RETURNS int LANGUAGE plpgsql AS $f$
DECLARE c CURSOR FOR SELECT v FROM mt ORDER BY v; r record; s int := 0; BEGIN
  FOR r IN c LOOP s := s + r.v; EXIT WHEN r.v >= 2; END LOOP;
  RETURN s; END $f$;
SELECT c_exit_when();

-- 24. field present only on the shadowed OUTER rec: C resolves r.w to the
--     nearest rec (the loop's private record) -> no-such-field error
CREATE FUNCTION c_field_leak() RETURNS int LANGUAGE plpgsql AS $f$
DECLARE c CURSOR FOR SELECT v FROM mt ORDER BY v; r record; s int := 0; BEGIN
  SELECT 1 AS v, 10 AS w INTO r;
  FOR r IN c LOOP s := s + r.w; END LOOP;
  RETURN s; END $f$;
SELECT c_field_leak();

-- 25. label-qualified reach-through to the shadowed outer rec
CREATE FUNCTION c_label_qualified() RETURNS text LANGUAGE plpgsql AS $f$
<<outer>>
DECLARE r record; s int := 0; BEGIN
  SELECT 100 AS v INTO r;
  DECLARE c CURSOR FOR SELECT v FROM mt ORDER BY v; BEGIN
    FOR r IN c LOOP s := s + r.v + outer.r.v; END LOOP;
  END;
  RETURN s::text || '/' || r.v::text; END $f$;
SELECT c_label_qualified();

-- 26. label-qualified reference to a shadowed UNASSIGNED outer rec (C: 55000)
CREATE FUNCTION c_label_unassigned() RETURNS int LANGUAGE plpgsql AS $f$
<<outer>>
DECLARE r record; s int := 0; BEGIN
  DECLARE c CURSOR FOR SELECT v FROM mt ORDER BY v; BEGIN
    FOR r IN c LOOP s := s + outer.r.v; END LOOP;
  END;
  RETURN s; END $f$;
SELECT c_label_unassigned();
