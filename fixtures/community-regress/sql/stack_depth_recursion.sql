-- Issue malisper/pgrust#69: unbounded PL/pgSQL recursion must raise the
-- clean 54001 "stack depth limit exceeded" error (C: check_stack_depth at
-- the executor/utility entry points), never overflow the native thread
-- stack and take the whole server down.
-- terse: the HINT names the configured max_stack_depth and the CONTEXT
-- stack is depth-dependent (frame sizes differ from C); the primary
-- message is the stable surface.
\set VERBOSITY terse
CREATE FUNCTION issue69_f() RETURNS void AS $$ BEGIN PERFORM issue69_f(); END; $$ LANGUAGE plpgsql;
SELECT issue69_f();
SELECT 'server-alive' AS witness;
DROP FUNCTION issue69_f();
