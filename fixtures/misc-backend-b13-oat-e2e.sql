-- bugs/batch-13-misc-backend: InvokeNamespaceSearchHook sites C reaches from
-- namespace.c (LookupExplicitNamespace 3415, LookupNamespaceNoError 3365,
-- finalNamespacePath 4212) and regproc's inlined LookupExplicitNamespace,
-- witnessed through the test_oat_hooks recorder. The C oracle install
-- carries no test modules, so the expected file is captured from pgrust; the
-- NOTICE sequence for CREATE TABLE / SELECT matches
-- src/test/modules/test_oat_hooks/expected/alter_table.out.
SET debug_discard_caches = 0;
LOAD 'test_oat_hooks';
CREATE SCHEMA b13_oat_schema;
CREATE TABLE b13_oat_schema.b13_oat_tab (c1 int);
CREATE TEMP TABLE b13_oat_temp (c1 int);
SET test_oat_hooks.audit = on;
CREATE TABLE b13_oat_schema.b13_oat_tab2 (c1 int);
SELECT * FROM b13_oat_schema.b13_oat_tab;
SELECT 'b13_oat_schema.b13_oat_tab'::regclass;
SELECT * FROM pg_temp.b13_oat_temp;
SET search_path = b13_oat_schema, public;
SELECT * FROM b13_oat_tab;
SET test_oat_hooks.audit = off;
RESET search_path;
DROP SCHEMA b13_oat_schema CASCADE;
