-- Two-binary differential corpus for the 2026-09-13 bug-inventory batch
-- batch-150-backend-regex (C 18.6 oracle vs pgrust). Each leg is one row
-- of the batch (backend/regex and its regexp.c caller).
\set VERBOSITY verbose
CREATE DATABASE b150e2e TEMPLATE template0 ENCODING 'UTF8';
\c b150e2e
\set VERBOSITY verbose
-- pgrust's RE2 tier (regex_engine=auto) keeps its own dispatch cache; the
-- ported regexp.c cache is the Spencer path, so pin it (no-op on C)
SELECT count(*) >= 0 AS spencer FROM (SELECT set_config('regex_engine', 'spencer', false) WHERE current_setting('pgrust.regex_re2_linked', true) IS NOT NULL) s;
-- fp-regex-regc_nfa-p2#1: every cached regexp owns a RegexpMemoryContext under
-- RegexpCacheMemoryContext, identified by its pattern (regexp.c:204,231)
SELECT 'abc' ~ 'b150_a(b)c' AS m1, 'xyz' ~* '^b150_x.*z$' AS m2, 'abc' ~ 'b150_a(b)c' AS m3;
SELECT name, ident, level FROM pg_backend_memory_contexts WHERE name IN ('RegexpCacheMemoryContext', 'RegexpMemoryContext') ORDER BY name, ident;
SELECT p.name AS parent, count(*) FROM pg_backend_memory_contexts c JOIN pg_backend_memory_contexts p ON p.level = c.level - 1 AND p.path = c.path[1:c.level - 1] WHERE c.name = 'RegexpMemoryContext' GROUP BY p.name;
-- the LRU eviction deletes the evicted pattern's context (regexp.c:244)
SELECT count(*) FROM generate_series(1, 40) g WHERE 'q' ~ ('b150_evict_' || g);
SELECT count(*) FROM pg_backend_memory_contexts WHERE name = 'RegexpMemoryContext';
SELECT count(*) FROM pg_backend_memory_contexts WHERE name = 'RegexpMemoryContext' AND ident IN ('b150_a(b)c', '^b150_x.*z$', 'b150_evict_1', 'b150_evict_8', 'b150_evict_9', 'b150_evict_40');
