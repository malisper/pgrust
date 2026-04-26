Goal:
Explain what PostgreSQL caches or avoids to make trigger-heavy DML faster than
the current pgrust triggers regression repro.

Key decisions:
- PG stores index OID lists in relcache via RelationGetIndexList and invalidates
  them with relcache invalidation.
- PG stores relation triggers in relcache TriggerDesc, including per-event hint
  flags, then copies that into ResultRelInfo for execution.
- PG caches per-trigger fmgr lookup in ResultRelInfo::ri_TrigFunctions.
- PG caches compiled PL/pgSQL functions through cached_function_compile and
  FmgrInfo::fn_extra, validating cached entries with pg_proc tuple identity.
- PG RI triggers use the same trigger machinery and additionally cache
  RI_ConstraintInfo and SPI plans in backend-local hash tables.

Files touched:
- .codex/task-notes/triggers-profile-pg-reference.md

Tests run:
- Source inspection only.

Remaining:
- Add pgrust caches for compiled PL/pgSQL trigger functions and trigger metadata.
- Consider caching index lists in relcache/visible catalog instead of rebuilding
  BoundIndexRelation metadata on every repeated bind.
- Replace FK trigger enable checks that scan every relation with a keyed lookup
  or relation-local trigger descriptor.
