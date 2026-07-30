# Ledger completeness audit — USER_FACING_FUNCTIONS.tsv vs the pg_proc catalog

Date: 2026-07-28. Trigger: two extraction gaps found by accident (brin_minmax 3383-3386 added post-hoc; get_bit/set_bit absent) — gate-blindness class, a missing row is a silently unproven function. This is the owed full re-derivation.

## Authoritative source chosen

`crates/backend/utils/fmgr/fmgr_core/src/canonical.rs` — a GENERATED table from `pg_proc.dat` (PostgreSQL 18.3), header documents the exact perl regeneration recipe. It contains every pg_proc row with `prolang = internal` and `prokind != 'a'` (aggregates excluded), as `(oid, prosrc, nargs, strict, retset)` tuples. That is the operational meaning of "user-facing": SQL-callable internal builtins with catalog oids. 3102 rows, zero duplicate oids.

Why not the per-crate `FmgrBuiltin` registration tables (the `b(oid, "name", nargs, fc_fn)` calls in `*/builtins.rs` and friends): they were also scanned (135 files, 3005 unique oids after excluding `fmgr_core/src/tests.rs`), but they are a *dispatch* inventory, not the catalog — functions dispatched outside fmgr (selectivity estimators, index AM handlers, window functions, RI triggers, BRIN support procs like brin_minmax) never appear in them. canonical.rs contains brin_minmax 3383-3386; the registration scan does not. The registration scan is used only to supply `source_file` for missing rows and to cross-check ledger source_file values.

## Method

Scripts in scratchpad (`extract_catalog.py`, `diff_ledger.py`), fully mechanical:

1. Parse canonical.rs rows `(oid, "prosrc", nargs, bool, bool)` — 3102 rows.
2. Scan every `crates/**/*.rs` mentioning `FmgrBuiltin`; extract entries built via locally-defined helper fns returning `FmgrBuiltin` (`b`, `srf`, `bn`, `nb`, `b_lax`, ...) plus `FmgrBuiltin { foid: N, name: "..." }` struct literals. Non-literal registrations flagged and resolved by hand: `F_SATISFIES_HASH_PARTITION` in `crates/backend/partitioning/partbounds/src/qual.rs` (oid 5028), `PGRUST_LANE_COVERAGE_FOID` (pgrust-internal, not catalog), `InvalidOid` sentinel in fmgr_core.
3. Parse the ledger (2972 lines = header + 2971 data rows) and diff both directions by oid; compare names and source_files.

## Sanity check on the two known gaps — PASSED

- brin_minmax 3383-3386: present in canonical.rs (lines 2100-2103) and present in the ledger → correctly NOT reported missing.
- get_bit/set_bit: the bytea variants are catalogued under their prosrc names `byteaGetBit` (723) / `byteaSetBit` (724) — both surface in the missing list below, along with siblings `byteaGetByte` (721) / `byteaSetByte` (722). (The bit-string variants `bitgetbit` 3032 / `bitsetbit` 3033 are already in the ledger.) Note: the SQL name is `get_bit` but canonical.rs and this audit key by prosrc, which is why a name-level grep for "get_bit" misses them — the original extraction likely failed the same way.

## Counts

| metric | value |
|---|---|
| catalog total (canonical.rs, PG 18.3 internal non-aggregate) | 3102 |
| ledger data rows | 2971 |
| duplicate oids in ledger | 0 |
| duplicate oids in catalog | 0 |
| **missing from ledger (catalog oid with no ledger row)** | **218** |
| — of which registered in an fmgr builtin table (implemented, dispatchable, silently unproven) | 39 |
| — of which no in-tree FmgrBuiltin registration (unimplemented, or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger/tablesample) | 179 |
| ledger oids not in canonical.rs | 87 (all explained, see below — no action) |
| name mismatches vs canonical | 90 (all proname-vs-prosrc convention, zero true drift) |
| source_file mismatches vs registration scan | 0 |

Arithmetic check: 2971 ledger = 2884 in-catalog + 87 explained extras; 3102 catalog − 2884 = 218 missing. ✓

## Missing rows — ready to append (oid, name, source_file, status, class, notes)

The 39 registered-but-unledgered rows are the urgent class (live, dispatchable, never triaged). The 179 unregistered rows carry `source_file = canonical.rs` because no implementing registration exists in-tree; they are still catalog functions and still owed triage (many are pgstatfuncs/selectivity/window/RI families).

```tsv
3	heap_tableam_handler	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
101	eqsel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
102	neqsel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
103	scalarltsel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
104	scalargtsel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
105	eqjoinsel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
106	neqjoinsel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
107	scalarltjoinsel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
108	scalargtjoinsel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
139	areasel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
140	areajoinsel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
275	pg_nextoid	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
330	bthandler	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
331	hashhandler	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
333	ginhandler	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
335	brinhandler	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
336	scalarlesel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
337	scalargesel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
386	scalarlejoinsel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
398	scalargejoinsel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
721	byteaGetByte	crates/backend/utils/adt/varlena/src/builtins.rs	untriaged	-	extraction-gap: registered builtin absent from initial ledger extraction (found in fmgr registration table); re-derived from fmgr_core canonical.rs (pg_proc.dat PG18.3) 2026-07-28
722	byteaSetByte	crates/backend/utils/adt/varlena/src/builtins.rs	untriaged	-	extraction-gap: registered builtin absent from initial ledger extraction (found in fmgr registration table); re-derived from fmgr_core canonical.rs (pg_proc.dat PG18.3) 2026-07-28
723	byteaGetBit	crates/backend/utils/adt/varlena/src/builtins.rs	untriaged	-	extraction-gap: registered builtin absent from initial ledger extraction (found in fmgr registration table); re-derived from fmgr_core canonical.rs (pg_proc.dat PG18.3) 2026-07-28
724	byteaSetBit	crates/backend/utils/adt/varlena/src/builtins.rs	untriaged	-	extraction-gap: registered builtin absent from initial ledger extraction (found in fmgr registration table); re-derived from fmgr_core canonical.rs (pg_proc.dat PG18.3) 2026-07-28
1264	PG_char_to_encoding	crates/backend/utils/adt/adt_misc/src/builtins.rs	untriaged	-	extraction-gap: registered builtin absent from initial ledger extraction (found in fmgr registration table); re-derived from fmgr_core canonical.rs (pg_proc.dat PG18.3) 2026-07-28
1300	positionsel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
1301	positionjoinsel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
1302	contsel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
1303	contjoinsel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
1349	oidvectortypes	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
1597	PG_encoding_to_char	crates/backend/utils/adt/adt_misc/src/builtins.rs	untriaged	-	extraction-gap: registered builtin absent from initial ledger extraction (found in fmgr registration table); re-derived from fmgr_core canonical.rs (pg_proc.dat PG18.3) 2026-07-28
1644	RI_FKey_check_ins	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
1645	RI_FKey_check_upd	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
1646	RI_FKey_cascade_del	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
1647	RI_FKey_cascade_upd	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
1648	RI_FKey_restrict_del	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
1649	RI_FKey_restrict_upd	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
1650	RI_FKey_setnull_del	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
1651	RI_FKey_setnull_upd	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
1652	RI_FKey_setdefault_del	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
1653	RI_FKey_setdefault_upd	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
1654	RI_FKey_noaction_del	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
1655	RI_FKey_noaction_upd	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
1713	length_in_encoding	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
1814	iclikesel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
1815	icnlikesel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
1816	iclikejoinsel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
1817	icnlikejoinsel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
1818	regexeqsel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
1819	likesel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
1820	icregexeqsel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
1821	regexnesel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
1822	nlikesel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
1823	icregexnesel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
1824	regexeqjoinsel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
1825	likejoinsel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
1826	icregexeqjoinsel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
1827	regexnejoinsel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
1828	nlikejoinsel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
1829	icregexnejoinsel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
2093	pg_conversion_is_visible	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
2094	pg_stat_get_backend_activity_start	crates/backend/utils/adt/pgstatfuncs/src/lib.rs	untriaged	-	extraction-gap: registered builtin absent from initial ledger extraction (found in fmgr registration table); re-derived from fmgr_core canonical.rs (pg_proc.dat PG18.3) 2026-07-28
2500	cstring_recv	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
2501	cstring_send	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
2503	anyarray_send	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
2743	ginarrayextract	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
2744	ginarrayconsistent	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
2770	pg_stat_get_checkpointer_num_requested	crates/backend/utils/adt/pgstatfuncs/src/lib.rs	untriaged	-	extraction-gap: registered builtin absent from initial ledger extraction (found in fmgr registration table); re-derived from fmgr_core canonical.rs (pg_proc.dat PG18.3) 2026-07-28
2771	pg_stat_get_checkpointer_buffers_written	crates/backend/utils/adt/pgstatfuncs/src/lib.rs	untriaged	-	extraction-gap: registered builtin absent from initial ledger extraction (found in fmgr registration table); re-derived from fmgr_core canonical.rs (pg_proc.dat PG18.3) 2026-07-28
2774	ginqueryarrayextract	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
2788	pg_stat_get_backend_wait_event_type	crates/backend/utils/adt/pgstatfuncs/src/lib.rs	untriaged	-	extraction-gap: registered builtin absent from initial ledger extraction (found in fmgr registration table); re-derived from fmgr_core canonical.rs (pg_proc.dat PG18.3) 2026-07-28
3069	pg_stat_get_db_conflict_startup_deadlock	crates/backend/utils/adt/pgstatfuncs/src/lib.rs	untriaged	-	extraction-gap: registered builtin absent from initial ledger extraction (found in fmgr registration table); re-derived from fmgr_core canonical.rs (pg_proc.dat PG18.3) 2026-07-28
3076	ginarrayextract_2args	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3077	gin_extract_tsvector_2args	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3086	pg_extension_config_dump	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3087	gin_extract_tsquery_5args	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3088	gin_tsquery_consistent_6args	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3092	pg_try_advisory_xact_lock_shared_int8	crates/backend/utils/adt/lockfuncs/src/lib.rs	untriaged	-	extraction-gap: registered builtin absent from initial ledger extraction (found in fmgr registration table); re-derived from fmgr_core canonical.rs (pg_proc.dat PG18.3) 2026-07-28
3096	pg_try_advisory_xact_lock_shared_int4	crates/backend/utils/adt/lockfuncs/src/lib.rs	untriaged	-	extraction-gap: registered builtin absent from initial ledger extraction (found in fmgr registration table); re-derived from fmgr_core canonical.rs (pg_proc.dat PG18.3) 2026-07-28
3100	window_row_number	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3101	window_rank	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3102	window_dense_rank	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3103	window_percent_rank	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3104	window_cume_dist	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3105	window_ntile	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3106	window_lag	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3107	window_lag_with_offset	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3108	window_lag_with_offset_and_default	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3109	window_lead	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3110	window_lead_with_offset	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3111	window_lead_with_offset_and_default	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3112	window_first_value	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3113	window_last_value	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3114	window_nth_value	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3129	btint2sortsupport	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3130	btint4sortsupport	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3131	btint8sortsupport	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3132	btfloat4sortsupport	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3133	btfloat8sortsupport	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3134	btoidsortsupport	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3135	btnamesortsupport	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3136	date_sortsupport	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3137	timestamp_sortsupport	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3169	rangesel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3283	numeric_sortsupport	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3300	uuid_sortsupport	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3313	tsm_bernoulli_handler	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3314	tsm_system_handler	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3331	bytea_sortsupport	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3332	bttext_pattern_sortsupport	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3359	macaddr_sortsupport	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3437	prefixsel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3438	prefixjoinsel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3445	pg_import_system_collations	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3448	pg_collation_actual_version	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3482	gin_extract_jsonb	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3483	gin_extract_jsonb_query	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3484	gin_consistent_jsonb	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3485	gin_extract_jsonb_path	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3486	gin_extract_jsonb_query_path	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3487	gin_consistent_jsonb_path	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3488	gin_triconsistent_jsonb	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3489	gin_triconsistent_jsonb_path	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3505	anyenum_out	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3560	networksel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3561	networkjoinsel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3572	int2int4_sum	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3584	binary_upgrade_set_next_array_pg_type_oid	crates/backend/utils/adt/pg_upgrade_support/src/lib.rs	untriaged	-	extraction-gap: registered builtin absent from initial ledger extraction (found in fmgr registration table); re-derived from fmgr_core canonical.rs (pg_proc.dat PG18.3) 2026-07-28
3586	binary_upgrade_set_next_heap_pg_class_oid	crates/backend/utils/adt/pg_upgrade_support/src/lib.rs	untriaged	-	extraction-gap: registered builtin absent from initial ledger extraction (found in fmgr registration table); re-derived from fmgr_core canonical.rs (pg_proc.dat PG18.3) 2026-07-28
3587	binary_upgrade_set_next_index_pg_class_oid	crates/backend/utils/adt/pg_upgrade_support/src/lib.rs	untriaged	-	extraction-gap: registered builtin absent from initial ledger extraction (found in fmgr registration table); re-derived from fmgr_core canonical.rs (pg_proc.dat PG18.3) 2026-07-28
3588	binary_upgrade_set_next_toast_pg_class_oid	crates/backend/utils/adt/pg_upgrade_support/src/lib.rs	untriaged	-	extraction-gap: registered builtin absent from initial ledger extraction (found in fmgr registration table); re-derived from fmgr_core canonical.rs (pg_proc.dat PG18.3) 2026-07-28
3656	gin_extract_tsvector	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3657	gin_extract_tsquery	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3658	gin_tsquery_consistent	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3686	tsmatchsel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3687	tsmatchjoinsel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3688	ts_typanalyze	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3697	gtsquery_picksplit	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3698	gtsquery_union	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3699	gtsquery_same	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3700	gtsquery_penalty	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3753	tsvector_update_trigger_bycolumn	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3756	pg_ts_parser_is_visible	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3757	pg_ts_dict_is_visible	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3758	pg_ts_config_is_visible	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3768	pg_ts_template_is_visible	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3776	pg_stat_reset_single_table_counters	crates/backend/utils/adt/pgstatfuncs/src/lib.rs	untriaged	-	extraction-gap: registered builtin absent from initial ledger extraction (found in fmgr registration table); re-derived from fmgr_core canonical.rs (pg_proc.dat PG18.3) 2026-07-28
3777	pg_stat_reset_single_function_counters	crates/backend/utils/adt/pgstatfuncs/src/lib.rs	untriaged	-	extraction-gap: registered builtin absent from initial ledger extraction (found in fmgr registration table); re-derived from fmgr_core canonical.rs (pg_proc.dat PG18.3) 2026-07-28
3783	pg_logical_slot_get_binary_changes	crates/backend/replication/logical/logicalfuncs/src/lib.rs	untriaged	-	extraction-gap: registered builtin absent from initial ledger extraction (found in fmgr registration table); re-derived from fmgr_core canonical.rs (pg_proc.dat PG18.3) 2026-07-28
3785	pg_logical_slot_peek_binary_changes	crates/backend/replication/logical/logicalfuncs/src/lib.rs	untriaged	-	extraction-gap: registered builtin absent from initial ledger extraction (found in fmgr registration table); re-derived from fmgr_core canonical.rs (pg_proc.dat PG18.3) 2026-07-28
3791	gin_extract_tsquery_oldsig	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3792	gin_tsquery_consistent_oldsig	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3815	pg_collation_is_visible	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3816	array_typanalyze	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3817	arraycontsel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3818	arraycontjoinsel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3819	pg_get_multixact_members	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3916	range_typanalyze	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3920	ginarraytriconsistent	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
3921	gin_tsquery_triconsistent	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
4105	brin_inclusion_opcinfo	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
4106	brin_inclusion_add_value	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
4107	brin_inclusion_consistent	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
4108	brin_inclusion_union	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
4227	anycompatiblemultirange_out	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
4242	multirange_typanalyze	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
4243	multirangesel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
4254	multirange_contained_by_multirange	crates/backend/utils/adt/multirangetypes/src/builtins.rs	untriaged	-	extraction-gap: registered builtin absent from initial ledger extraction (found in fmgr registration table); re-derived from fmgr_core canonical.rs (pg_proc.dat PG18.3) 2026-07-28
4269	multirange_overright_multirange	crates/backend/utils/adt/multirangetypes/src/builtins.rs	untriaged	-	extraction-gap: registered builtin absent from initial ledger extraction (found in fmgr registration table); re-derived from fmgr_core canonical.rs (pg_proc.dat PG18.3) 2026-07-28
4390	binary_upgrade_set_next_multirange_pg_type_oid	crates/backend/utils/adt/pg_upgrade_support/src/lib.rs	untriaged	-	extraction-gap: registered builtin absent from initial ledger extraction (found in fmgr registration table); re-derived from fmgr_core canonical.rs (pg_proc.dat PG18.3) 2026-07-28
4391	binary_upgrade_set_next_multirange_array_pg_type_oid	crates/backend/utils/adt/pg_upgrade_support/src/lib.rs	untriaged	-	extraction-gap: registered builtin absent from initial ledger extraction (found in fmgr registration table); re-derived from fmgr_core canonical.rs (pg_proc.dat PG18.3) 2026-07-28
4546	binary_upgrade_set_next_index_relfilenode	crates/backend/utils/adt/pg_upgrade_support/src/lib.rs	untriaged	-	extraction-gap: registered builtin absent from initial ledger extraction (found in fmgr registration table); re-derived from fmgr_core canonical.rs (pg_proc.dat PG18.3) 2026-07-28
4547	binary_upgrade_set_next_toast_relfilenode	crates/backend/utils/adt/pg_upgrade_support/src/lib.rs	untriaged	-	extraction-gap: registered builtin absent from initial ledger extraction (found in fmgr registration table); re-derived from fmgr_core canonical.rs (pg_proc.dat PG18.3) 2026-07-28
4548	binary_upgrade_set_next_pg_tablespace_oid	crates/backend/utils/adt/pg_upgrade_support/src/lib.rs	untriaged	-	extraction-gap: registered builtin absent from initial ledger extraction (found in fmgr registration table); re-derived from fmgr_core canonical.rs (pg_proc.dat PG18.3) 2026-07-28
4591	brin_bloom_opcinfo	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
4592	brin_bloom_add_value	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
4593	brin_bloom_consistent	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
4594	brin_bloom_union	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
4616	brin_minmax_multi_opcinfo	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
4617	brin_minmax_multi_add_value	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
4618	brin_minmax_multi_consistent	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
4619	brin_minmax_multi_union	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
5028	satisfies_hash_partition	crates/backend/partitioning/partbounds/src/qual.rs	untriaged	-	extraction-gap: registered builtin absent from initial ledger extraction (found in fmgr registration table); re-derived from fmgr_core canonical.rs (pg_proc.dat PG18.3) 2026-07-28
5033	network_sortsupport	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
5040	matchingsel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
5041	matchingjoinsel	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
5089	anycompatiblearray_out	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
5091	anycompatiblearray_send	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
5095	anycompatiblerange_out	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
6098	jsonb_subscript_handler	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
6118	pg_stat_get_subscription	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
6179	array_subscript_handler	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
6180	raw_array_subscript_handler	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
6218	pg_stat_get_xact_tuples_newpage_updated	crates/backend/utils/adt/pgstatfuncs/src/lib.rs	untriaged	-	extraction-gap: registered builtin absent from initial ledger extraction (found in fmgr registration table); re-derived from fmgr_core canonical.rs (pg_proc.dat PG18.3) 2026-07-28
6224	pg_get_wal_resource_managers	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
6249	pg_database_collation_actual_version	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
6312	binary_upgrade_logical_slot_has_caught_up	crates/backend/utils/adt/pg_upgrade_support/src/lib.rs	untriaged	-	extraction-gap: registered builtin absent from initial ledger extraction (found in fmgr registration table); re-derived from fmgr_core canonical.rs (pg_proc.dat PG18.3) 2026-07-28
6314	pg_stat_get_checkpointer_stat_reset_time	crates/backend/utils/adt/pgstatfuncs/src/lib.rs	untriaged	-	extraction-gap: registered builtin absent from initial ledger extraction (found in fmgr registration table); re-derived from fmgr_core canonical.rs (pg_proc.dat PG18.3) 2026-07-28
6321	pg_available_wal_summaries	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
6322	pg_wal_summary_contents	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
6327	pg_stat_get_checkpointer_restartpoints_timed	crates/backend/utils/adt/pgstatfuncs/src/lib.rs	untriaged	-	extraction-gap: registered builtin absent from initial ledger extraction (found in fmgr registration table); re-derived from fmgr_core canonical.rs (pg_proc.dat PG18.3) 2026-07-28
6328	pg_stat_get_checkpointer_restartpoints_requested	crates/backend/utils/adt/pgstatfuncs/src/lib.rs	untriaged	-	extraction-gap: registered builtin absent from initial ledger extraction (found in fmgr registration table); re-derived from fmgr_core canonical.rs (pg_proc.dat PG18.3) 2026-07-28
6329	pg_stat_get_checkpointer_restartpoints_performed	crates/backend/utils/adt/pgstatfuncs/src/lib.rs	untriaged	-	extraction-gap: registered builtin absent from initial ledger extraction (found in fmgr registration table); re-derived from fmgr_core canonical.rs (pg_proc.dat PG18.3) 2026-07-28
6353	pg_get_loaded_modules	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
6355	pg_stat_get_db_parallel_workers_to_launch	crates/backend/utils/adt/pgstatfuncs/src/lib.rs	untriaged	-	extraction-gap: registered builtin absent from initial ledger extraction (found in fmgr registration table); re-derived from fmgr_core canonical.rs (pg_proc.dat PG18.3) 2026-07-28
6356	pg_stat_get_db_parallel_workers_launched	crates/backend/utils/adt/pgstatfuncs/src/lib.rs	untriaged	-	extraction-gap: registered builtin absent from initial ledger extraction (found in fmgr registration table); re-derived from fmgr_core canonical.rs (pg_proc.dat PG18.3) 2026-07-28
6366	pg_stat_get_checkpointer_slru_written	crates/backend/utils/adt/pgstatfuncs/src/lib.rs	untriaged	-	extraction-gap: registered builtin absent from initial ledger extraction (found in fmgr registration table); re-derived from fmgr_core canonical.rs (pg_proc.dat PG18.3) 2026-07-28
6377	pg_stat_get_checkpointer_num_performed	crates/backend/utils/adt/pgstatfuncs/src/lib.rs	untriaged	-	extraction-gap: registered builtin absent from initial ledger extraction (found in fmgr registration table); re-derived from fmgr_core canonical.rs (pg_proc.dat PG18.3) 2026-07-28
6380	array_subscript_handler_support	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
6402	btint2skipsupport	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
6403	btint4skipsupport	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
6404	btint8skipsupport	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
6405	btoidskipsupport	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
6406	btcharskipsupport	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
6407	date_skipsupport	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
6408	btboolskipsupport	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
6409	timestamp_skipsupport	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
6410	uuid_skipsupport	crates/backend/utils/fmgr/fmgr_core/src/canonical.rs	untriaged	-	extraction-gap: in pg_proc catalog (canonical.rs) but no in-tree FmgrBuiltin registration found (unimplemented or non-fmgr dispatch: selectivity/AM-handler/window/RI-trigger); re-derived 2026-07-28
```

## Ledger oids NOT in canonical.rs (87) — explained, keep in ledger

- 1215 `obj_description`, 1216 `col_description`, 1993 `shobj_description` (`crates/backend/utils/adt/adt_misc/src/builtins.rs`): SQL-language functions in PG (`prolang = sql`, so excluded by the canonical.rs generator) that pgrust implements as internal builtins. Legitimately SQL-callable; keep.
- 4302-4387 (84 rows, `crates/backend/utils/mb/conv/src/lib.rs`): encoding-conversion functions, `prolang = c` in pg_proc.dat (loaded from libdir in PG), registered as native builtins in pgrust. Keep.

Consequence for future audits: the full "user-facing" universe = canonical.rs ∪ these 87; neither source alone is complete.

## Name mismatches (90) — all benign convention, no drift

Every one of the 90 rows where ledger name ≠ canonical prosrc has a ledger name exactly equal to the SQL proname used by the in-tree registration table (verified mechanically): `strpos`/`textpos` (868), `like`/`textlike` (1569), `abs`/`numeric_abs` (1705), the range/multirange constructor families (`int4range`/`range_constructor2` etc.), `gcd`/`numeric_gcd` (5048), and so on. Zero rows match neither source. No corrections needed; the ledger convention is proname-where-it-differs. Full list:

```tsv
oid	ledger-name(proname)	canonical-name(prosrc)
868	strpos	textpos
1293	unnest	multirange_unnest
1376	factorial	numeric_fac
1569	like	textlike
1570	notlike	textnlike
1571	like	namelike
1572	notlike	namenlike
1631	bpcharlike	textlike
1632	bpcharnlike	textnlike
1656	bpcharicregexeq	texticregexeq
1657	bpcharicregexne	texticregexne
1658	bpcharregexeq	textregexeq
1659	bpcharregexne	textregexne
1660	bpchariclike	texticlike
1661	bpcharicnlike	texticnlike
1705	abs	numeric_abs
1706	sign	numeric_sign
1711	ceil	numeric_ceil
1712	floor	numeric_floor
1728	mod	numeric_mod
1730	sqrt	numeric_sqrt
1732	exp	numeric_exp
1734	ln	numeric_ln
1736	log	numeric_log
1738	pow	numeric_power
2007	like	bytealike
2008	notlike	byteanlike
2009	like_escape	like_escape_bytea
2167	ceiling	numeric_ceil
2169	power	numeric_power
2170	width_bucket	width_bucket_numeric
2319	pg_encoding_max_length	pg_encoding_max_length_sql
3259	generate_series	generate_series_step_numeric
3260	generate_series	generate_series_numeric
3303	jsonb_delete	jsonb_delete_idx
3343	jsonb_delete	jsonb_delete_array
3840	int4range	range_constructor2
3841	int4range	range_constructor3
3844	numrange	range_constructor2
3845	numrange	range_constructor3
3848	lower	range_lower
3849	upper	range_upper
3850	isempty	range_empty
3851	lower_inc	range_lower_inc
3852	upper_inc	range_upper_inc
3853	lower_inf	range_lower_inf
3854	upper_inf	range_upper_inf
3933	tsrange	range_constructor2
3934	tsrange	range_constructor3
3937	tstzrange	range_constructor2
3938	tstzrange	range_constructor3
3941	daterange	range_constructor2
3942	daterange	range_constructor3
3945	int8range	range_constructor2
3946	int8range	range_constructor3
4220	pg_copy_physical_replication_slot	pg_copy_physical_replication_slot_a
4221	pg_copy_physical_replication_slot	pg_copy_physical_replication_slot_b
4222	pg_copy_logical_replication_slot	pg_copy_logical_replication_slot_a
4223	pg_copy_logical_replication_slot	pg_copy_logical_replication_slot_b
4224	pg_copy_logical_replication_slot	pg_copy_logical_replication_slot_c
4228	range_merge	range_merge_from_multirange
4235	lower	multirange_lower
4236	upper	multirange_upper
4237	isempty	multirange_empty
4238	lower_inc	multirange_lower_inc
4239	upper_inc	multirange_upper_inc
4240	lower_inf	multirange_lower_inf
4241	upper_inf	multirange_upper_inf
4280	int4multirange	multirange_constructor0
4281	int4multirange	multirange_constructor1
4282	int4multirange	multirange_constructor2
4283	nummultirange	multirange_constructor0
4284	nummultirange	multirange_constructor1
4285	nummultirange	multirange_constructor2
4286	tsmultirange	multirange_constructor0
4287	tsmultirange	multirange_constructor1
4288	tsmultirange	multirange_constructor2
4289	tstzmultirange	multirange_constructor0
4290	tstzmultirange	multirange_constructor1
4291	tstzmultirange	multirange_constructor2
4292	datemultirange	multirange_constructor0
4293	datemultirange	multirange_constructor1
4294	datemultirange	multirange_constructor2
4295	int8multirange	multirange_constructor0
4296	int8multirange	multirange_constructor1
4297	int8multirange	multirange_constructor2
4298	multirange	multirange_constructor1
5048	gcd	numeric_gcd
5049	lcm	numeric_lcm
6226	multirange_agg_finalfn	range_agg_finalfn
```

## Duplicates

- Ledger: none (2971 rows, 2971 unique oids).
- Registration tables: oid 861 `current_database` is registered in BOTH `crates/backend/commands/dbcommands/src/builtins.rs` and `crates/backend/utils/adt/adt_misc/src/builtins.rs` — not a ledger defect, but a double-registration worth a look by the fmgr owner. (Oids 65/177 also appear in `fmgr_core/src/tests.rs`, test-only, excluded.)

## Scripts / intermediates

The extraction/diff scripts (`extract_catalog.py`, `diff_ledger.py`) and their
intermediates were session-local working files; the method above is complete
enough to re-derive them mechanically.
