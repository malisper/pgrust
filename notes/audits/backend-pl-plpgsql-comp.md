# Audit: `backend-pl-plpgsql-comp` (`src/pl/plpgsql/src/pl_comp.c`)

Independent re-derivation from C ground truth (PG 18.3), the src-idiomatic guide,
and the headers. **Verdict: FAIL** — 1 MISSING, 2 PARTIAL, 1 DIVERGES.

C source: `postgres-18.3/src/pl/plpgsql/src/pl_comp.c` (2327 LOC, 33 functions).
Out of scope (PG18 moved to `funccache.c`, not in `pl_comp.c`):
`compute_function_hashkey`, `delete_function`, `plpgsql_HashTable*`,
`cached_function_compile`, `cfunc_resolve_polymorphic_argtypes` (callees).

## Per-function table

| # | C function | C loc | Port loc | Verdict | Notes |
|---|-----------|-------|----------|---------|-------|
| 1 | `plpgsql_compile` | 106 | — | **MISSING** | funccache driver (save `fn_extra`, dispatch `cached_function_compile`) absent; no entry calls the cache path. src-idiomatic keeps it (`comp.rs:208`). See F1. |
| 2 | `plpgsql_compile_callback` | 167 | `plpgsql_compile_from_source` lib.rs:1562 | **PARTIAL** | body ported but the entire `forValidator` axis dropped: no validator syntax-check, `extra_warnings/errors` hardcoded 0 (C 249-250), validator polymorphic-return substitution gone (C 401-414), polymorphic-arg resolution dropped (C 292), **`$0` polymorphic-return install missing** (C 469-478). See F2. |
| 3 | `plpgsql_compile_inline` | 739 | `plpgsql_compile_inline` lib.rs:1491 | MATCH | parallel structure faithful; `check_syntax=check_function_bodies` is set false (acceptable: GUC owner) but C uses the GUC — minor, noted F2b. |
| 4 | `plpgsql_compile_error_callback` | 885 | — | SEAMED | error-context callback; `function_parse_error_transpose`/`errcontext` are infra callees. Not modeled (cold diagnostics). Acceptable. |
| 5 | `add_parameter_name` | 915 | lib.rs:166 | MATCH | dup check via `ns_lookup(top,true,...)`, then `ns_additem`. |
| 6 | `add_dummy_return` | 940 | lib.rs:178 | MATCH | exception/label wrap + trailing-RETURN check; stmtid via `++nstatements`. |
| 7 | `plpgsql_parser_setup` | 985 | — | SEAMED | installs parser hooks on `ParseState` (exec-time); ParseState owner not in compile path. |
| 8 | `plpgsql_pre_column_ref` | 998 | — | SEAMED | exec-time parse hook (needs ParseState). |
| 9 | `plpgsql_post_column_ref` | 1012 | — | SEAMED | exec-time parse hook. |
| 10 | `plpgsql_param_ref` | 1056 | — | SEAMED | exec-time parse hook. |
| 11 | `resolve_column_ref` | 1083 | — | SEAMED | exec-time; reads `expr->func->cur_estate`. |
| 12 | `make_datum_param` | 1241 | — | SEAMED | exec-time; `plpgsql_exec_get_datum_type_info` callee. |
| 13 | `plpgsql_parse_word` | 1294 | lib.rs:231 | MATCH | lookup gated on `IDENTIFIER_LOOKUP_NORMAL`; VAR/REC → datum, else word; `quoted` via `yytxt[0]=='"'`. |
| 14 | `plpgsql_parse_dblword` | 1349 | lib.rs:262 | MATCH | `!= DECLARE` gate; VAR→datum, REC nnames==1→recfield else var; cword fallback. |
| 15 | `plpgsql_parse_tripword` | 1430 | lib.rs:305 | MATCH | REC-only; nnames==1 → recfield(word2)+2 idents, else recfield(word3)+3 idents. |
| 16 | `plpgsql_parse_wordtype` | 1514 | lib.rs:355 | MATCH | VAR/REC datatype, else `ereport(UNDEFINED_OBJECT 42704)`→panic. |
| 17 | `plpgsql_parse_cwordtype` | 1555 | lib.rs:373 | SEAMED | 2-name var/rec branch ported; relation-column branch panics via `relname_get_relid` (namespace/syscache-attname owner unwired). Acceptable. |
| 18 | `plpgsql_parse_wordrowtype` | 1667 | lib.rs:401 | SEAMED | `RelnameGetRelid` panics (namespace owner). Logic shape faithful. |
| 19 | `plpgsql_parse_cwordrowtype` | 1704 | lib.rs:414 | SEAMED | `RangeVarGetRelid` panics. **minor**: passes only `idents.last()` to the relid lookup vs C `makeRangeVarFromNameList(idents)` — moot while it panics, but wrong if wired. |
| 20 | `plpgsql_build_variable` | 1748 | lib.rs:431 | MATCH | SCALAR→var, REC→`build_record`, PSEUDO→`ereport(42P16)`→panic. |
| 21 | `plpgsql_build_record` | 1811 | lib.rs:493 | MATCH | firstfield=-1, erh=None, adddatum, optional ns_additem. |
| 22 | `build_row_from_vars` | 1838 | lib.rs:525 | **PARTIAL** | `rowtupdesc` left `None` (the genuine composite TupleDesc + `TupleDescInitEntry`/`TupleDescInitEntryCollation` per member, C 1847-1893, are dropped). tupdesc owner not in compile path → SEAMED-ish, BUT the per-member typoid/typmod/typcoll read is in-crate logic that is also skipped. See F3. |
| 23 | `plpgsql_build_recfield` | 1905 | lib.rs:561 | MATCH | reuse existing field, else new + link into firstfield chain. |
| 24 | `plpgsql_build_datatype` | 1952 | `plpgsql_build_datatype_internal` lib.rs:590 | MATCH | syscache TYPEOID → `build_datatype`. |
| 25 | `build_datatype` | 1974 | lib.rs:601 | **DIVERGES** | typtype→ttype map, collation override, REC-tupdesc-id all faithful; but `IsTrueArrayType` is approximated as `typelem valid && typlen==-1` instead of `typelem valid && typsubscript==F_ARRAY_SUBSCRIPT_HANDLER` (seam.rs:80). See F4. |
| 26 | `plpgsql_build_datatype_arrayof` | 2086 | lib.rs:678 | MATCH | typisarray short-circuit, `get_array_type`, inherit typmod/collation. |
| 27 | `plpgsql_recognize_err_condition` | 2117 | lib.rs:698 | MATCH | sqlstate 5-char `[0-9A-Z]` check + `MAKE_SQLSTATE`; else table; else `ereport(42704)`. `is_ascii_digit()||is_ascii_uppercase()` == C `strspn("0-9A-Z")`. |
| 28 | `plpgsql_parse_err_condition` | 2153 | lib.rs:716 | MATCH | "others"→PLPGSQL_OTHERS; else collect all label matches as reversed list; empty→error. |
| 29 | `plpgsql_start_datums` | 2200 | lib.rs:746 | MATCH | alloc=128, nDatums=0, datums_last=0. |
| 30 | `plpgsql_adddatum` | 2217 | lib.rs:758 | MATCH | set dno, push, bump; alloc-doubling cosmetic (Vec auto-grows). |
| 31 | `plpgsql_finish_datums` | 2234 | lib.rs:774 | MATCH | ndatums, datums copy, copiable_size = Σ MAXALIGN(sizeof var/rec). |
| 32 | `plpgsql_add_initdatums` | 2278 | lib.rs:801 | MATCH | VAR/REC dnos since datums_last; C switch-fallthrough is a no-op, faithfully a match-push. |
| 33 | `plpgsql_start_datums`/accessors etc. | — | — | — | thread-local globals modeled correctly (non-reentrant compiler), per-backend → `thread_local!`. |

Extra in-crate builders backing pl_gram.y actions (cursor var, cursor arg row,
record/int loop var, exc special var, into row, make_scalar_list1, check_shadowvar):
shapes faithful to the grammar actions; share `build_row_from_vars`'s F3 gap for
the ROW-building ones. `quote_identifier` (lib.rs:1961): see F5.

## Findings (FAIL)

### F1 — `plpgsql_compile` MISSING
- **C** 106-136: saves the compiled function into `fcinfo->flinfo->fn_extra`,
  dispatching through `cached_function_compile(..., plpgsql_compile_callback,
  plpgsql_delete_callback, ...)`.
- **Port**: no counterpart. `plpgsql_compile_from_source(facts)` takes
  pre-extracted `pg_proc` facts and never touches the funccache cache path.
- src-idiomatic guide keeps `plpgsql_compile` (`comp.rs:208`) delegating to
  `seam::cached_function_compile`. The cache dispatch is a funccache callee
  (legitimately SEAMED), but the function entry itself — and the `fn_extra`
  save — is absent. **At minimum the entry must exist and SEAM to funccache;
  it cannot be dropped.**

### F2 — `plpgsql_compile_callback` PARTIAL: `forValidator` axis + `$0` install dropped
`plpgsql_compile_from_source` (lib.rs:1562) has **no `for_validator` parameter**.
Concretely missing in-crate logic (not callee gates):
1. `function->extra_warnings/errors = forValidator ? plpgsql_extra_* : 0`
   (C 249-250) → hardcoded `0` (lib.rs:1577-1578).
2. `plpgsql_check_syntax = forValidator` (C 223) → hardcoded `set_check_syntax(false)`.
3. Validator-mode polymorphic-**return** substitution (C 401-414:
   ANYARRAY/ANYCOMPATIBLEARRAY→INT4ARRAY, ANYRANGE/…→INT4RANGE,
   ANYMULTIRANGE→INT4MULTIRANGE, else→INT4) — **entirely absent**;
   `compile_scalar_function_setup` (lib.rs:1736) panics on *any* polymorphic
   return unconditionally.
4. **`$0` install** for polymorphic returns with `num_out_args==0`
   (C 469-478: `plpgsql_build_variable("$0", 0, build_datatype(typeTup,…), true)`)
   — absent. This is pure in-crate logic, not a callee gate.
5. Polymorphic-**input**-arg resolution (`cfunc_resolve_polymorphic_argtypes`,
   C 292) dropped. This one *is* a funccache callee operating on
   `fcinfo->flinfo->fn_expr`, which `ProcCompileFacts` does not carry — so the
   SEAM is acceptable, but the **validator fallback (assume INT4)** that the
   same call performs is also gone.

Note: the non-trigger-return-type pseudotype/byval/typlen logic (C 430-462)
and trigger/event-trigger arms (C 483-653) are faithfully ported.

### F3 — `build_row_from_vars` PARTIAL: rowtupdesc + per-member type read dropped
- **C** 1838-1897: builds `row->rowtupdesc = CreateTemplateTupleDesc(numvars)`,
  and for each member reads typoid/typmod/typcoll (VAR/PROMISE from
  `datatype`, REC from `rectypeid`/`-1`/`InvalidOid`) and calls
  `TupleDescInitEntry` + `TupleDescInitEntryCollation`.
- **Port** lib.rs:525-557: `rowtupdesc: None`; the per-member type read and
  tupledesc population are skipped (only fieldnames/varnos filled). The
  `CreateTemplateTupleDesc`/`TupleDescInitEntry` calls are tupdesc-owner callees
  (SEAMED-ok), **but the in-crate member-type extraction is also absent** and
  the `default => panic` for an unrecognized dtype is the only retained branch.
  Affects every ROW builder (OUT-param row, cursor arg row, into row,
  make_scalar_list1) — all leave `rowtupdesc: None`.

### F4 — `build_datatype` / `is_true_array_type` DIVERGES
- **C** (`IsTrueArrayType`, pg_type.h:334): `OidIsValid(typelem) &&
  typsubscript == F_ARRAY_SUBSCRIPT_HANDLER` (= **6179**, fmgroids.h:3198).
- **Port** (seam.rs:80-82): `oid_is_valid(typelem) && typlen == -1`.
- `typlen==-1` (varlena) is **not** the subscript-handler test. Misclassifies:
  a varlena non-array type with a valid `typelem` passes the Rust test but fails
  C; a type using a non-array subscript handler is mis-flagged. `typisarray`
  drives `expand_array` at exec, so this is a correctness divergence.
- The form struct (`types_tuple::pg_type::FormData_pg_type`) carries
  `typsubscript`, so the correct check is available in-crate.
- **Fix**: `oid_is_valid(form.typelem) && form.typsubscript == 6179`
  (`F_ARRAY_SUBSCRIPT_HANDLER`). The separate `typstorage != TYPSTORAGE_PLAIN`
  AND already lives in `build_datatype` and is correct.

### F5 — `quote_identifier` reimplemented/approximated (DIVERGES)
- `quote_identifier` is a **ruleutils.c** function (called by pl_gram.y:4051),
  not defined in this unit. The port reimplements a conservative approximation
  in-crate (lib.rs:1961) that **drops the keyword check**: e.g.
  `quote_identifier("select")` → `select` (unquoted) where ruleutils quotes it,
  and quotes anything non-lowercase. This changes the positional cursor-arg list
  text. It should SEAM to the ruleutils owner rather than approximate. (If the
  owner is unreachable, mirror-PG-and-panic is the faithful stand-in, not a
  divergent reimplementation.)

## Seam audit — PASS

- Owned seam crate: `backend-pl-plpgsql-comp-seams` (51 decls). All **51
  installed** by `init_seams()` (1:1, verified by name-set diff). `init_seams`
  contains only `::set(...)` calls (closures delegate to crate fns).
- Outward seams in `src/seam.rs` are thin marshal+delegate to real owners
  (syscache `pg_type_form`, lsyscache `get_array_type`/`get_base_element_type`/
  `get_rel_type_id`/`type_is_rowtype`, `format_type_be`) or mirror-PG-and-panic
  for unreached owners (typcache composite tupdesc, `RangeVarGetRelid`/
  `RelnameGetRelid`, `parse_datatype`, `get_collation_oid`, `check_sql_expr`).
  No branching/node-construction in a seam path. **Exception**:
  `is_true_array_type` (seam.rs) embeds an *approximated predicate* — that is the
  F4 logic finding, not a wiring finding.
- Constants verified against headers: all type OIDs (BOOL=16, NAME=19, INT4=23,
  TEXT=25, OID=26, RECORD=2249, VOID=2278, TRIGGER=2279, EVENT_TRIGGER=3838,
  TEXTARRAY=1009) and all 11 polymorphic OIDs MATCH `pg_type_d.h`.
  `PROKIND_FUNCTION='f'`, `PROKIND_PROCEDURE='p'`, `PROVOLATILE_VOLATILE='v'`,
  all `TYPTYPE_*`, `TYPSTORAGE_PLAIN='p'`, `PROARGMODE_*` MATCH.
  `MAKE_SQLSTATE`/`PGSIXBIT` MATCH (`make_sqlstate` lib.rs:1313).
  `IsPolymorphicType` set MATCH. `MAXALIGN(8)` MATCH.
- `errcodes.rs` `EXCEPTION_LABEL_MAP`: **251 entries, byte-identical labels AND
  `ERRCODE_*` macro names in identical order** to `plerrcodes.h` (diff clean).
  (Correctness of the `types_error::ERRCODE_*` integer values is that crate's
  concern.)

## Design conformance — PASS (no new findings)
- Per-backend globals (`plpgsql_Datums`, `nDatums`, `datums_alloc/last`,
  `curr_compile`, `error_funcname`, dump/check-syntax flags, identifier-lookup)
  are `thread_local!` — correct for the non-reentrant compiler (no shared static).
- Allocating builders return owned values; `mem.rs` routes `palloc` paths
  through `try_reserve`→OOM panic (the repo's catch_unwind model). No invented
  u64/usize opacity for typed pointers (datums addressed by `dno: i32`, matching
  C's array-index model). No locks held across `?`.

## Verdict: **FAIL**
Fix F1 (restore `plpgsql_compile` entry, SEAM to funccache), F2 (thread
`for_validator`: extra_warnings/errors, validator polymorphic-return
substitution, **`$0` install**), F3 (populate `rowtupdesc` + per-member type
read; SEAM the tupdesc calls), F4 (`typsubscript==6179` not `typlen==-1`),
F5 (SEAM `quote_identifier` to ruleutils or mirror-panic, drop the approximation).
Then re-audit the fixed functions from scratch.
