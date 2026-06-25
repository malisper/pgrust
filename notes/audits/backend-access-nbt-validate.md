# Logic audit: backend-access-nbt-validate

- C: `/Users/malisper/workspace/work/pgrust/postgres-18.3/src/backend/access/nbtree/nbtvalidate.c`
- c2rust: `/Users/malisper/workspace/work/pgrust/c2rust-runs/backend-access-nbt-validate/src/nbtvalidate.rs`
- Port: `/Users/malisper/workspace/work/pgrust-fabled/.claude/worktrees/agent-aeebd1af93738e95b/crates/backend-access-nbt-validate/src/lib.rs`

Method: re-derived control flow, error paths (SQLSTATE+severity+predicate),
constants vs headers/catalog. Verified all 6 support-proc signature cases, the
5-strategy operator-set completeness mask, ORDER BY rejection, opclass-group +
cross-type completeness, `result &= ...` accumulation (port: set `result=false`
per failing check), INFO-vs-ERROR severity, and btadjustmembers dependency logic.

| # | Function | Verdict | Notes |
|---|----------|---------|-------|
| 1 | `btvalidate` | MATCH/SEAMED | see detailed check below. All catalog/amvalidate/lsyscache/regproc/format_type/syscache access via per-owner seams (genuinely external). Logic in-crate. |
| 2 | `btadjustmembers` | MATCH/SEAMED | see detailed check below. CCI + lsyscache + opclass_for_family_datatype via seams. |
| 3 | `list_append_unique_oid` | MATCH | append-if-absent (`!contains`), fallible reserve mapping to OOM, mirrors lappend_oid semantics. |
| 4 | `report_info` | MATCH | `ereport(INFO, errcode(ERRCODE_INVALID_OBJECT_DEFINITION), errmsg)`; INFO never raises (returns Ok); caller sets result=false. |

## btvalidate detailed verification

Support-function switch (all 6 cases) — signatures vs C:
- `BTORDER_PROC` (1): `check_amproc_signature(amproc, INT4OID, true, 2, 2, [lefttype, righttype])`. MATCH.
- `BTSORTSUPPORT_PROC` (2): `(VOIDOID, true, 1, 1, [INTERNALOID])`. MATCH.
- `BTINRANGE_PROC` (3): `(BOOLOID, true, 5, 5, [lefttype, lefttype, righttype, BOOLOID, BOOLOID])`. MATCH (arg order incl. duplicated lefttype).
- `BTEQUALIMAGE_PROC` (4): `(BOOLOID, true, 1, 1, [OIDOID])`. MATCH.
- `BTOPTIONS_PROC` (5): `check_amoptsproc_signature(amproc)`. MATCH. Matched via `n if n as u16 == BTOPTIONS_PROC` guard (BTOPTIONS_PROC is u16=5 in types_nbtree; arm ordered before `_`). Functionally amprocnum==5.
- `BTSKIPSUPPORT_PROC` (6): `(VOIDOID, true, 1, 1, [INTERNALOID])`. MATCH.
- default: INFO "invalid support number", `result=false`, `continue` (skip the !ok message). MATCH.
- `!ok` path: INFO "wrong signature for support number", `result=false`. MATCH.

Operator loop:
- strategy bounds `< 1 || > BTMaxStrategyNumber` (5): INFO "invalid strategy number". MATCH (`> BTMaxStrategyNumber as i16`).
- ORDER BY rejection: `amoppurpose != AMOP_SEARCH || OidIsValid(amopsortfamily)`: INFO "invalid ORDER BY specification". MATCH (AMOP_SEARCH='s').
- operator signature `check_amop_signature(opr, BOOLOID, lefttype, righttype)`: on false INFO "wrong signature". MATCH.

Group consistency:
- `identify_opfamily_groups(oprlist, proclist)` via seam; port projects owned rows to amvalidate AmopRow/AmprocRow (fields read by the helper). MATCH (faithful projection).
- in_range-only skip: `operatorset == 0 && functionset == (1 << BTINRANGE_PROC)` -> continue. MATCH (`1u64 << 3`).
- `usefulgroups++`. MATCH.
- opclassgroup match: `lefttype == opcintype && righttype == opcintype`. MATCH.
- familytypes: `list_append_unique_oid` for left+right. MATCH.
- operator-set completeness mask = `(1<<Less)|(1<<LessEqual)|(1<<Equal)|(1<<GreaterEqual)|(1<<Greater)` = strategies 1..5: on mismatch INFO "missing operator(s) for types". MATCH.
- `(functionset & (1<<BTORDER_PROC)) == 0`: INFO "missing support function for types". MATCH.
- `!opclassgroup`: INFO "missing operator(s)" (opclass-named). MATCH.
- cross-type completeness: `usefulgroups != list_length(familytypes)^2`: INFO "missing cross-type operator(s)". MATCH.
- elog(ERROR) "cache lookup failed for operator class %u" -> `Err(PgError::error(...))` (ERROR severity, raising). MATCH.
- ReleaseCatCacheList/ReleaseSysCache -> owned lists drop. MATCH (semantic).

## btadjustmembers detailed verification
- `OidIsValid(opclassoid)`: CommandCounterIncrement (xact seam) + `get_opclass_input_type(opclassoid)`; else `opcintype = InvalidOid`. MATCH.
- iterate `operators.chain(functions)` = `list_concat_copy(operators, functions)` order. MATCH.
- branch 1: `op.is_func && op.number != BTORDER_PROC` -> soft family dep (hard=false, family=true, refobjid=opfamilyoid). MATCH.
- branch 2: `op.lefttype != op.righttype` -> soft family dep. MATCH.
- else (not cross-type): if `lefttype != opcintype` re-lookup via `opclass_for_family_datatype(BTREE_AM_OID, opfamilyoid, opcintype)` updating cached opcintype; then if OidIsValid(opclassoid) hard opclass dep (hard=true, family=false, refobjid=opclassoid) else soft family dep. MATCH.

Constants verified vs headers/catalog:
- `BTORDER_PROC=1`, `BTSORTSUPPORT_PROC=2`, `BTINRANGE_PROC=3`, `BTEQUALIMAGE_PROC=4`, `BTOPTIONS_PROC=5`, `BTSKIPSUPPORT_PROC=6` (nbtree.h:717-722). MATCH.
- `BTMaxStrategyNumber=5`; strategy 1..5 = Less/LessEqual/Equal/GreaterEqual/Greater (stratnum.h:29-35). MATCH.
- `AMOP_SEARCH='s'` (pg_amop.h:100). MATCH.
- `BTREE_AM_OID=403` (pg_am.dat). MATCH.
- `INT4OID=23`, `BOOLOID=16`, `VOIDOID=2278`, `OIDOID=26`, `INTERNALOID=2281` (pg_type.dat). MATCH.
- `ERRCODE_INVALID_OBJECT_DEFINITION` + INFO severity on all warnings; ERROR (raising) only for cache-lookup-failed. MATCH.

Spot-checked in full: support-proc switch (all 6 signatures + default + !ok),
group operator-set/functionset masks (bit positions vs strategy/proc numbers),
btadjustmembers three-way dependency strength logic.

VERDICT: PASS — 4/4 functions MATCH or MATCH/SEAMED. External substrate
(syscache/amvalidate/lsyscache/regproc/format_type/xact/error) correctly reached
via per-owner seams; all in-crate logic, constants, masks, branch order, SQLSTATE
and severity match C. No FAIL/MISSING/PARTIAL/DIVERGES.
