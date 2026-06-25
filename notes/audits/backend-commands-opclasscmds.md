# Audit: backend-commands-opclasscmds

- **Verdict: PASS**
- Date: 2026-06-12
- Model: claude-opus-4-8[1m]
- C source: `../pgrust/postgres-18.3/src/backend/commands/opclasscmds.c` (sole `c_sources` entry)
- c2rust: `../pgrust/c2rust-runs/backend-commands-opclasscmds/src/opclasscmds.rs`
- Port: `crates/backend-commands-opclasscmds/src/lib.rs` (+ parse-node/catalog types in `crates/types-opclass`, `crates/types-catalog::opclasscmds_catalog`)

This audit was re-derived independently from the C and c2rust; the port's own
comments and the pre-existing `audited` catalog marking were not trusted.

## Function inventory & verdicts

All 21 C function definitions (incl. statics) enumerated from the C source and
cross-checked against the c2rust rendering (which kept every built function).

| # | C function | C loc | Port loc (lib.rs) | Verdict | Notes |
|---|-----------|-------|-------------------|---------|-------|
| 1 | `OpFamilyCacheLookup` | :80 | `OpFamilyCacheLookup` :127 | MATCH | qualified→`syscache_opfamily_oid`, unqualified→`OpfamilynameGetOpfid`; not-found+!missing_ok raises `ERRCODE_UNDEFINED_OBJECT` with am-name; am cache miss → internal error. Returns `Option<Oid>` mirroring NULL htup. |
| 2 | `get_opfamily_oid` | :138 | `get_opfamily_oid` :184 | MATCH | `None`→`InvalidOid`, else oid. |
| 3 | `OpClassCacheLookup` | :161 | `OpClassCacheLookup` :201 | MATCH | structurally identical to (1) for opclass (`get_opclass_oid` syscache / `OpclassnameGetOpcid`). |
| 4 | `get_opclass_oid` | :219 | `get_opclass_oid` :260 | MATCH | mirror of (2). |
| 5 | `CreateOpFamily` | :242 | `CreateOpFamily` :279 | MATCH | dup-name check → `ERRCODE_DUPLICATE_OBJECT`; pg_opfamily insert (opfmethod/opfname/opfnamespace/opfowner=GetUserId); deps: AM=AUTO, namespace=NORMAL, owner, current-extension(false); EventTriggerCollectSimpleCommand + post-create hook. Insert+OID alloc delegated to indexing seam. |
| 6 | `DefineOpClass` | :332 | `DefineOpClass` :376 | MATCH | full branch order preserved: namespace ACL_CREATE check; `get_index_am_oid`+`get_index_am_info`; `maxOpNumber<=0 → SHRT_MAX(32767)`; superuser check; datatype lookup; opfamily lookup-or-create; item loop (operator/function/storage dispatch incl. op-number & proc-number range errors, sortfamily lookup via BTREE_AM_OID, storage-once check); storage legality (drop-if-==typeoid / amstorage error); opclass dup-name check; isDefault scan via `systable_scan` on `OpclassAmNameNspIndexId` with opcintype==typeoid && opcdefault → DUPLICATE_OBJECT+errdetail; pg_opclass insert; ref-field defaulting (hard, !family, refobjid=opclassoid); amadjustmembers; storeOperators/Procedures(false); EventTriggerCollectCreateOpClass; opclass deps (namespace=NORMAL, opfamily=AUTO, typeoid=NORMAL, storage=NORMAL if valid, owner, extension); post-create hook. |
| 7 | `DefineOpFamily` | :771 | `DefineOpFamily` :739 | MATCH | namespace ACL; `get_index_am_oid`; superuser check; delegates to CreateOpFamily. |
| 8 | `AlterOpFamily` | :817 | `AlterOpFamily` :783 | MATCH | am lookup (`get_index_am_oid`+info; C uses SearchSysCache(AMNAME) then GetIndexAmRoutineByAmId — equivalent name→oid lookup with same UNDEFINED_OBJECT failure); maxOpNumber clamp; opfamily lookup; superuser check; isDrop→Drop else Add; returns opfamilyoid. |
| 9 | `AlterOpFamilyAdd` | :880 | `AlterOpFamilyAdd` :830 | MATCH | re-fetches amroutine; item loop with operator (objargs-required else SYNTAX_ERROR), soft-dep fields (ref_is_hard=false, ref_is_family=true, refobjid=opfamilyoid), function, STORAGE→SYNTAX_ERROR, default→internal; amadjustmembers(InvalidOid opclass); storeOperators/Procedures(true); EventTriggerCollectAlterOpFam. |
| 10 | `AlterOpFamilyDrop` | :1029 | `AlterOpFamilyDrop` :959 | MATCH | item loop op/func via processTypesSpec (range errors), STORAGE falls into default→internal error (grammar-prevented); dropOperators/dropProcedures; collect alter event. |
| 11 | `processTypesSpec` | :1107 | `processTypesSpec` :1036 | MATCH | Assert(args!=NIL)→debug_assert; lefttype from [0]; righttype from [1] or =lefttype; len>2 → SYNTAX_ERROR. |
| 12 | `assignOperTypes` | :1136 | `assignOperTypes` :1063 | MATCH | OPEROID lookup→internal err on miss; oprkind!='b'→INVALID_OBJECT_DEFINITION; sortfamily branch re-fetches amroutine, !amcanorderbyop error; else oprresult!=BOOLOID error; default lefttype/righttype from oprleft/oprright. |
| 13 | `assignProcTypes` | :1202 | `assignProcTypes` :1128 | MATCH | PROCOID lookup; opclassOptsProcNum branch (type-match checks + signature (internal)→void) takes priority via if/else-if; else-if amcanorder chain (BTORDER=1/SORTSUPPORT=2/INRANGE=3/EQUALIMAGE=4/SKIPSUPPORT=6) with exact arg/ret checks and lefttype/righttype inference; else-if amcanhash (HASHSTANDARD=1/HASHEXTENDED=2) + input-type defaulting; final typeoid defaulting + must-be-specified error. amcanorder/amcanhash each re-fetch amroutine (matches c2rust :5715/:6228). |
| 14 | `addFamilyMember` | :1416 | `addFamilyMember` :1354 | MATCH | duplicate (number,lefttype,righttype) → func/op-specific "appears more than once" with format_type_be; append. |
| 15 | `storeOperators` | :1453 | `storeOperators` :1399 | MATCH | isAdd dup check via `amop_oid` (AMOPSTRATEGY) → DUPLICATE_OBJECT; oppurpose=ORDER('o')/SEARCH('s'); pg_amop insert (all 9 Anums); deps: operator (NORMAL/AUTO by ref_is_hard), family-or-class (INTERNAL/AUTO), lefttype if typeDepNeeded, righttype if !=left && needed, sortfamily if valid; post-create hook. |
| 16 | `storeProcedures` | :1583 | `storeProcedures` :1544 | MATCH | mirror of (15) for pg_amproc via `amproc_oid` (AMPROCNUM); 6 Anums; same dep structure. |
| 17 | `typeDepNeeded` | :1699 | `typeDepNeeded` :1671 | MATCH | pinned→false; func→`get_func_signature` arg scan; else `op_input_types` left/right compare. |
| 18 | `dropOperators` | :1749 | `dropOperators` :1704 | MATCH | `amop_oid` lookup, !valid→UNDEFINED_OBJECT, else performDeletion(amop, DROP_RESTRICT, 0). |
| 19 | `dropProcedures` | :1789 | `dropProcedures` :1740 | MATCH | mirror of (18) on pg_amproc. |
| 20 | `IsThereOpClassInNamespace` | :1829 | `IsThereOpClassInNamespace` :1780 | MATCH | `opclass_exists`→DUPLICATE_OBJECT with am-name + schema-name. |
| 21 | `IsThereOpFamilyInNamespace` | :1852 | `IsThereOpFamilyInNamespace` :1802 | MATCH | mirror of (20) on opfamily. |

### Constant verification (against headers, not memory)
- `BTORDER_PROC=1, BTSORTSUPPORT_PROC=2, BTINRANGE_PROC=3, BTEQUALIMAGE_PROC=4,
  BTSKIPSUPPORT_PROC=6` — confirmed `access/nbtree.h:717-722`.
- `HASHSTANDARD_PROC=1, HASHEXTENDED_PROC=2` — confirmed `access/hash.h:355-356`.
- `AMOP_SEARCH='s', AMOP_ORDER='o'` — confirmed `pg_amop.h:100-101`; port
  `types_opclass` encodes as `b's' as i8` / `b'o' as i8`.
- `BOOLOID=16, INT8OID=20, INT4OID=23, VOIDOID=2278, INTERNALOID=2281,
  BTREE_AM_OID=403` — match `types_core::catalog`.
- `SHRT_MAX=32767` — correct.

## Seam audit (step 3)

**Owned seam crates: none.** The unit's only `c_sources` file is
`opclasscmds.c`; the only seam crate that would map to it is a hypothetical
`crates/opclasscmds-seams`, which does not exist. Therefore there is no inward
seam to install. `init_seams()` is empty (lib.rs:109), which is correct here —
the sole C callers of these routines (utility.c, alter.c) are unported and will
depend on this crate directly when they land (no dependency cycle). An empty
installer is correct, not a finding, because there are zero owned seam crates
outstanding.

**Outward seam calls** — all resolve to *other-unit-owned* seam crates and are
thin marshal+delegate (no branching/node-construction/computation in any seam
path); each is justified by the callee unit being unported:

- amapi: `get_index_am_info` (scalar projection of `IndexAmRoutine`),
  `am_adjust_members` (in-place list mutation marshaled as owned-in/owned-out
  PgVec pair).
- genam: `systable_scan` (isDefault scan).
- aclchk: `object_aclcheck`, `aclcheck_error`.
- catalog: `is_pinned_object`. dependency: `perform_deletion`.
- indexing: `catalog_tuple_insert_pg_{opfamily,opclass,amop,amproc}` (insert +
  OID allocation, returns assigned oid).
- pg_depend: `recordDependencyOn`, `recordDependencyOnCurrentExtension`.
  pg_shdepend: `record_dependency_on_owner`.
- amcmds: `get_index_am_oid`. event_trigger: collect simple/create-opclass/
  alter-opfam. objectaccess: `object_access_hook_present` +
  `run_object_post_create_hook` (the `InvokeObjectPostCreateHook` macro guard).
- parse_func/parse_oper/parse_type: `lookup_func_with_args`,
  `lookup_oper_with_args`, `lookup_oper_name`, `typename_type_id`,
  `typename_to_string`. The dropped C args (`OBJECT_FUNCTION`, `noError=false`,
  `location=-1`) are correctly baked into the seam signatures.
- lsyscache: `get_am_name`, `get_func_signature`, `op_input_types`,
  `get_namespace_name`. syscache: `opfamily_exists`, `opclass_exists`,
  `get_opfamily_oid`, `get_opclass_oid`, `amop_oid`, `amproc_oid`,
  `oper_row_by_oid`, `proc_row_by_oid`.
- table: `table_open` (+ `Relation::close`).
- miscinit: `get_user_id`, `superuser_arg`.

The orchestration (which catalog rows, which dependencies, in which order, all
validity branches) lives in this crate. No function body was replaced by a seam
to "somewhere else" — every C body is present in-crate.

## Design conformance (step 3b)

- Allocating routines take `Mcx` and return `PgResult` (e.g. `DefineOpClass`,
  `CreateOpFamily`, the `vec_with_capacity_in(mcx, …)` member lists). OK.
- No invented opacity: parse nodes are concrete `TypeName` / `ObjectWithArgs` /
  `CreateOpClassItem` / `OpFamilyMember` in `types-opclass`; catalog FormData
  rows are concrete in `types-catalog::opclasscmds_catalog`. No opaque `Node`
  handles introduced. OK (types.md rules 6-7).
- No shared statics for per-backend globals; no ambient-global seam; no locks
  held across `?` (relations opened/closed via `Relation` with explicit
  `close`; errors propagate before close only on the raising path, matching C
  where `ereport(ERROR)` longjmps past `table_close`). OK.
- No registry-shaped side tables; no unledgered divergence markers.
- Minor, non-blocking: `am_name_for_error` / namespace error helpers spin a
  private `MemoryContext` for error-message rendering instead of reusing the
  in-scope `mcx`; behavior is identical (string consumed into the error
  immediately), no leak. Not a finding.

## Auditor self-check (re-derived MATCH samples)

- `DefineOpClass` opfamily auto-create: C `opfstmt->opfamilyname =
  stmt->opclassname; CreateOpFamily(opfstmt, opcname, …)` ⇄ port
  `opfamilyname: stmt.opclassname.clone(); CreateOpFamily(&opfstmt, &opcname,
  …)`. Verified identical.
- `assignProcTypes` priority: C uses `if (opts) … else if (amcanorder) … else
  if (amcanhash) …`; port preserves the exact if/else-if chain with the same
  re-fetch of the amroutine in each `else if` (matches c2rust :5715/:6228).
- `storeOperators` dependency strengths: NORMAL/AUTO and INTERNAL/AUTO selection
  by `ref_is_hard`, family-vs-class by `ref_is_family` — byte-for-byte mirror.

## Verdict

All 21 functions **MATCH**. No owned seam crates (empty `init_seams()`
correct); all outward seams are thin, justified delegates. No logic or
design-conformance findings.

**PASS.**
