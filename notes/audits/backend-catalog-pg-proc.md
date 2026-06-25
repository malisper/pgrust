# Audit: backend-catalog-pg-proc

C source: `src/backend/catalog/pg_proc.c` (1217 lines, PostgreSQL 18.3).
Port: `crates/backend-catalog-pg-proc/src/lib.rs`.
Owned seam crate: `crates/backend-catalog-pg-proc-seams`.
Carrier: `crates/types-catalog/src/pg_proc.rs`.
Catalog-tuple F1 bodies: `crates/backend-catalog-indexing/src/family1.rs`.

Independent re-derivation from the C and `catalog/pg_proc.h`; the port's
comments were not trusted.

## Function inventory

| # | C function (loc) | Port location | Verdict | Notes |
|---|---|---|---|---|
| 1 | `ProcedureCreate` (97-735) | `lib.rs::ProcedureCreate` | MATCH | full decision logic in-crate; catalog-tuple value layer + replace-path old-tuple reads + validators SEAMED |
| 2 | `fmgr_internal_validator` (745-777) | `lib.rs::fmgr_internal_validator` | MATCH (SEAMED body) | access check + `fmgr_internal_function` lookup SEAMED |
| 3 | `fmgr_c_validator` (788-823) | `lib.rs::fmgr_c_validator` | MATCH (SEAMED body) | access check + `load_external_function`/`fetch_finfo_record` SEAMED |
| 4 | `fmgr_sql_validator` (831-993) | `lib.rs::fmgr_sql_validator` | MATCH | pseudotype gates carried in-crate; body parse SEAMED |
| 5 | `sql_function_parse_error_callback` (998-1009) | `lib.rs::sql_function_parse_error_callback` | MATCH | |
| 6 | `function_parse_error_transpose` (1022-1081) | `lib.rs::function_parse_error_transpose` | MATCH | error-position plumbing SEAMED |
| 7 | `match_prosrc_to_query` (1089-1137) | `lib.rs::match_prosrc_to_query` | MATCH | byte/char scan ported 1:1 (NUL-safe reads) |
| 8 | `match_prosrc_to_literal` (1147-1202) | `lib.rs::match_prosrc_to_literal` | MATCH | backslash/doubled-quote handling ported 1:1 |
| 9 | `oid_array_to_list` (1204-1217) | `lib.rs::oid_array_to_list` | MATCH | `deconstruct_array_builtin(OIDOID)` direct |

## Constant-table audit (mandatory for a catalog carrier)

Verified field-for-field against `catalog/pg_proc.h` (PostgreSQL 18.3):

- `ProcedureRelationId = 1255` (CATALOG line). MATCH.
- `ProcedureOidIndexId = 2690` (`DECLARE_UNIQUE_INDEX_PKEY(pg_proc_oid_index, 2690, ...)`). MATCH.
- `ProcedureNameArgsNspIndexId = 2691` (`DECLARE_UNIQUE_INDEX(pg_proc_proname_args_nsp_index, 2691, ...)`). MATCH.
- `Natts_pg_proc = 30`. Column count of `FormData_pg_proc`. MATCH.
- `Anum_pg_proc_*` 1..30 in field order: oid(1), proname(2), pronamespace(3),
  proowner(4), prolang(5), procost(6), prorows(7), provariadic(8), prosupport(9),
  prokind(10), prosecdef(11), proleakproof(12), proisstrict(13), proretset(14),
  provolatile(15), proparallel(16), pronargs(17), pronargdefaults(18),
  prorettype(19), proargtypes(20), proallargtypes(21), proargmodes(22),
  proargnames(23), proargdefaults(24), protrftypes(25), prosrc(26), probin(27),
  prosqlbody(28), proconfig(29), proacl(30). MATCH (the C `CATALOG_VARLEN`
  columns 21-30 keep their genbki order: oidvector `proargtypes` is column 20,
  the first direct-access varlen, then the `#ifdef CATALOG_VARLEN` block).
- `PROKIND_FUNCTION='f'`, `PROKIND_AGGREGATE='a'`, `PROKIND_WINDOW='w'`,
  `PROKIND_PROCEDURE='p'`. MATCH (pg_proc.h:159-164).
- `PROARGMODE_IN='i'`, `_OUT='o'`, `_INOUT='b'`, `_VARIADIC='v'`, `_TABLE='t'`.
  MATCH (pg_proc.h:182-186).
- `SQLlanguageId = 14` (pg_language.dat `oid => '14', oid_symbol => 'SQLlanguageId'`).
  MATCH.
- `FUNC_MAX_ARGS = 100` (pg_config_manual.h). MATCH.

## ProcedureCreate per-block detail

- Parameter-count check (155-161): `parameterCount = dim1`; `< 0 || > FUNC_MAX_ARGS`
  -> `ERRCODE_TOO_MANY_ARGUMENTS` `errmsg_plural`. MATCH.
- `allParameterTypes` deconstruct (165-188): the C verifies the 1-D OID array and
  reads `ARR_DATA_PTR`; the idiomatic caller hands the already-deconstructed
  `Vec` ("we assume caller got the contents right"). `allParamCount`/`allParams`
  selection MATCHES; the `parameterModes` 1-D char-array verify is likewise the
  caller's. MATCH.
- Polymorphic + internal return-type checks (211-231): `check_valid_polymorphic_signature`
  / `check_valid_internal_signature` called directly on `backend-parser-coerce`
  (real ported owner, no cycle). Error code/message/`errdetail_internal` MATCH.
- OUT-arg checks (236-262): input-only skip (`paramModes == NULL || IN ||
  VARIADIC`), then the same two checks per OUT arg. MATCH.
- Variadic-type identification loop (265-314): per-mode switch incl.
  `OUT && prokind == PROKIND_PROCEDURE` guard, the ANY/ANYARRAY/ANYCOMPATIBLEARRAY
  special cases, `get_element_type` else, and the four `elog(ERROR)` paths.
  MATCH (`get_element_type` via lsyscache-seams; `Option<Oid>` `None` =
  `!OidIsValid`).
- Field formation (327-379): assembled into `PgProcInsertRow`; the
  `heap_form_tuple` `values[]`/`nulls[]` build runs in `family1.rs::proc_values_nulls`
  in the catalog-tuple owner, column-by-column verified below.
- Replace branch (391-590): `SearchSysCache3(PROCNAMEARGSNSP)` SEAMED
  (`search_proc_name_args_nsp` returns `OldProcFacts` + held `FormedTuple`);
  `!replace` duplicate-function error, `object_ownercheck`/`aclcheck_error`,
  routine-kind change, return-type change, RECORD OUT-param row-type change
  (`record_type_change` seam), input-param-name change
  (`check_input_param_names_unchanged` seam), existing-defaults compatibility
  (`check_defaults_compatible` seam) — all branch order / error code / message /
  `errhint` ("Use DROP ... first.") MATCH. The replaces[] clearing of
  oid/proowner/proacl happens inside `catalog_tuple_update_pg_proc`. MATCH.
- Insert branch (591-609): `get_user_default_acl` (aclchk-seams) -> proacl;
  `GetNewOidWithIndex` (indexing-seams) -> newOid; `catalog_tuple_insert_pg_proc`.
  MATCH.
- Dependency recording (615-683): `deleteDependencyRecordsFor` on update;
  `new_object_addresses` + `add_exact_object_address` for namespace / language /
  return type / each param type / each transform / support func; then
  `record_object_address_dependencies(DEPENDENCY_NORMAL)`; SQL-body +
  parameter-defaults `recordDependencyOnExpr` (SEAMED — cooked-tree owner);
  owner + new-ACL deps on insert; `recordDependencyOnCurrentExtension`. Order
  and conditions MATCH.
- Post-create hook + close (685-690): `InvokeObjectPostCreateHook` then
  `table_close`. MATCH. (The C `heap_freetuple(tup)` before the hook is the
  owned `FormedTuple` drop inside the seam — behaviour-identical.)
- Validator (692-728): `CommandCounterIncrement` (xact-seams) then
  `run_language_validator` (the GUC nest-level + `OidFunctionCall1` dance must
  wrap together, SEAMED). MATCH.
- `pgstat_create_function` on insert (730-732). SEAMED. MATCH.

## family1.rs `proc_values_nulls` column audit (the heap_form_tuple values[])

Each column matches pg_proc.c:327-379:
- Fixed cols oid..prorettype: `from_oid`/`from_f32`/`from_char`/`from_bool`/
  `from_u16` (pronargs/pronargdefaults are `UInt16GetDatum`). MATCH.
- proargtypes: `buildoidvector` (24-byte header + n*Oid, `SET_VARSIZE`). MATCH.
- proallargtypes/protrftypes: `construct_array(OIDOID)` or null. MATCH.
- proargmodes: `construct_array(CHAROID)` or null. MATCH.
- proargnames/proconfig: `build_text_array` (TEXTOID, no-null) or null; unnamed
  slots are the empty string (`CStringGetTextDatum("")`). MATCH.
- proargdefaults/prosqlbody: `CStringGetTextDatum(nodeToString(...))` text framed
  as a `text` varlena, or null. MATCH.
- prosrc: `CStringGetTextDatum(prosrc)`, always present. MATCH.
- probin: text or null. MATCH.
- proacl: present on insert with a default ACL (header-only carrier note below);
  null otherwise. MATCH structurally.

## Seamed legs (mirror-pg-and-panic, genuinely unported owners)

Declared in `backend-catalog-pg-proc-seams`, uninstalled until their owners land:

- `search_proc_name_args_nsp`, `record_type_change`,
  `check_input_param_names_unchanged`, `check_defaults_compatible` — the
  replace-path syscache reads of the held old tuple (`SearchSysCache3` +
  `SysCacheGetAttr` + `build_function_result_tupdesc_t/_d` + `equalRowTypes` +
  `get_func_input_arg_names` + `stringToNode`/`exprType`). Reach the syscache /
  funcapi owners. (funcapi is ported, but the held-old-tuple syscache projection
  + `equalRowTypes` are not yet exposed as a pg_proc projection seam.)
- `check_function_validator_access`, `check_function_bodies`,
  `search_proc_oid_sql`, `validate_internal_function`, `validate_c_function`,
  `run_sql_function_body_check`, `run_language_validator` — the validator bodies,
  reaching fmgr / dfmgr / parser (`pg_parse_query` / `pg_analyze_and_rewrite_withcb`)
  / executor-functions (`check_sql_fn_*`) / GUC owners.
- `node_to_string_sqlbody`, `node_to_string_defaults`,
  `record_dependency_on_sqlbody`, `record_dependency_on_defaults` — the cooked
  `prosqlbody` / `parameterDefaults` serialization + reference-walk. These carry
  the consumer's `types_parsenodes::Node` vocabulary (functioncmds.c builds the
  cooked trees in that model); the outfuncs / dependency owners install them when
  the cooked-node model lands.
- `pgstat_create_function`, and the `function_parse_error_transpose`
  error-position plumbing (`geterrposition` / `getinternalerrposition` /
  `errposition` / `internalerrposition` / `internalerrquery` /
  `active_portal_source_text` / `errcontext_sql_function`).

These are the same class of boundary as pg_type.c's uninstalled catalog-tuple
seams: real cross-unit edges whose owners are not yet ported.

## Installed seams

- `backend-catalog-indexing-seams::{get_new_oid_with_index_pg_proc,
  catalog_tuple_insert_pg_proc, catalog_tuple_update_pg_proc}` — NEW, installed
  by `backend-catalog-indexing::family1::install` (the catalog-tuple owner).
- `backend-commands-functioncmds-seams::procedure_create` — installed by this
  unit's `init_seams` (pg_proc.c owns `ProcedureCreate`; functioncmds.c is the
  first consumer). The `ProcedureCreateArgs` bundle is adapted to the positional
  call.

## Known carrier limitation (banked, not a divergence in this unit)

`proacl` rides the shared header-only `types_array::ArrayType` carrier (16-byte
varlena header, no `aclitem[]` data area) — the same limitation that keeps
`backend-catalog-aclchk`'s `get_user_default_acl` / `record_dependency_on_new_acl`
uninstalled (mirror-and-panic). Until the ACL-data carrier lands those producers
panic, so the `proacl` framing path is never reached with real bytes; the
`array_type_header_bytes` framer is the faithful image of the carrier as modeled.

## Gate

`cargo check --workspace` green; `no-todo-guard` green; `seams-init`
recurrence guards (`every_seam_installing_crate_is_wired_into_init_all`,
`every_declared_seam_is_installed_by_its_owner`) green. No `todo!` /
`unimplemented!` / owned-logic `unwrap` / `panic!` in the ported code.
