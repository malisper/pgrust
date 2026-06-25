# Audit: backend-commands-extension (F0)

C source: `src/backend/commands/extension.c` (PostgreSQL 18.3).
c2rust reference: extension.c is the sole C file for the unit.

F0 scope (per task #321): the parse/analysis half + the version-update-path
graph + `RemoveExtensionById` + `CreateExtension` parse-side + the
`creating_extension` / `CurrentExtensionObject` backend-globals. The
executor-/SPI-/catalog-DML command bodies are out of F0 and live in
`src/deferred.rs` behind loud panics (mirror-pg-and-panic); see the ledger at
the end.

Base: NOT copied from src-idiomatic verbatim. That base targets the abandoned
`seams_ub_ddlgiants` centralized-seam model; the pure algorithms are reproduced
1:1, but every external is re-pointed at this repo's real owners
(syscache/genam/table/indexing/fd/guc-file/path/encoding/varlena), and the
catalog-read cores + `RemoveExtensionById` + `CreateExtension` + the globals are
added (they were deferred panics in the base).

## Globally visible state (C 78-80)

| C | Port | Verdict |
|---|---|---|
| `bool creating_extension = false` | `thread_local CREATING_EXTENSION: Cell<bool>` + `creating_extension()` reader / `set_creating_extension` | OK — per-backend global → thread_local (decision table) |
| `Oid CurrentExtensionObject = InvalidOid` | `thread_local CURRENT_EXTENSION_OBJECT: Cell<Oid>` + `current_extension_object()` / setter | OK |

## new_ExtensionControlFile (C 4003-4018)

Defaults `relocatable=false`, `superuser=true`, `trusted=false`, `encoding=-1`,
pointer fields null → `None`/empty `Vec`. `char*`→`Option<String>`,
`List*`→`Vec<String>`; `name` non-optional. OK.

## check_valid_extension_name / check_valid_version_name (C 359-448)

Four checks each, in C order: empty / contains `--` / leading-or-trailing `-` /
directory separator. Each `ereport(ERROR, ERRCODE_INVALID_PARAMETER_VALUE)` with
the exact `errmsg` ("invalid extension name: \"%s\"" / "invalid extension
version name: \"%s\"") and the matching `errdetail`. Directory-separator check
via in-crate `first_dir_separator` (`/` and `\`, common/path.c). OK — byte-exact.

## is_extension_{control,script}_filename (C 453-467)

`rfind('.')` then compare the suffix to `.control` / `.sql`. Matches the C
`last_dot` / `strcmp`. OK.

## Path derivation (C 472-623)

| C fn | Port | Verdict |
|---|---|---|
| `get_extension_control_directories` (472-533) | reads `Extension_control_path` GUC (`vars::Extension_control_path` accessor) + `get_share_path(my_exec_path)` (common-path seam over the init-small `my_exec_path` global); `$system` empty → single `<share>/extension`; else `:`-split walk with `$system` macro-sub / `/extension` suffix / `canonicalize_path` each | OK — split loop + suffix in-crate; `first_path_var_separator` + `substitute_path_macro` ported in-crate 1:1 (the dfmgr copies are the model); canonicalize via common-path seam |
| `find_in_paths` (4028-4059) | canonicalize each, require `is_absolute_path` (else ERRCODE_INVALID_NAME, exact "component in parameter \"extension_control_path\" is not an absolute path"), `<path>/<basename>`, `pg_file_exists` (fd seam) → first hit | OK |
| `find_extension_control_filename` (540-564) | `<name>.control` + `get_extension_control_directories` + `find_in_paths`; on hit record `control_dir = result[..rfind('/')]` | OK |
| `get_extension_script_directory` (566-583) | `control_dir` when `directory` unset; `directory` when absolute (is_absolute_path seam); else `<basedir>/<directory>` | OK |
| `get_extension_aux_control_filename` (585-601) | `<scriptdir>/<name>--<ver>.control` | OK |
| `get_extension_script_filename` (603-623) | `<scriptdir>/<name>--[from--]<ver>.sql` | OK |

## parse_extension_control_file (C 640-823) + read_extension_{control,aux} (828-865)

- File locate: aux → `get_extension_aux_control_filename`; primary with
  `control_dir` set → `<dir>/<name>.control`; else `find_extension_control_filename`
  (None → ERRCODE_FEATURE_NOT_SUPPORTED "extension \"%s\" is not available" +
  the install-hint). OK.
- `basedir = control_dir[..len-"/extension".len()]` with the `/extension`
  debug_assert. OK.
- File read+parse: `read_control_file_items` = fd `allocate_file_read` (ENOENT →
  `None`) + guc-file `ParseConfigFp(contents, filename, CONF_FILE_START_DEPTH,
  ERROR, &mut vars)`. `None`+aux → silent `Ok(())` (C `errno==ENOENT && version`
  early return); `None`+primary → errcode_for_file_access "could not open
  extension control file \"%s\"" (C AllocateFile failure). Non-ENOENT open/read
  errors surface as `Err` from the fd seam (matches C). OK.
- ConfigVariable dispatch: directory/default_version (secondary-file rejection
  when `version`, ERRCODE_SYNTAX_ERROR exact msg); module_pathname/comment/schema
  (copy); relocatable/superuser/trusted (`parse_bool` via the scalar-bool seam,
  else "parameter \"%s\" requires a Boolean value"); encoding
  (`pg_valid_server_encoding`, `<0` → ERRCODE_UNDEFINED_OBJECT "\"%s\" is not a
  valid encoding name"); requires/no_relocate (`SplitIdentifierString` via
  varlena seam, `None` → "parameter \"%s\" must be a list of extension names");
  unrecognized → ERRCODE_SYNTAX_ERROR "unrecognized parameter \"%s\" in file
  \"%s\"". Then `relocatable && schema` → ERRCODE_SYNTAX_ERROR conflict. OK —
  branch order, SQLSTATEs, and messages all match.
- `read_extension_control_file` wraps parse(primary); `read_extension_aux_control_file`
  flat-clones `pcontrol` and re-parses(aux) without mutating the original. OK.

## Version-update-path graph (C 1468-1773)

`ExtensionVersionInfo`: C raw-pointer `reachable`(List*)/`previous` → index arena
(`EviList = Vec<...>`), `previous: usize` with `NO_PREV = usize::MAX` for the C
NULL. Every traversal/tie-break/predecessor walk preserved.

| C fn | Port | Verdict |
|---|---|---|
| `get_ext_ver_info` (1468) | find-or-create vertex; new = distance INT_MAX / not-known / NO_PREV | OK |
| `get_nearest_unprocessed_vertex` (1501) | min-distance over not-`distance_known` | OK |
| `get_ext_ver_list` (1529) | `list_dir` (fd) over `get_extension_script_directory`; per entry: `.sql` filter, byte-exact `<name>--` prefix (`strncmp` + `[extnamelen]`/`[+1]`), strip trailing `.` via `rfind`, `strstr("--")` split → install (no second `--`, mark installable) vs update (`from->to` edge; third `--` ignored) | OK — `AllocateDir` complains on a missing dir → `list_dir(.., missing_ok=false)` errors on `None` |
| `find_update_path` (1635) | Dijkstra: reinitialize reset, start dist 0, INT_MAX break, target break, reachable snapshot (mutate other vertices only), `newdist<` relax + strcmp tie-break (`newdist==`, prev!=NULL, name `<`), lcons predecessor walk → forward names excluding start | OK — tie-break + start-excluded result match |
| `identify_update_path` (1592) | build graph + start/target + `find_update_path(false,false)`; None → "extension \"%s\" has no update path from version \"%s\" to version \"%s\"" (ERRCODE_INVALID_PARAMETER_VALUE) | OK |
| `find_install_path` (1728) | installable target → (self, []); else over installable candidates `find_update_path(true,true)`, keep shorter / strcmp-tie-break-on-start | OK |

## Catalog-read cores

| C fn | Port | Verdict |
|---|---|---|
| `get_extension_oid` (188-204) | `GetSysCacheOid(EXTENSIONNAME, Anum_pg_extension_oid, Str(extname), …)`; `!OidIsValid && !missing_ok` → ERRCODE_UNDEFINED_OBJECT "extension \"%s\" does not exist" | OK — `GetSysCacheOid1` ≡ GetSysCacheOid + 3×UNUSED |
| `get_extension_name` (210-225) | `SearchSysCache1(EXTENSIONOID, oid)` → None for no row; `SysCacheGetAttr(extname)` NameStr bytes-to-first-NUL copied into mcx; `ReleaseSysCache` | OK — null extname (non-nullable NAME) → hard error (corruption), faithful |

## RemoveExtensionById (C 2280-2322)

| C step | Port | Verdict |
|---|---|---|
| `if (extId == CurrentExtensionObject) ereport(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, "cannot drop extension \"%s\" because it is being modified", get_extension_name(extId))` | same; name via `get_extension_name` (None → empty, mirrors a NULL `%s`) | OK |
| `rel = table_open(ExtensionRelationId, RowExclusiveLock)` | `table_open(mcx, ExtensionRelationId, RowExclusiveLock)` (seam) | OK (3079) |
| `ScanKeyInit(&entry[0], Anum_pg_extension_oid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(extId))` | `ScanKeyInit(.., Anum_pg_extension_oid, BTEqualStrategyNumber, F_OIDEQ, Datum::from_oid(extId))` | OK |
| `systable_beginscan(rel, ExtensionOidIndexId, true, NULL, 1, entry)` | `systable_beginscan(&rel, ExtensionOidIndexId, true, None, &[key])` (genam seam, SysScanGuard) | OK (3080, index_ok=true, catalog snapshot) |
| `tuple = systable_getnext(scandesc)` + `if (HeapTupleIsValid) CatalogTupleDelete(rel, &tuple->t_self)` | `systable_getnext` → `if Some CatalogTupleDelete(mcx, &rel, tup.tuple.t_self)` | OK — at most one row assumption preserved |
| `systable_endscan` + `table_close(rel, RowExclusiveLock)` | `scan.end()` + `rel.close(RowExclusiveLock)` (RAII guard + owned close) | OK |

## CreateExtension parse-side (C 2094-2176)

| C step | Port | Verdict |
|---|---|---|
| `check_valid_extension_name(stmt->extname)` | same (extname from `Option<PgString>`) | OK |
| `if (get_extension_oid(stmt->extname, true) != InvalidOid)` dup branch: IF NOT EXISTS → `ereport(NOTICE, ERRCODE_DUPLICATE_OBJECT, "extension \"%s\" already exists, skipping"); return InvalidObjectAddress` else `ereport(ERROR, … "already exists")` | same; NOTICE emitted via `.finish(here)` returning Ok then `return Ok(InvalidObjectAddress)`; ERROR via `.into_error()` | OK |
| `if (creating_extension) ereport(ERROR, ERRCODE_FEATURE_NOT_SUPPORTED, "nested CREATE EXTENSION is not supported")` | same | OK |
| `foreach option`: schema/new_version/cascade with `errorConflictingDefElem` on dup, `defGetString`/`defGetBoolean`, else `elog(ERROR,"unrecognized option: %s")` | in-crate `def_get_string`/`def_get_boolean`/`error_conflicting_def_elem` on `ddlnodes::DefElem` (the model `CreateExtensionStmt.options` carry — the shared define.c crate is on the `parsenodes::DefElem` model, so the readers are ported in-crate 1:1) | OK |
| `return CreateExtensionInternal(extname, schemaName, versionName, cascade, NIL, true)` | `deferred::CreateExtensionInternal()` (loud panic — executor/SPI/catalog install body is out of F0) | OK (mirror-pg-and-panic) |

Note: `defGetString` here handles the value-node kinds CREATE EXTENSION can
produce (String/Integer/Boolean); the T_TypeName/T_List/T_A_Star arms of the full
define.c `defGetString` are not reachable for these three options (grammar
produces only opt_boolean_or_string / NumericOnly), so they fall through to the
"unrecognized node type" elog, exactly as those would never appear.

## Seam ownership

Installs (in `init_seams`, wired into seams-init `init_all`) all 5 inward seams
this unit owns: `creating_extension`, `current_extension_object`,
`get_extension_name`, `get_extension_oid`, `RemoveExtensionById`. Signatures
unchanged vs the existing decls; the consumers (pg_depend / dependency /
objectaddress) call them as-is.

RE-HOMING: `check_membership_in_current_extension` was mis-declared in
`backend-commands-extension-seams` — it is `catalog/pg_depend.c`'s
`checkMembershipInCurrentExtension`, not an extension.c function (extension.h only
externs it). Its real seam already exists and is installed by
`backend-catalog-pg-depend` (`backend-catalog-pg-depend-seams::checkMembershipInCurrentExtension`,
taking `mcx, &ObjectAddress`). The mis-homed decl was removed and its only
consumer (sequence.c `DefineSequence`) re-pointed to the pg_depend owner seam.
This avoids a false `every_declared_seam_is_installed_by_its_owner` failure
(extension.c would never install a pg_depend function) and removes a duplicate.

## Out-of-F0 ledger (src/deferred.rs — loud panic, NOT silent stub)

`CreateExtensionInternal`, `get_required_extension`, `InsertExtensionTuple`,
`ExecAlterExtensionStmt`/`ApplyExtensionUpdates`,
`ExecAlterExtensionContentsStmt`/`…Recurse`, `AlterExtensionNamespace`,
`execute_extension_script`/`execute_sql_string`, `read_extension_script_file`,
`read_whole_file`, `extension_is_trusted`, `get_extension_schema`,
`get_function_sibling_type`/`ext_sibling_callback`, the SRFs
(`pg_available_extension{s,_versions}`, `pg_extension_update_paths`,
`pg_get_loaded_modules`), `pg_extension_config_dump`/`extension_config_remove`,
`convert_requires_to_datum`, `extension_file_exists`, `script_error_callback` —
each drives the executor / SPI / catalog-DML / fmgr-Datum / filesystem-write
subsystems that are not yet wired.

## Gate

`cargo check --workspace` clean; `cargo test -p no-todo-guard` and
`cargo test -p seams-init` (both recurrence guards) green. No `todo!`/
`unimplemented!`; no own-logic stubs (every panic is a deferred-callee
mirror-pg-and-panic or a corruption-only `PgError::error`).
