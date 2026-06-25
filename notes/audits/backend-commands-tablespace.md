# Audit: backend-commands-tablespace (tablespace.c)

C source: `src/backend/commands/tablespace.c` (1569 lines).
Port: `crates/backend-commands-tablespace/src/lib.rs`.
Independent audit, re-derived from C + c2rust. **Verdict: PASS.**

## Function inventory & verdicts

| C function (line) | Port location | Verdict | Notes |
|---|---|---|---|
| `TablespaceCreateDbspace` (111) | `TablespaceCreateDbspace` | MATCH | GLOBALTABLESPACE_OID short-circuit; stat → ENOENT → lock/recheck/MakePGDirectory/(redo)pg_mkdir_p; non-ENOENT errors; S_ISDIR check. `GetDatabasePath` direct. |
| `CreateTableSpace` (207) | `CreateTableSpace` | MATCH | superuser; owner via RoleSpec→get_rolespec_oid else GetUserId; canonicalize_path; single-quote ban; in_place; absolute-path; length bound (uses canonical `TABLESPACE_VERSION_DIRECTORY`/`OIDCHARS`/`FORKNAMECHARS`/`MAXPGPATH`); DataDir-prefix WARNING; reserved-name; duplicate-name; table_open RowExclusive; binary-upgrade vs GetNewOidWithIndex(TablespaceOidIndexId,1); options build; insert; recordDependencyOnOwner; post-create hook; create_tablespace_directories; XLOG create; ForceSyncCommit; close NoLock. |
| `DropTableSpace` (394) | `DropTableSpace` | MATCH | scan by name; missing_ok NOTICE vs UNDEFINED_OBJECT; ownercheck (NOT_OWNER); IsPinnedObject (NO_PRIV); checkSharedDependencies → DEPENDENT_OBJECTS error w/ detail+detail_log; drop hook; delete tuple; DeleteSharedComments; DeleteSharedSecurityLabel; deleteSharedDependencyRecordsFor; LWLock; destroy retry path: RequestCheckpoint(IMMEDIATE\|FORCE\|WAIT) + release/barrier/acquire + retry → NOT_IN_PREREQUISITE_STATE "is not empty"; XLOG drop; ForceSyncCommit; release; close. |
| `create_tablespace_directories` (571, static) | `create_tablespace_directories` | MATCH | linkloc; in_place MakePGDirectory(EEXIST ok); version dir path; chmod (ENOENT→UNDEFINED_FILE + InRecovery hint, else file error); stat version dir (ENOENT→mkdir, non-ENOENT error, exists-not-dir→WRONG_OBJECT_TYPE, exists+!InRecovery→OBJECT_IN_USE); InRecovery remove old symlink; symlink. |
| `destroy_tablespace_directories` (685, static) | `destroy_tablespace_directories` + `remove_symlink_phase` | MATCH | AllocateDir: ENOENT (warn unless redo)→remove_symlink goto; redo other-error LOG→false; ReadDir loop skip ./..; friendly empty check; rmdir(redo?LOG:ERROR); FreeDir; rmdir version dir→false; symlink phase: lstat / S_ISDIR rmdir / S_ISLNK unlink / else NOT_IN_PREREQUISITE_STATE — all with redo?LOG:(ENOENT?WARNING:ERROR) severity. `goto remove_symlink` modeled as early `remove_symlink_phase` return. |
| `directory_is_empty` (852) | `directory_is_empty` | MATCH | AllocateDir/ReadDir loop skip ./.. → false on first real entry, else true. |
| `remove_tablespace_symlink` (882) | `remove_tablespace_symlink` | MATCH | lstat ENOENT→ok; S_ISDIR rmdir(≠ENOENT ERROR); S_ISLNK unlink(≠ENOENT ERROR); else NOT_IN_PREREQUISITE_STATE. Always ERROR (not redo-conditional). |
| `RenameTableSpace` (929) | `RenameTableSpace` | MATCH | scan old → UNDEFINED_OBJECT; ownercheck (NO_PRIV); reserved-name; new-name exists → DUPLICATE_OBJECT; update_tablespace_name (namestrcpy+CatalogTupleUpdate); post-alter hook; ObjectAddressSet. |
| `AlterTableSpaceOptions` (1014) | `AlterTableSpaceOptions` | MATCH | scan → UNDEFINED_OBJECT; ownercheck (NOT_OWNER); update_tablespace_options (old opts + transform + reloptions validate + heap_modify_tuple + CatalogTupleUpdate); post-alter hook. |
| `check_default_tablespace` (1090) | `check_default_tablespace` | MATCH | IsTransactionState && MyDatabaseId!=Invalid; empty-string fast pass; lookup; PGC_S_TEST NOTICE vs GUC_check_errdetail+false. |
| `GetDefaultTablespace` (1142) | `GetDefaultTablespace` | MATCH | TEMP → Prepare + GetNextTempTableSpace; empty default → InvalidOid; lookup; == MyDatabaseTableSpace → partitioned FEATURE_NOT_SUPPORTED else InvalidOid. |
| `check_temp_tablespaces` (1197) | `check_temp_tablespaces` + `resolve_temp_tablespaces` | MATCH | SplitIdentifierString (List-syntax-invalid→false); txn+db guard; per-name: empty→Invalid, lookup (missing_ok=source<=PGC_S_TEST), TEST NOTICE, ==MyDatabaseTableSpace→Invalid, object_aclcheck CREATE (interactive aclcheck_error else skip); extra = validated OID list. |
| `assign_temp_tablespaces` (1305) | `assign_temp_tablespaces` | MATCH | extra→SetTempTablespaces(list) else SetTempTablespaces(empty). C void; SetTempTablespaces infallible. |
| `PrepareTempTablespaces` (1330) | `PrepareTempTablespaces` + `resolve_prepare_tablespaces` | MATCH | TempTablespacesAreSet short-circuit; !IsTransactionState short-circuit; SplitIdentifierString (syntax err → SetTempTablespaces empty); per-name: empty→Invalid, lookup missing_ok, **skip silently** (no NOTICE), ==MyDatabaseTableSpace→Invalid, aclcheck skip-only; SetTempTablespaces. Distinct from check path (no NOTICE / no aclcheck_error). |
| `get_tablespace_oid` (1425) | `get_tablespace_oid` | MATCH | AccessShareLock open; scan_by_name; close AccessShareLock; !valid && !missing_ok → UNDEFINED_OBJECT. |
| `get_tablespace_name` (1471) | `get_tablespace_name` | MATCH | AccessShareLock open; scan_name_by_oid (pstrdup in mcx); close; None on no match. |
| `tblspc_redo` (1510) | `tblspc_redo` | MATCH | info = XLogRecGetInfo & ~XLR_INFO_MASK; CREATE → decode {ts_id:Oid, ts_path:cstr} → create_tablespace_directories; DROP → smgr barrier + destroy(redo) → on fail ResolveRecoveryConflictWithTablespace + retry → LOG "directories…could not be removed" + hint; else PANIC "unknown op code". |

Helpers `SplitIdentifierString`/`IdentifierListParser`/`scanner_isspace`/`truncate_identifier` (varlena.c/scansup.c logic) ported in-crate: MATCH (quoted "" doubling, unquoted lowercase, separator/whitespace, NAMEDATALEN-1 byte clamp on char boundary, scanner_isspace incl. \v \f).

## Constants verified against C headers
- `RM_TBLSPC_ID = 5` (rmgrlist.h order XLOG=0..TBLSPC=5). ✓
- `XLOG_TBLSPC_CREATE=0x00`, `XLOG_TBLSPC_DROP=0x10` (tablespace.h). ✓
- `CHECKPOINT_IMMEDIATE=0x0004`, `FORCE=0x0008`, `WAIT=0x0020` (xlog.h). ✓
- `TableSpaceRelationId=1213`, `TablespaceOidIndexId=2697`, `Anum_pg_tablespace_oid=1` (pg_tablespace.h). ✓
- `GLOBALTABLESPACE_OID`, `MAXPGPATH=1024`, `OIDCHARS=10`, `FORKNAMECHARS=4`, `PG_TBLSPC_DIR="pg_tblspc"`, `TABLESPACE_VERSION_DIRECTORY="PG_18_202506291"`. ✓

### Finding fixed during audit
The initial port hardcoded `TABLESPACE_VERSION_DIRECTORY = "PG_18_202504071"`, diverging from the workspace's canonical `types_storage::file::TABLESPACE_VERSION_DIRECTORY = "PG_18_202506291"` (== `CATALOG_VERSION_NO`) used by `GetDatabasePath`/relcache. This would have made tablespace path layout inconsistent with the rest of the cluster. Fixed by importing the canonical constants from `types-core`/`types-storage` instead of local literals.

## Seam audit
Outward seams in `backend-commands-tablespace-seams` (consumed by rmgr/acl/pg-shdepend) all installed by `init_seams()`, wired into `seams-init::init_all()`: `tblspc_redo`, `get_tablespace_name`, `get_tablespace_oid`, `prepare_temp_tablespaces`, `tablespace_create_dbspace`. The two no-mcx outward seams open a transient `MemoryContext` to adapt to the mcx-taking impls (thin marshal; results are Copy OIDs / fd.c-owned state, so the transient context dropping is correct).

Catalog `pg_tablespace` primitives live in `backend-catalog-pg-tablespace-seams` (TablespaceTuple{oid,handle}, table_open/close, scan_by_name, scan_name_by_oid, build_create_options, insert/update_name/update_options/delete) — consumed but **not** installed by tablespace, owned by a future pg_tablespace catalog provider; panic-until-landed. This is the established repo pattern (identical to seclabel.c's pg_seclabel primitives in `backend-commands-seclabel-seams`, which seclabel also does not install). The `Form_pg_tablespace` marshaling stays at the seam boundary; the command crate carries no raw heap scan — so the values[]/nulls[]/repl arrays are at the catalog owner, not "missing" from tablespace (tablespace's own logic is the control flow, which is fully present).

New external seam crates for unported owners (declared, not installed, panic-until-landed — owned by the named owner, not tablespace):
- `backend-storage-file-tblspc-fs-seams` — raw stat/lstat/chmod/symlink/rmdir/unlink + make_pg_directory/pg_mkdir_p (owner: storage/fd.c + src/port), with StatResult/StatKind + ENOENT/EEXIST so C's errno branching is exact.
- `backend-commands-tablespace-globals-seams` — path helpers (canonicalize_path/is_absolute_path/path_is_prefix_of_path/get_parent_directory), per-backend globals (MyDatabaseId/MyDatabaseTableSpace/InRecovery/allowSystemTableMods/IsBinaryUpgrade/take_binary_upgrade_next_oid), GUC string readers (default_tablespace/temp_tablespaces/allow_in_place_tablespaces), TablespaceCreateLock acquire/release.

Real ported callees called directly (no cycle): IsReservedName/IsPinnedObject/GetNewOidWithIndex, GetUserId, GetDatabasePath, AllocateDir/ReadDir/FreeDir + SetTempTablespaces/TempTablespacesAreSet/GetNextTempTableSpace, XLogBeginInsert/RegisterData/Insert, ForceSyncCommit/IsTransactionState, EmitProcSignalBarrier/WaitForProcSignalBarrier, ResolveRecoveryConflictWithTablespace, DeleteSharedComments, DeleteSharedSecurityLabel, GUC_check_errdetail, DataDir. Via existing owner -seams: superuser, get_rolespec_oid, object_ownercheck/object_aclcheck/aclcheck_error, recordDependencyOnOwner/checkSharedDependencies/deleteSharedDependencyRecordsFor, invoke_object_*_hook, request_checkpoint.

## Design-conformance
- No `todo!`/`unimplemented!`/`unreachable!` for logic (no-todo-guard PASS).
- Allocations on palloc paths use fallible `try_reserve` (materialize_def_elems, the temp resolvers, the identifier parser). `format!`/`to_string` hits are at error-construction sites only.
- No invented opacity: TablespaceTuple.handle is a real `ItemPointerData`; OID/index constants are real values.
- `TablespaceCreateLock` held across `?` in DropTableSpace mirrors C's manual LWLock acquire/release (PG releases LWLocks on transaction/resource-owner abort, not RAII); the "lock released on transaction abort" comment documents this. Faithful mirror of C, not a new leak.
- Allocating seams take `Mcx`; getters for foreign globals are parameter-free only where the C global is genuinely ambient per-backend state owned elsewhere (routed through the owner's seam crate, not modeled as a tablespace static).

## Not ported (intentional, matches C/src-idiomatic)
- `ENFORCE_REGRESSION_TEST_NAME_RESTRICTIONS` `#ifdef` warnings in CreateTableSpace/RenameTableSpace — compiled out in normal builds.
- GUC hook registration into guc-tables slots — that wiring belongs to the GUC-table owner; the hooks are present as pub fns with the crate's own `TempTablespacesExtra`.

PASS — every function MATCH or correctly-SEAMED per repo discipline; constants verified against headers (one divergence found and fixed); seams owned/installed correctly; gate green.
