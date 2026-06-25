# Audit: backend-bootstrap-bootstrap

C source: `src/backend/bootstrap/bootstrap.c` (998 lines).
Port: `crates/backend-bootstrap-bootstrap/src/lib.rs`.
c2rust reference: `../pgrust/c2rust-runs/backend-bootstrap-bootstrap/src/bootstrap.rs`.

Re-derived independently from the C source and the c2rust rendering; constants
re-checked against the c2rust post-preprocessor values (the generated headers
`fmgroids.h`/`pg_type_d.h`/`pg_collation_d.h` are not present in the source
tree).

## Function inventory (every C definition)

| # | C function (line) | Port location | Verdict | Notes |
|---|---|---|---|---|
| 1 | `CheckerModeMain` (180, static) | `CheckerModeMain` | SEAMED | body is `proc_exit(0)` → `backend_storage_ipc_ipc_seams::proc_exit::call(0)` (ipc.c owner, unported → panics). `-> !`. |
| 2 | `BootstrapModeMain` (198) | `BootstrapModeMain` | MATCH | full option-parse loop, every getopt case, the `argc != optind` check, the SelectConfigFiles/checkDataDir/.../proc_exit ordering, the `check_only` early `CheckerModeMain` path, link-canary check, bootparse drive, RelationMapFinishBootstrap, cleanup, proc_exit(0). All externals SEAMED. `Assert(!IsUnderPostmaster)` → `debug_assert!`. |
| 3 | `bootstrap_signals` (413, static) | `bootstrap_signals` | MATCH | the four `pqsignal(SIG*, SIG_DFL)` calls (SIGHUP/SIGINT/SIGTERM/SIGQUIT) via `port_pqsignal_seams::pqsignal(libc::SIG*, SigHandler::Default)`; `Assert(!IsUnderPostmaster)` → `debug_assert!`. |
| 4 | `boot_openrel` (440) | `boot_openrel` | MATCH | NAMEDATALEN truncation, populate-Typ-if-NIL, close-if-open, DEBUG4 trace, `table_openrv(makeRangeVar(NULL,relname,-1),NoLock)`, numattr loop, allocate-then-memmove attr, per-attr DEBUG4. table_openrv called directly (acyclic). makeRangeVar built in-crate (faithful defaults: catalogname NULL, inh true, RELPERSISTENCE_PERMANENT, location -1). |
| 5 | `closerel` (485) | `closerel` | MATCH | relname-mismatch elog(ERROR), no-open-relation elog(ERROR), DEBUG4 trace, `table_close(rel,NoLock)` via `Relation::close(NoLock)`, clear boot_reldesc. |
| 6 | `DefineAttr` (522) | `DefineAttr` | MATCH | open-rel WARNING+close, allocate, MemSet→default, namestrcpy, attnum+1, gettype, both Typ!=NIL and TypInfo branches (atttypid/attlen/attbyval/attalign/attstorage/attcompression=InvalidCompressionMethod/attcollation/attndims), C-collation forcing, atttypmod=-1, attislocal=true, the three nullness cases incl. the BOOTCOL_NULL_AUTO fixed-width prior-column scan with the `i==attnum` test. |
| 7 | `InsertOneTuple` (629) | `InsertOneTuple` | SEAMED | DEBUG4 trace, assembles attrtypes/values/nulls slices for numattr, the CreateTupleDesc+heap_form_tuple+simple_heap_insert+heap_freetuple sequence batched into `heapam_seams::insert_one_tuple` (heap owner, unported), row-inserted DEBUG4, reset Nulls loop. |
| 8 | `InsertOneValue` (657) | `InsertOneValue` | MATCH/SEAMED | bounds debug_assert, DEBUG4, reads atttypid off boot_reldesc->rd_att, `boot_get_type_io_data` (in-crate), `OidInputFunctionCall`/`OidOutputFunctionCall` SEAMED to fmgr.c; DEBUG4 inserted-trace. |
| 9 | `InsertOneNull` (695) | `InsertOneNull` | MATCH | DEBUG4, bounds debug_assert, attnotnull → elog(ERROR) with attname/relname, values[i]=NULL datum, Nulls[i]=true. |
| 10 | `cleanup` (713, static) | `cleanup` | MATCH | close boot_reldesc if open. |
| 11 | `populate_typ_list` (726, static) | `populate_typ_list` | SEAMED | the `table_open(TypeRelationId)`+`table_beginscan_catalog`+`heap_getnext` loop batched into `heapam_seams::read_pg_type` (heap owner, unported); builds the Typ list of typmap. `Assert(Typ==NIL)` → debug_assert. |
| 12 | `gettype` (765, static) | `gettype` | MATCH | the Typ!=NIL path (lookup, reload-and-retry for composite types via populate_typ_list, set Ap), the Typ==NIL path (TypInfo index scan returning the *index*, DEBUG4 external-type, populate, recurse), final unrecognized-type elog(ERROR). The ugly index-vs-OID contract preserved. |
| 13 | `boot_get_type_io_data` (837) | `boot_get_type_io_data` | MATCH | Typ!=NIL path (foreach with the C's "ap set even when not matched, then ap->am_oid != typid ⇒ elog" semantics, getTypeIOParam typioparam logic, fields), TypInfo path (index scan, elog on overflow, typdelim=',', getTypeIOParam, fields). |
| 14 | `AllocateAttribute` (914, static) | `AllocateAttribute` | MATCH | `MemoryContextAllocZero(TopMemoryContext, ATTRIBUTE_FIXED_PART_SIZE)` → zeroed FormData_pg_attribute (Default). |
| 15 | `index_register` (931) | `index_register` | MATCH | copy IndexInfo + push onto ILHead. C's `copyObject(ii_Expressions/ii_Predicate)` + `ii_ExpressionsState=NIL`/`ii_PredicateState=NULL` + the three `Assert(ii_Exclusion*==NULL)` operate on fields absent from the shared trimmed `types_nodes::execnodes::IndexInfo` (consumed as-is per freeze-vocabulary); on this branch the deep-copy is a value copy and the no-exclusion-constraint precondition is structural. Control flow (copy → list-head push) preserved. |
| 16 | `build_indices` (982) | `build_indices` | MATCH/SEAMED | the `for (; ILHead; ILHead=il_next)` walk: table_open (direct, acyclic), index_open (SEAMED indexam.c), index_build (SEAMED index.c), index_close/table_close via `Relation::close(NoLock)`. |

## Constants verified (against c2rust post-preprocessor values)

Type OIDs: PG_NODE_TREEOID=194, REGNAMESPACEOID=4089, REGROLEOID=4096,
ACLITEMOID=1033, INT4ARRAYOID=1007, C_COLLATION_OID=950,
DEFAULT_COLLATION_OID=100. fmgr OIDs: F_PG_NODE_TREE_IN/OUT=195/196,
F_REGNAMESPACEIN/OUT=4084/4085, F_REGROLEIN/OUT=4098/4092, F_BYTEAOUT=31,
F_CHAROUT=33. The non-OID array literals (1009/1028/1002/1034 for
_text/_oid/_char/_aclitem) match the C source. TypInfo has 25 entries
(`n_types()` test asserts 25). MAXATTR=40, NAMEDATALEN=64,
ATTRIBUTE_FIXED_PART_SIZE=100, PG_DATA_CHECKSUM_VERSION=1, the BOOTCOL_NULL_*
codes 1/2/3, the TYPALIGN_*/TYPSTORAGE_* chars — all match.

## Seam audit

Owned C file: `bootstrap.c`. The unit declares **no inward seam crate**: nothing
calls bootstrap.c across a dependency cycle — the BKI front end (bootparse) calls
these functions by direct dependency. `init_seams()` is therefore empty by
design (no owned `-seams` crate exists), and it is wired into
`seams-init::init_all()`. Per step 3 this is correct, not an empty-installer
FAIL (there are no owned seam declarations outstanding).

Outward seam calls (each justified by a real cycle to an unported owner;
thin marshal+delegate, no logic in the seam path):

- guc.c: `initialize_guc_options`, `set_config_option`, `parse_long_option`
  (Mcx, PgResult), `select_config_files` — extend `backend-utils-misc-guc-seams`.
- main.c: `parse_dispatch_option` (→ `DispatchOption`) — new `backend-main-main-seams`.
- miscinit.c: init_standalone_process, check_data_dir, change_to_data_dir,
  create_data_dir_lock_file, set_processing_mode_bootstrap/normal,
  set_ignore_system_indexes — extend `backend-utils-init-miscinit-seams`.
- postinit.c: initialize_max_backends, initialize_fast_path_locks, base_init,
  init_postgres_bootstrap — new `backend-utils-init-postinit-seams`.
- pmchild.c: init_postmaster_child_slots — new.
- ipci.c: create_shared_memory_and_semaphores — new.
- ipc.c: proc_exit (`-> !`) — new.
- fd.c: set_max_safe_fds — extend.
- proc.c: init_process — extend.
- xlog.c: boot_strap_xlog — extend `backend-access-transam-xlog-seams`.
- relmapper.c: relation_map_finish_bootstrap — extend.
- fmgr.c: oid_input_function_call, oid_output_function_call_datum — extend
  `backend-utils-fmgr-fmgr-seams` (the pre-existing TupleValue-shaped
  `oid_output_function_call` serves typed-attribute callers; bootstrap holds a
  bare `Datum`, so a distinct raw-Datum form is declared — not a workaround, a
  different marshaling of the same C function).
- heapam.c: insert_one_tuple, read_pg_type (batched) — new `backend-access-heap-heapam-seams`.
- index.c: index_build — extend `backend-catalog-index-seams`.
- indexam.c: index_open — existing seam, used as-is.
- bootparse.y/bootscanner.l: boot_yylex_init, boot_yyparse — new.
- link-canary.c: pg_link_canary_is_frontend — new.
- pqsignal.c: pqsignal — existing `port-pqsignal-seams`.
- table.c: table_open/table_openrv/Relation::close — direct dep (acyclic).

No branching/node-construction/computation lives in any seam path; the seam
signatures mirror each C function's failure surface (fallible → `PgResult`,
infallible → bare value; allocating → `Mcx`).

## Design conformance

- Per-backend globals (boot_reldesc, attrtypes[]/numattr, values[]/Nulls[],
  Typ/Ap, ILHead, OutputFileName) → `thread_local! RefCell<…>` (AGENTS.md
  "Backend-global state"), never shared statics/atomics. `TYP_INFO` is an
  immutable `static &[…]` const table (the C `static const`), not mutable state.
- Allocating entry points take `Mcx<'static>` and return `PgResult`; allocating
  seams take `Mcx`. boot_reldesc holds a `types_rel::Relation<'static>`
  (process/Top-context lifetime).
- Types: `FormData_pg_type` (types-tuple::pg_type, real catalog row through
  typcollation), `ATTRIBUTE_FIXED_PART_SIZE` (types-tuple), `DispatchOption`
  (`#[repr(i32)]` enum in types-startup, verified against postmaster.h order) —
  no integer aliases / byte blobs / stand-in opaque structs (types.md 6-7).
  Shared `IndexInfo` consumed as-is, not reshaped.
- No locks held across `?` (the relation handle's lock release is its `Drop`/
  `close`); no registry side tables; no ambient-global getter seams (the
  zero-arg seams are init actions / a build constant, not foreign per-backend
  value getters); no unledgered divergence markers.
- elog(ERROR) sites map to `ereport(ERROR).finish()` returning `Err`;
  `.unwrap_err()` is used only on a `finish()` already known to be `Err` at the
  ERROR level (the C `/* not reached */`), not as a stand-in for an error path.

## Gate

`cargo check -p backend-bootstrap-bootstrap -p seams-init` and
`cargo check --workspace` pass (only pre-existing warnings elsewhere). The 11
in-crate unit tests pass.

## Verdict: PASS

Every C function is MATCH or SEAMED (SEAMED only where the called *owner* is
unported — the logic of bootstrap.c itself is fully present). Zero seam
findings; zero design-conformance findings. The SEAMED paths panic loudly until
their owners land, which is the sanctioned state.
