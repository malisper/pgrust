# Audit: backend-access-transam-rmgr

- **C source**: `src/backend/access/transam/rmgr.c` (+ `src/include/access/rmgr.h`,
  `src/include/access/rmgrlist.h`, the `RmgrData` struct and
  `RmgrIdExists`/`GetRmgr` inlines of `src/include/access/xlog_internal.h`,
  and `pg_strcasecmp` from `src/port/pgstrcasecmp.c` inlined as a private helper)
- **c2rust**: `c2rust-runs/backend-access-transam-rmgr/src/rmgr.rs`
- **Port**: `crates/backend-access-transam-rmgr/src/lib.rs`; shared types in
  `crates/types-wal/src/rmgr.rs` and `crates/types-core/src/fmgr.rs`
- **Branch / commit audited**: `port/backend-access-transam-rmgr` @ 7382e19
  (+ one fix commit produced by this audit, see Findings)
- **Auditor**: independent re-derivation from C + c2rust. `cargo check
  --workspace` clean; `cargo test -p backend-access-transam-rmgr` 12/12 pass.

## 1. Function inventory

Every function definition in `rmgr.c`, cross-checked against the c2rust
rendering (which also pulls in the static inlines the unit instantiates):

| # | C function | C location | c2rust | Port | Verdict |
|---|---|---|---|---|---|
| 1 | `RmgrTable[RM_MAX_ID + 1]` initializer | rmgr.c:50-52 | rmgr.rs:3453 (`[RmgrData; 256]`) | `RMGR_BUILTIN_TABLE` + `initial_rmgr_table()` (lib.rs:87, 364) | MATCH (slots SEAMED, see below) |
| 2 | `RmgrStartup` | rmgr.c:57-68 | rmgr.rs:6224 | `RmgrStartup` (lib.rs:393) | MATCH |
| 3 | `RmgrCleanup` | rmgr.c:73-84 | rmgr.rs:6238 | `RmgrCleanup` (lib.rs:407) | MATCH |
| 4 | `RmgrNotFound` | rmgr.c:90-95 | rmgr.rs:6252 | `RmgrNotFound` (lib.rs:421) | MATCH |
| 5 | `RegisterCustomRmgr` | rmgr.c:106-146 | rmgr.rs:6285 | `RegisterCustomRmgr` (lib.rs:439) | MATCH |
| 6 | `pg_get_wal_resource_managers` | rmgr.c:149-170 | rmgr.rs:6510 | `pg_get_wal_resource_managers` (lib.rs:525) | MATCH |
| 7 | `RmgrIdExists` (inline, xlog_internal.h:369-373) | header | rmgr.rs:3437 | `RmgrIdExists` (lib.rs:377) | MATCH |
| 8 | `GetRmgr` (inline, xlog_internal.h:375-381) | header | rmgr.rs:3443 | `GetRmgr` (lib.rs:383) | MATCH |
| 9 | `RmgrIdIsBuiltin` (inline, rmgr.h) | header | rmgr.rs:3430 | `types_wal::rmgr::RmgrIdIsBuiltin` | MATCH |
| 10 | `RmgrIdIsCustom` (inline, rmgr.h) | header | rmgr.rs:3433 | `types_wal::rmgr::RmgrIdIsCustom` | MATCH |
| 11 | `RmgrIdIsValid` (macro, rmgr.h) | header | (macro-expanded) | `types_wal::rmgr::RmgrIdIsValid` | MATCH |
| 12 | `pg_strcasecmp` (src/port/pgstrcasecmp.c, called by #5) | port lib | extern decl | private `pg_strcasecmp` (lib.rs:564) | MATCH (note below) |

No other definitions exist in the translation unit (verified by reading the
entire 170-line C file and grepping the c2rust output for `fn ` definitions:
everything else is types/constants/extern decls).

## 2. Per-function comparison

### Constants (access/rmgr.h, verified against the header)

| Item | C value | Port (`types-wal/src/rmgr.rs`) | Verdict |
|---|---|---|---|
| `RM_NEXT_ID` | 22 (22 `PG_RMGR` lines in rmgrlist.h, counted) | 22 | MATCH |
| `RM_MAX_ID` | `UINT8_MAX` = 255 | implied by `RM_N_IDS - 1` loops | MATCH |
| `RM_MAX_BUILTIN_ID` | `RM_NEXT_ID - 1` = 21 | 21 | MATCH |
| `RM_MIN_CUSTOM_ID` | 128 | 128 | MATCH |
| `RM_MAX_CUSTOM_ID` | `UINT8_MAX` = 255 | 255 | MATCH |
| `RM_N_IDS` | 256 | 256 | MATCH |
| `RM_N_BUILTIN_IDS` | 22 | 22 | MATCH |
| `RM_N_CUSTOM_IDS` | 128 | 128 | MATCH |
| `RM_EXPERIMENTAL_ID` | 128 | 128 | MATCH |
| `RmgrId` | `uint8` | `types_core::RmgrId = uint8` | MATCH |
| `RmgrIdIsBuiltin(rmid)` | `rmid <= RM_MAX_BUILTIN_ID` | same | MATCH |
| `RmgrIdIsCustom(rmid)` | `rmid >= 128 && rmid <= 255` | same | MATCH |
| `RmgrIdIsValid(rmid)` | builtin \|\| custom | same | MATCH |
| `RmgrData` field order | name, redo, desc, identify, startup, cleanup, mask, decode (xlog_internal.h:349-360) | same | MATCH |

### RmgrTable initializer — all 22 rows re-derived from rmgrlist.h

Every row was compared cell-by-cell against `access/rmgrlist.h` and the
c2rust `static RmgrTable: [RmgrData; 256]` (22 named rows + 234
zero/`null` rows; the port has 22 builtin rows + 234 `RmgrData::EMPTY`):

| id | name | redo | desc | identify | startup | cleanup | mask | decode | Verdict |
|---|---|---|---|---|---|---|---|---|---|
| 0 | XLOG | xlog_redo | xlog_desc | xlog_identify | NULL | NULL | NULL | xlog_decode | MATCH/SEAMED |
| 1 | Transaction | xact_redo | xact_desc | xact_identify | NULL | NULL | NULL | xact_decode | MATCH/SEAMED |
| 2 | Storage | smgr_redo | smgr_desc | smgr_identify | NULL | NULL | NULL | NULL | MATCH/SEAMED |
| 3 | CLOG | clog_redo | clog_desc | clog_identify | NULL | NULL | NULL | NULL | MATCH/SEAMED |
| 4 | Database | dbase_redo | dbase_desc | dbase_identify | NULL | NULL | NULL | NULL | MATCH/SEAMED |
| 5 | Tablespace | tblspc_redo | tblspc_desc | tblspc_identify | NULL | NULL | NULL | NULL | MATCH/SEAMED |
| 6 | MultiXact | multixact_redo | multixact_desc | multixact_identify | NULL | NULL | NULL | NULL | MATCH/SEAMED |
| 7 | RelMap | relmap_redo | relmap_desc | relmap_identify | NULL | NULL | NULL | NULL | MATCH/SEAMED |
| 8 | Standby | standby_redo | standby_desc | standby_identify | NULL | NULL | NULL | standby_decode | MATCH/SEAMED |
| 9 | Heap2 | heap2_redo | heap2_desc | heap2_identify | NULL | NULL | heap_mask | heap2_decode | MATCH/SEAMED |
| 10 | Heap | heap_redo | heap_desc | heap_identify | NULL | NULL | heap_mask | heap_decode | MATCH/SEAMED |
| 11 | Btree | btree_redo | btree_desc | btree_identify | btree_xlog_startup | btree_xlog_cleanup | btree_mask | NULL | MATCH/SEAMED |
| 12 | Hash | hash_redo | hash_desc | hash_identify | NULL | NULL | hash_mask | NULL | MATCH/SEAMED |
| 13 | Gin | gin_redo | gin_desc | gin_identify | gin_xlog_startup | gin_xlog_cleanup | gin_mask | NULL | MATCH/SEAMED |
| 14 | Gist | gist_redo | gist_desc | gist_identify | gist_xlog_startup | gist_xlog_cleanup | gist_mask | NULL | MATCH/SEAMED |
| 15 | Sequence | seq_redo | seq_desc | seq_identify | NULL | NULL | seq_mask | NULL | MATCH/SEAMED |
| 16 | SPGist | spg_redo | spg_desc | spg_identify | spg_xlog_startup | spg_xlog_cleanup | spg_mask | NULL | MATCH/SEAMED |
| 17 | BRIN | brin_redo | brin_desc | brin_identify | NULL | NULL | brin_mask | NULL | MATCH/SEAMED |
| 18 | CommitTs | commit_ts_redo | commit_ts_desc | commit_ts_identify | NULL | NULL | NULL | NULL | MATCH/SEAMED |
| 19 | ReplicationOrigin | replorigin_redo | replorigin_desc | replorigin_identify | NULL | NULL | NULL | NULL | MATCH/SEAMED |
| 20 | Generic | generic_redo | generic_desc | generic_identify | NULL | NULL | generic_mask | NULL | MATCH/SEAMED |
| 21 | LogicalMessage | logicalmsg_redo | logicalmsg_desc | logicalmsg_identify | NULL | NULL | NULL | logicalmsg_decode | MATCH/SEAMED |

Notes:
- Heap2 and Heap share `heap_mask` in C; the port has both rows pointing at
  the same `heapam_xlog::heap_mask::call` — verified.
- Every NULL cell in rmgrlist.h is `None` in the port; every named cell is
  the owning subsystem's seam `call` fn. The port's tests additionally
  re-verify the non-NULL pattern of all 22 rows against an independently
  transcribed copy of rmgrlist.h (`tests.rs::builtin_table_matches_rmgrlist`).
- C global `RmgrTable[]` is backend-private, mutated only by
  `RegisterCustomRmgr` during `shared_preload_libraries`; the port's
  `thread_local! RefCell<[RmgrData; 256]>` reproduces per-process semantics.
  `rmgr_table_slot` copies the row out (no borrow held across callbacks), so
  reentrancy behavior matches C's by-value `GetRmgr`.

### Function bodies

- **`RmgrStartup`** — C: `for rmid in 0..=RM_MAX_ID`, skip
  `!RmgrIdExists`, call `rm_startup` if non-NULL. Port: `for rmid in
  0..RM_N_IDS` (identical range 0..=255), same skip, `if let Some(startup)`.
  Returns `PgResult` because the four real startup callbacks
  (btree/gin/gist/spg) allocate recovery contexts and can `ereport(ERROR)`.
  MATCH.
- **`RmgrCleanup`** — identical loop shape; cleanup callbacks are infallible
  in C (context delete), port type `fn()`. MATCH.
- **`RmgrNotFound`** — `ereport(ERROR, errmsg("resource manager with ID %d
  not registered"), errhint(...preload...))`. Port returns
  `Err(PgError::new(ERROR, ...))` with identical message and hint text,
  default SQLSTATE XX000 (C ereport carries no errcode; `PgError::new`
  applies elog.c's level-default, verified in the types-error audit).
  Location `rmgr.c:94 RmgrNotFound` matches C `__LINE__` per c2rust. MATCH.
- **`RegisterCustomRmgr`** — all five ERROR checks present in C order with
  identical predicates, messages, hints, and details:
  1. `rm_name == NULL || strlen(rm_name) == 0` → "custom resource manager
     name is invalid" + hint (port: `None` or empty `&str`);
  2. `!RmgrIdIsCustom(rmid)` → "...ID %d is out of range" + hint "between
     128 and 255" (constants interpolated, values verified above);
  3. `!process_shared_preload_libraries_in_progress` (read through the
     miscinit owner seam) → "failed to register..." + detail about
     shared_preload_libraries;
  4. `RmgrTable[rmid].rm_name != NULL` → detail names the *existing*
     registrant (C prints `RmgrTable[rmid].rm_name`, port prints the
     existing slot's name — same value);
  5. duplicate-name scan over `0..=RM_MAX_ID` skipping nonexistent ids,
     `pg_strcasecmp(existing, new) == 0` → detail "Existing resource manager
     with ID %d has the same name." with the *existing* id.
  Then `RmgrTable[rmid] = *rmgr` (port: copy into the thread-local slot) and
  the LOG-level ereport "registered custom resource manager \"%s\" with ID
  %d", emitted through the error owner seam (LOG must not unwind; severity
  15 = LOG matches). Error locations 111/116/121/127/138/145 match the
  c2rust `errfinish` line arguments exactly. MATCH.
- **`pg_get_wal_resource_managers`** — `InitMaterializedSRF(fcinfo, 0)`
  first, then loop `0..=RM_MAX_ID` skipping nonexistent ids; per row
  `values[0]=Int32GetDatum(rmid)` (sign-extending `Datum::from_i32`),
  `values[1]=CStringGetTextDatum(GetRmgr(rmid).rm_name)` (varlena owner
  seam), `values[2]=BoolGetDatum(RmgrIdIsBuiltin(rmid))`; `nulls` all-false
  exactly as C's zero-initialized stack array; `tuplestore_putvalues` via
  the funcapi owner seam; returns `(Datum) 0` = `Datum::null()` (`Self(0)`,
  verified in types-datum). `PG_GET_RESOURCE_MANAGERS_COLS` = 3. The
  `fcinfo`/`ReturnSetInfo` shapes are fmgr-owned and cross as opaque handles
  (`types_core::fmgr::{FunctionCallInfoHandle, MaterializedSrfHandle}`);
  no logic was moved across the seam (resolving `rsinfo->setResult/setDesc`
  from `fcinfo->resultinfo` is fmgr bookkeeping, not rmgr logic). MATCH.
- **`RmgrIdExists` / `GetRmgr`** (xlog_internal.h inlines) — exists =
  `rm_name != NULL` → `rm_name.is_some()`; GetRmgr errors via RmgrNotFound
  when missing, else returns the row by value. C's noreturn `ereport(ERROR)`
  becomes `Err` propagation. MATCH.
- **`pg_strcasecmp`** (private helper, src/port/pgstrcasecmp.c) — ASCII A-Z
  fold on inequality, byte loop, sign-correct result for length mismatch
  (only `== 0` is consumed here, as in C). The C version additionally
  applies the locale's `tolower()` to bytes with the high bit set; under the
  C/POSIX locale `isupper()` is false for those bytes, making the C function
  byte-identical to the port. The locale-sensitive fold is not reproducible
  over Rust UTF-8 `&str` and is documented at the definition. MATCH
  (C-locale-exact; noted).

## 3. Seam audit

This unit **owns no inward seams**: nothing in rmgr.c is called back from a
dependency in a cycle-forming way; consumers (xlog recovery, waldump, etc.)
will depend on the crate directly. Accordingly there is no `init_seams()` in
this crate, no `seams-init` change, and `seams-init` was verified untouched
by the port commit — consistent with the `backend-access-transam-twophase-rmgr`
precedent.

Outward references: every non-NULL `RmgrTable` cell in C is an extern symbol
defined in another translation unit, resolved across the link — exactly the
seam pattern. The port created/extended 45 owner-side declaration crates
(xlog, clog, commit-ts, generic-xlog, storage, dbcommands, tablespace,
relmapper, standby, heapam-xlog, nbt/hash/gin/gist/spg/brin xlog, sequence,
origin/message/decode, the 21 rmgrdesc desc/identify crates, multixact and
xact extensions, plus miscinit
(`process_shared_preload_libraries_in_progress`), funcapi
(`InitMaterializedSRF` / `materialized_srf_putvalues`), varlena
(`cstring_to_text`), and the pre-existing error-owner `ereport` for the LOG
path). Checks performed:

- Read all the seam-crate `lib.rs` files touched by the port: each contains
  only `seam_core::seam!` declarations — zero logic, zero branching. Clean.
- Signatures are uniform per rmgrlist column family and consistent with the
  `RmgrData` callback types in `types_wal::rmgr`
  (redo `&mut XLogReaderState -> PgResult<()>`, desc `(&mut String,
  &XLogReaderState) -> PgResult<()>`, identify `u8 -> Option<&'static str>`,
  startup `-> PgResult<()>`, cleanup `fn()`, mask `(&mut [u8], BlockNumber)
  -> PgResult<()>`, decode `(&mut LogicalDecodingContext, &mut
  XLogRecordBuffer) -> PgResult<()>`), matching the C pointer types in
  xlog_internal.h:349-360. The placeholder opaque parameter types cannot be
  constructed, so no callback can be invoked with fabricated state.
- `grep '::set('` across the unit and all touched seam crates: hits only in
  the port's own `#[cfg(test)]` module (stubbing owners to prove dispatch).
  No production `set()` outside an owner. Clean.
- Every table cell is a bare `owner::seam::call` fn-pointer — thin
  delegation, no marshaling logic in any seam path. Uninstalled seams panic
  loudly (seam-core), which is the accepted posture for unported callees.

**Seam findings: none.**

## 4. Findings and fixes

1. **(fixed during audit)** The seven `ErrorLocation` line numbers recorded
   the first line of each multi-line `ereport(...)` statement (93, 110, 114,
   119, 124, 136, 143) whereas C's `__LINE__` expands to the statement's
   closing line — verified against the c2rust `errfinish` arguments (94, 111,
   116, 121, 127, 138, 145). Cosmetic log-metadata divergence; corrected to
   the exact C values and re-verified each call site against c2rust after the
   fix. Tests re-run green.

No other findings: no missing functions, no simplified logic, no constant
transcription errors.

## 5. Verdict

All 12 inventory rows `MATCH` (table cells `SEAMED` per the rules: external
callbacks delegated through thin owner-named declarations); constants
verified against `rmgr.h`/`rmgrlist.h`/`xlog_internal.h`; the one metadata
finding fixed and re-audited; zero seam findings; `cargo check --workspace`
clean and 12/12 unit tests pass.

**PASS**
